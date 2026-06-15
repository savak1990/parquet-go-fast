package bench

import (
	"bytes"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// structured-wikipedia (HuggingFace wikimedia/structured-wikipedia): a deeply
// nested record — optional/nested structs, lists-of-structs, list<string>,
// struct-in-struct — the un-idiomatic, heavily-structured shape close to a real
// application data model (ps-model-style), the opposite of a flat analytical
// table. The nesting forces our row path everywhere except a scalar projection.

type Article struct {
	Identifier  int64     `parquet:"identifier"`
	Name        string    `parquet:"name"`
	Abstract    string    `parquet:"abstract"`
	Description string    `parquet:"description"`
	URL         string    `parquet:"url"`
	DateCreated time.Time `parquet:"date_created"`

	Image      *SWImage  `parquet:"image"`       // optional struct
	MainEntity *SWEntity `parquet:"main_entity"` // optional struct
	InLanguage *SWLang   `parquet:"in_language"` // optional struct

	AdditionalEntities []SWAddEntity `parquet:"additional_entities"` // list<struct{list<string>,...}>
	License            []SWLicense   `parquet:"license"`             // list<struct>

	Version *SWVersion `parquet:"version"` // struct-in-struct
}

type SWImage struct {
	ContentURL string `parquet:"content_url"`
	Height     int64  `parquet:"height"`
	Width      int64  `parquet:"width"`
}

type SWEntity struct {
	Identifier string `parquet:"identifier"`
	URL        string `parquet:"url"`
}

type SWLang struct {
	Identifier string `parquet:"identifier"`
}

type SWAddEntity struct {
	Aspects    []string `parquet:"aspects"`
	Identifier string   `parquet:"identifier"`
	URL        string   `parquet:"url"`
}

type SWLicense struct {
	Identifier string `parquet:"identifier"`
	Name       string `parquet:"name"`
	URL        string `parquet:"url"`
}

type SWVersion struct {
	Comment    string    `parquet:"comment"`
	Identifier int64     `parquet:"identifier"`
	Editor     *SWEditor `parquet:"editor"` // nested optional struct
}

type SWEditor struct {
	Name      string   `parquet:"name"`
	EditCount int64    `parquet:"edit_count"`
	Groups    []string `parquet:"groups"`
	IsAdmin   bool     `parquet:"is_admin"`
}

// ArticleMeta is the scalar-only projection (no nesting → columnar fast path).
type ArticleMeta struct {
	Identifier int64  `parquet:"identifier"`
	Name       string `parquet:"name"`
	URL        string `parquet:"url"`
}

func init() {
	parquetfast.RegisterStructAlloc[SWImage]()
	parquetfast.RegisterStructAlloc[SWEntity]()
	parquetfast.RegisterStructAlloc[SWLang]()
	parquetfast.RegisterStructAlloc[SWVersion]()
	parquetfast.RegisterStructAlloc[SWEditor]()
	parquetfast.RegisterStructList[SWAddEntity]()
	parquetfast.RegisterStructList[SWLicense]()
}

func structWikiPath() string {
	if p := os.Getenv("STRUCTWIKI_FILE"); p != "" {
		return p
	}

	return "../data/structwiki.parquet"
}

func readStructWiki(tb testing.TB) []byte {
	tb.Helper()

	data, err := os.ReadFile(structWikiPath())
	if err != nil {
		tb.Skipf("structured-wikipedia data missing (%v); see bench/README", err)
	}

	return data
}

func skipIfNoStructWiki(b *testing.B) {
	if _, err := os.Stat(structWikiPath()); err != nil {
		b.Skipf("structured-wikipedia data missing (%v); see bench/README", err)
	}
}

// swFilterName splits names ~45/55 (digit/quote-prefixed titles sort below "A");
// a string predicate → our row path (no columnar late-mat for strings).
const swFilterName = "A"

// ── 1. Full materialization (deeply nested → rows; row path everywhere) ────────
// Compared ours vs parquet-go only — arrow-go/DuckDB nested→Go transposes are
// bespoke. parquet-go uses ArticlePG: identical fields but with the ,list tag it
// requires to bind 3-level LIST columns. With the bare []T tags in Article (what
// our reader uses) parquet-go silently returns empty license/additional_entities
// — no error — so this fair variant gives it the tags and times the real work.

func BenchmarkSWFull_Ours(b *testing.B) { benchOurs[Article](b, readStructWiki(b)) }

func BenchmarkSWFull_OursConcurrent(b *testing.B) {
	data := readStructWiki(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[Article](data, parquetfast.WithConcurrency(0))
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

func BenchmarkSWFull_ParquetGo(b *testing.B) { benchParquetGo[ArticlePG](b, readStructWiki(b)) }

// arrow-go reads the whole nested table into columnar Arrow arrays (List/Struct);
// no row transpose — for a schema this deep a hand-written nested→Go transpose is
// bespoke, so this measures the columnar read only (all 65 leaf columns).
func BenchmarkSWFull_ArrowGoColumnar(b *testing.B) {
	data := readStructWiki(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := int64(0)
	for b.Loop() {
		total += arrowReadTableColumnar(b, data)
	}

	reportRows(b, int(total))
}

// DuckDB → Go materializes the 12 mapped top-level fields. Nested columns come back
// BOXED from the database/sql driver — lists as []interface{}, structs as
// map[string]interface{} — not typed Go structs (reflected in the Returns column).
func BenchmarkSWFull_DuckDB(b *testing.B) {
	skipIfNoStructWiki(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	benchDuck(b, db, func(tb testing.TB, db *sql.DB) int { return duckSWFull(tb, db) })
}

func duckSWFull(tb testing.TB, db *sql.DB) int {
	tb.Helper()

	rows, err := db.Query("SELECT identifier,name,abstract,description,url,date_created," +
		"image,main_entity,in_language,additional_entities,license,version " +
		"FROM read_parquet('" + structWikiPath() + "')")
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]swDuckRow, 0, 180000)

	var (
		id                            sql.NullInt64
		name, abs, desc, url          sql.NullString
		dc                            sql.NullTime
		img, ment, lang, ae, lic, ver any
	)

	for rows.Next() {
		if err := rows.Scan(&id, &name, &abs, &desc, &url, &dc, &img, &ment, &lang, &ae, &lic, &ver); err != nil {
			tb.Fatal(err)
		}

		out = append(out, swDuckRow{
			Identifier: id.Int64, Name: name.String, Abstract: abs.String,
			Description: desc.String, URL: url.String, DateCreated: dc.Time,
			Image: img, MainEntity: ment, InLanguage: lang,
			AdditionalEntities: ae, License: lic, Version: ver,
		})
	}

	return len(out)
}

// swDuckRow holds DuckDB's row: scalars typed, nested columns left boxed (the form
// the driver returns — []interface{} / map[string]interface{}).
type swDuckRow struct {
	Identifier                           int64
	Name, Abstract, Description, URL     string
	DateCreated                          time.Time
	Image, MainEntity, InLanguage        any
	AdditionalEntities, License, Version any
}

// ── 2. Projection (scalar identifier+name+url → columnar fast path) ────────────

func BenchmarkSWProj_Ours(b *testing.B) { benchOurs[ArticleMeta](b, readStructWiki(b)) }

func BenchmarkSWProj_OursConcurrent(b *testing.B) { benchOursConc[ArticleMeta](b, readStructWiki(b)) }

func BenchmarkSWProj_ParquetGo(b *testing.B) { benchParquetGo[ArticleMeta](b, readStructWiki(b)) }

func BenchmarkSWProj_DuckDB(b *testing.B) {
	skipIfNoStructWiki(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	benchDuck(b, db, func(tb testing.TB, db *sql.DB) int { return len(duckSWMeta(tb, db, "")) })
}

// ── 3. Filter (name < "A" → matching rows; string predicate → row path) ───────

func BenchmarkSWFilter_Ours(b *testing.B) {
	data := readStructWiki(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[ArticleMeta](data,
			parquetfast.Where(parquetfast.Col("name").Less(swFilterName)))
		if err != nil {
			b.Fatal(err)
		}

		matched = len(rows)
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkSWFilter_OursConcurrent(b *testing.B) {
	data := readStructWiki(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[ArticleMeta](data,
			parquetfast.Where(parquetfast.Col("name").Less(swFilterName)),
			parquetfast.WithConcurrency(0))
		if err != nil {
			b.Fatal(err)
		}

		matched = len(rows)
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkSWFilter_ParquetGo(b *testing.B) {
	data := readStructWiki(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		all, err := parquet.Read[ArticleMeta](bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}

		matched = 0

		for i := range all {
			if all[i].Name < swFilterName {
				matched++
			}
		}
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkSWFilter_DuckDB(b *testing.B) {
	skipIfNoStructWiki(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		matched = len(duckSWMeta(b, db, "WHERE name < '"+swFilterName+"'"))
	}

	b.ReportMetric(float64(matched), "matched")
}

func duckSWMeta(tb testing.TB, db *sql.DB, where string) []ArticleMeta {
	tb.Helper()

	rows, err := db.Query("SELECT identifier,name,url FROM read_parquet('" + structWikiPath() + "') " + where)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	var (
		out       []ArticleMeta
		id        sql.NullInt64
		name, url sql.NullString
	)

	for rows.Next() {
		if err := rows.Scan(&id, &name, &url); err != nil {
			tb.Fatal(err)
		}

		out = append(out, ArticleMeta{Identifier: id.Int64, Name: name.String, URL: url.String})
	}

	return out
}

// ArticlePG is the fair parquet-go mirror of Article: same fields, but list
// fields carry the ,list tag parquet-go needs to bind a 3-level LIST column.
// With bare []T tags parquet-go reads these lists as empty (silently). Our reader
// resolves list structure on its own and needs no such tag.
type ArticlePG struct {
	Identifier  int64     `parquet:"identifier"`
	Name        string    `parquet:"name"`
	Abstract    string    `parquet:"abstract"`
	Description string    `parquet:"description"`
	URL         string    `parquet:"url"`
	DateCreated time.Time `parquet:"date_created"`

	Image      *SWImage  `parquet:"image"`
	MainEntity *SWEntity `parquet:"main_entity"`
	InLanguage *SWLang   `parquet:"in_language"`

	AdditionalEntities []SWAddEntityPG `parquet:"additional_entities,list"`
	License            []SWLicense     `parquet:"license,list"`

	Version *SWVersionPG `parquet:"version"`
}

type SWAddEntityPG struct {
	Aspects    []string `parquet:"aspects,list"`
	Identifier string   `parquet:"identifier"`
	URL        string   `parquet:"url"`
}

type SWVersionPG struct {
	Comment    string      `parquet:"comment"`
	Identifier int64       `parquet:"identifier"`
	Editor     *SWEditorPG `parquet:"editor"`
}

type SWEditorPG struct {
	Name      string   `parquet:"name"`
	EditCount int64    `parquet:"edit_count"`
	Groups    []string `parquet:"groups,list"`
	IsAdmin   bool     `parquet:"is_admin"`
}
