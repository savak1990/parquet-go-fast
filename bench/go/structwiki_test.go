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
// bespoke. NOTE: parquet-go silently returns empty for the list-of-struct fields
// (license, additional_entities) — the list elements aren't named "element" — so
// its number reflects decoding less than ours.

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

func BenchmarkSWFull_ParquetGo(b *testing.B) { benchParquetGo[Article](b, readStructWiki(b)) }

// ── 2. Projection (scalar identifier+name+url → columnar fast path) ────────────

func BenchmarkSWProj_Ours(b *testing.B) { benchOurs[ArticleMeta](b, readStructWiki(b)) }

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
