package bench

import (
	"bytes"
	"database/sql"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// dbpedia-entities-openai-1M (HuggingFace KShivendu/...): three string columns
// plus a 1536-dim `openai` embedding as a LIST<float64> (DOUBLE). The list makes the
// record NOT scalar-only, so the full read takes our row path (the honest weak
// case, same class as map/list-heavy production data). Projecting just the scalar
// metadata (no embedding) flips it back to the fast columnar path — so one file
// exercises both. Three workloads mirror the other suites.

type WikiEmbedding struct {
	ID    string    `parquet:"_id"`
	Title string    `parquet:"title"`
	Text  string    `parquet:"text"`
	Emb   []float64 `parquet:"openai"`
}

// WikiMeta is the scalar-only projection (no embedding → columnar fast path).
type WikiMeta struct {
	ID    string `parquet:"_id"`
	Title string `parquet:"title"`
}

// dbpediaFilterTitle splits the data ~in half by title (a string range predicate,
// so our filter takes the row path).
const dbpediaFilterTitle = "M"

func dbpediaPath() string {
	if p := os.Getenv("DBPEDIA_FILE"); p != "" {
		return p
	}

	return "../data/dbpedia.parquet"
}

func readDbpedia(tb testing.TB) []byte {
	tb.Helper()

	data, err := os.ReadFile(dbpediaPath())
	if err != nil {
		tb.Skipf("dbpedia data missing (%v); see bench/README to download", err)
	}

	return data
}

func skipIfNoDbpedia(b *testing.B) {
	if _, err := os.Stat(dbpediaPath()); err != nil {
		b.Skipf("dbpedia data missing (%v); see bench/README", err)
	}
}

// ── 1. Full materialization (3 strings + LIST<float64> (DOUBLE) → rows; row path) ───────

func BenchmarkWikiFull_Ours(b *testing.B) { benchOurs[WikiEmbedding](b, readDbpedia(b)) }

func BenchmarkWikiFull_OursConcurrent(b *testing.B) {
	data := readDbpedia(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[WikiEmbedding](data, parquetfast.WithConcurrency(0))
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

func BenchmarkWikiFull_ParquetGo(b *testing.B) { benchParquetGo[WikiEmbedding](b, readDbpedia(b)) }

func BenchmarkWikiFull_ArrowGoRows(b *testing.B) {
	data := readDbpedia(b)
	benchArrow(b, data, func(tb testing.TB, d []byte) int {
		tbl, done := arrowProjTable(tb, d, []int{0, 1, 2, 3})
		defer done()

		out := make([]WikiEmbedding, tbl.NumRows())
		colString(tbl, "_id", func(i int, v string) { out[i].ID = v })
		colString(tbl, "title", func(i int, v string) { out[i].Title = v })
		colString(tbl, "text", func(i int, v string) { out[i].Text = v })
		colFloat64List(tbl, "openai", func(i int, v []float64) { out[i].Emb = v })

		return len(out)
	})
}

func BenchmarkWikiFull_DuckDB(b *testing.B) {
	skipIfNoDbpedia(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	benchDuck(b, db, func(tb testing.TB, db *sql.DB) int { return duckWikiFull(tb, db) })
}

// ── 2. Projection (scalar metadata only, no embedding → columnar path) ─────────

func BenchmarkWikiProj_Ours(b *testing.B) { benchOurs[WikiMeta](b, readDbpedia(b)) }

func BenchmarkWikiProj_ParquetGo(b *testing.B) { benchParquetGo[WikiMeta](b, readDbpedia(b)) }

func BenchmarkWikiProj_ArrowGoRows(b *testing.B) {
	data := readDbpedia(b)
	benchArrow(b, data, func(tb testing.TB, d []byte) int {
		tbl, done := arrowProjTable(tb, d, []int{0, 1})
		defer done()

		out := make([]WikiMeta, tbl.NumRows())
		colString(tbl, "_id", func(i int, v string) { out[i].ID = v })
		colString(tbl, "title", func(i int, v string) { out[i].Title = v })

		return len(out)
	})
}

func BenchmarkWikiProj_DuckDB(b *testing.B) {
	skipIfNoDbpedia(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	benchDuck(b, db, func(tb testing.TB, db *sql.DB) int { return len(duckWikiMeta(tb, db, "")) })
}

// ── 3. Filter (title < "M", ~half → matching rows; string predicate → row path) ─

func BenchmarkWikiFilter_Ours(b *testing.B) {
	data := readDbpedia(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[WikiMeta](data,
			parquetfast.Where(parquetfast.Col("title").Less(dbpediaFilterTitle)))
		if err != nil {
			b.Fatal(err)
		}

		matched = len(rows)
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkWikiFilter_ParquetGo(b *testing.B) {
	data := readDbpedia(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		all, err := parquet.Read[WikiMeta](bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}

		matched = 0

		for i := range all {
			if all[i].Title < dbpediaFilterTitle {
				matched++
			}
		}
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkWikiFilter_DuckDB(b *testing.B) {
	skipIfNoDbpedia(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		matched = len(duckWikiMeta(b, db, "WHERE title < '"+dbpediaFilterTitle+"'"))
	}

	b.ReportMetric(float64(matched), "matched")
}

// ── helpers ───────────────────────────────────────────────────────────────────

// colFloat64List transposes a LIST<float64> (DOUBLE) Arrow column into per-row []float64,
// copying each sublist so the result is independent (GC-safe), matching what our
// decoder produces.
func colFloat64List(tbl arrow.Table, name string, set func(i int, v []float64)) {
	col := tbl.Column(tbl.Schema().FieldIndices(name)[0])

	idx := 0
	for _, chunk := range col.Data().Chunks() {
		la := chunk.(*array.List)
		vals := la.ListValues().(*array.Float64).Float64Values()
		offs := la.Offsets()

		for i := 0; i < la.Len(); i++ {
			if !la.IsNull(i) {
				lo, hi := offs[i], offs[i+1]
				s := make([]float64, hi-lo)
				copy(s, vals[lo:hi])
				set(idx, s)
			}

			idx++
		}
	}
}

func duckWikiFull(tb testing.TB, db *sql.DB) int {
	tb.Helper()

	rows, err := db.Query("SELECT _id,title,text,openai FROM read_parquet('" + dbpediaPath() + "')")
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]WikiEmbedding, 0, 60000)

	var (
		id, title, text sql.NullString
		embRaw          any
	)

	for rows.Next() {
		if err := rows.Scan(&id, &title, &text, &embRaw); err != nil {
			tb.Fatal(err)
		}

		emb := toFloat64s(embRaw)
		out = append(out, WikiEmbedding{ID: id.String, Title: title.String, Text: text.String, Emb: emb})
	}

	return len(out)
}

func duckWikiMeta(tb testing.TB, db *sql.DB, where string) []WikiMeta {
	tb.Helper()

	rows, err := db.Query("SELECT _id,title FROM read_parquet('" + dbpediaPath() + "') " + where)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	var (
		out       []WikiMeta
		id, title sql.NullString
	)

	for rows.Next() {
		if err := rows.Scan(&id, &title); err != nil {
			tb.Fatal(err)
		}

		out = append(out, WikiMeta{ID: id.String, Title: title.String})
	}

	return out
}

// toFloat64s converts the []interface{} that go-duckdb yields for a LIST<DOUBLE>
// column into []float64 (the real cost of getting a list into Go via the driver).
func toFloat64s(v any) []float64 {
	s, ok := v.([]interface{})
	if !ok {
		return nil
	}

	out := make([]float64, len(s))
	for i, x := range s {
		if f, ok := x.(float64); ok {
			out[i] = f
		}
	}

	return out
}
