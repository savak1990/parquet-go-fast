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

// Open-Orca (HuggingFace Open-Orca/OpenOrca, 1M-GPT4-Augmented): four string
// columns — a string-heavy text record, the counterpoint to the flat-numeric taxi
// file. It is scalar-only, so our columnar path runs with the boxed-string
// fallback (strings aren't part of the typed numeric gather). Three workloads
// mirror the taxi suite: full materialization, projection, filter.

type OpenOrca struct {
	ID           string `parquet:"id"`
	SystemPrompt string `parquet:"system_prompt"`
	Question     string `parquet:"question"`
	Response     string `parquet:"response"`
}

// OpenOrca2 is the projection subset (2 of 4 columns).
type OpenOrca2 struct {
	ID       string `parquet:"id"`
	Question string `parquet:"question"`
}

// orcaFilterPrompt is a system_prompt value of moderate selectivity, used by the
// filter workload. Set from the data (see bench/README); a string predicate, so
// our filter takes the row path (the numeric columnar filter doesn't apply).
const orcaFilterPrompt = "You are an AI assistant. You will be given a task. You must generate a detailed and long answer."

func orcaPath() string {
	if p := os.Getenv("ORCA_FILE"); p != "" {
		return p
	}

	return "../data/openorca.parquet"
}

func readOrca(tb testing.TB) []byte {
	tb.Helper()

	data, err := os.ReadFile(orcaPath())
	if err != nil {
		tb.Skipf("Open-Orca data missing (%v); see bench/README to download", err)
	}

	return data
}

// ── 1. Full materialization (all 4 string columns → rows) ─────────────────────

func BenchmarkOrcaFull_Ours(b *testing.B) { benchOurs[OpenOrca](b, readOrca(b)) }

func BenchmarkOrcaFull_OursConcurrent(b *testing.B) {
	data := readOrca(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[OpenOrca](data, parquetfast.WithConcurrency(0))
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

func BenchmarkOrcaFull_ParquetGo(b *testing.B) { benchParquetGo[OpenOrca](b, readOrca(b)) }

func BenchmarkOrcaFull_ArrowGoRows(b *testing.B) {
	data := readOrca(b)
	benchArrow(b, data, func(tb testing.TB, d []byte) int {
		tbl, done := arrowProjTable(tb, d, []int{0, 1, 2, 3})
		defer done()

		out := make([]OpenOrca, tbl.NumRows())
		colString(tbl, "id", func(i int, v string) { out[i].ID = v })
		colString(tbl, "system_prompt", func(i int, v string) { out[i].SystemPrompt = v })
		colString(tbl, "question", func(i int, v string) { out[i].Question = v })
		colString(tbl, "response", func(i int, v string) { out[i].Response = v })

		return len(out)
	})
}

func BenchmarkOrcaFull_DuckDB(b *testing.B) {
	skipIfNoOrca(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	benchDuck(b, db, func(tb testing.TB, db *sql.DB) int {
		return len(duckOrca(tb, db, "id,system_prompt,question,response", ""))
	})
}

// ── 2. Projection (2 of 4 columns → rows) ─────────────────────────────────────

func BenchmarkOrcaProj_Ours(b *testing.B) { benchOurs[OpenOrca2](b, readOrca(b)) }

func BenchmarkOrcaProj_OursConcurrent(b *testing.B) { benchOursConc[OpenOrca2](b, readOrca(b)) }

func BenchmarkOrcaProj_ParquetGo(b *testing.B) { benchParquetGo[OpenOrca2](b, readOrca(b)) }

func BenchmarkOrcaProj_ArrowGoRows(b *testing.B) {
	data := readOrca(b)
	benchArrow(b, data, func(tb testing.TB, d []byte) int {
		tbl, done := arrowProjTable(tb, d, []int{0, 2})
		defer done()

		out := make([]OpenOrca2, tbl.NumRows())
		colString(tbl, "id", func(i int, v string) { out[i].ID = v })
		colString(tbl, "question", func(i int, v string) { out[i].Question = v })

		return len(out)
	})
}

func BenchmarkOrcaProj_DuckDB(b *testing.B) {
	skipIfNoOrca(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	benchDuck(b, db, func(tb testing.TB, db *sql.DB) int {
		return len(duckOrca2(tb, db, ""))
	})
}

// ── 3. Filter (system_prompt == X → matching rows) ────────────────────────────

func BenchmarkOrcaFilter_Ours(b *testing.B) {
	data := readOrca(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[OpenOrca2](data,
			parquetfast.Where(parquetfast.Col("system_prompt").Equal(orcaFilterPrompt)))
		if err != nil {
			b.Fatal(err)
		}

		matched = len(rows)
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkOrcaFilter_OursConcurrent(b *testing.B) {
	data := readOrca(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[OpenOrca2](data,
			parquetfast.Where(parquetfast.Col("system_prompt").Equal(orcaFilterPrompt)),
			parquetfast.WithConcurrency(0))
		if err != nil {
			b.Fatal(err)
		}

		matched = len(rows)
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkOrcaFilter_ParquetGo(b *testing.B) {
	data := readOrca(b)
	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		all, err := parquet.Read[OpenOrca](bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}

		matched = 0

		for i := range all {
			if all[i].SystemPrompt == orcaFilterPrompt {
				matched++
			}
		}
	}

	b.ReportMetric(float64(matched), "matched")
}

func BenchmarkOrcaFilter_DuckDB(b *testing.B) {
	skipIfNoOrca(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	b.ReportAllocs()
	b.ResetTimer()

	var matched int
	for b.Loop() {
		matched = len(duckOrca2(b, db, "WHERE system_prompt = '"+orcaFilterPrompt+"'"))
	}

	b.ReportMetric(float64(matched), "matched")
}

// ── helpers ───────────────────────────────────────────────────────────────────

func skipIfNoOrca(b *testing.B) {
	if _, err := os.Stat(orcaPath()); err != nil {
		b.Skipf("Open-Orca data missing (%v); see bench/README", err)
	}
}

// colString transposes a (Large)String Arrow column into rows.
func colString(tbl arrow.Table, name string, set func(i int, v string)) {
	col := tbl.Column(tbl.Schema().FieldIndices(name)[0])

	idx := 0
	for _, chunk := range col.Data().Chunks() {
		switch a := chunk.(type) {
		case *array.String:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					set(idx, a.Value(i))
				}

				idx++
			}
		case *array.LargeString:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					set(idx, a.Value(i))
				}

				idx++
			}
		}
	}
}

func duckOrca(tb testing.TB, db *sql.DB, cols, where string) []OpenOrca {
	tb.Helper()

	q := "SELECT " + cols + " FROM read_parquet('" + orcaPath() + "') " + where

	rows, err := db.Query(q)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	var (
		out                  []OpenOrca
		id, sysp, ques, resp sql.NullString
	)

	for rows.Next() {
		if err := rows.Scan(&id, &sysp, &ques, &resp); err != nil {
			tb.Fatal(err)
		}

		out = append(out, OpenOrca{ID: id.String, SystemPrompt: sysp.String, Question: ques.String, Response: resp.String})
	}

	return out
}

func duckOrca2(tb testing.TB, db *sql.DB, where string) []OpenOrca2 {
	tb.Helper()

	q := "SELECT id,question FROM read_parquet('" + orcaPath() + "') " + where

	rows, err := db.Query(q)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	var (
		out      []OpenOrca2
		id, ques sql.NullString
	)

	for rows.Next() {
		if err := rows.Scan(&id, &ques); err != nil {
			tb.Fatal(err)
		}

		out = append(out, OpenOrca2{ID: id.String, Question: ques.String})
	}

	return out
}
