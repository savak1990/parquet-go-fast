package parquetfast_test

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// benchContainer / benchRow model a realistically nested record: scalars, an
// optional struct, a primitive map, and a struct-valued map.
type benchStats struct {
	CPU float64 `parquet:"cpu"`
	Mem float64 `parquet:"mem"`
}

type benchContainer struct {
	Name  string `parquet:"name"`
	Image string `parquet:"image"`
	Count int64  `parquet:"count"`
}

type benchRow struct {
	ID         int64                     `parquet:"id"`
	Name       string                    `parquet:"name"`
	Namespace  string                    `parquet:"namespace"`
	Replicas   int32                     `parquet:"replicas"`
	Ratio      float64                   `parquet:"ratio"`
	Active     bool                      `parquet:"active"`
	Stats      *benchStats               `parquet:"stats,optional"`
	Labels     map[string]string         `parquet:"labels"`
	Containers map[string]benchContainer `parquet:"containers"`
}

func init() {
	parquetfast.RegisterStructAlloc[benchStats]()
	parquetfast.RegisterStructValuedMap[string, benchContainer](func(v parquet.Value) string {
		return string(v.ByteArray())
	})
}

func makeBenchRow(i int) benchRow {
	return benchRow{
		ID:        int64(i),
		Name:      fmt.Sprintf("workload-%d", i),
		Namespace: fmt.Sprintf("ns-%d", i%32),
		Replicas:  int32(i % 10),
		Ratio:     float64(i) * 0.125,
		Active:    i%2 == 0,
		Stats:     &benchStats{CPU: float64(i%100) * 0.5, Mem: float64(i % 256)},
		Labels: map[string]string{
			"app":  fmt.Sprintf("app-%d", i%50),
			"tier": "backend",
			"env":  "prod",
		},
		Containers: map[string]benchContainer{
			"main":    {Name: "main", Image: "img:latest", Count: int64(i % 5)},
			"sidecar": {Name: "sidecar", Image: "proxy:v2", Count: 1},
		},
	}
}

var (
	benchOnce sync.Once
	benchData []byte
)

// largeFixture builds (once) a multi-row-group parquet file of `benchFixtureRows`
// records, snappy-compressed. Generated synthetically — no external data.
const benchFixtureRows = 200_000

func largeFixture(tb testing.TB) []byte {
	tb.Helper()

	benchOnce.Do(func() {
		var buf bytes.Buffer

		w := parquet.NewGenericWriter[benchRow](&buf,
			parquet.Compression(&parquet.Snappy),
			parquet.MaxRowsPerRowGroup(50_000),
		)

		const batch = 4096
		rows := make([]benchRow, batch)

		for written := 0; written < benchFixtureRows; {
			n := min(batch, benchFixtureRows-written)
			for i := 0; i < n; i++ {
				rows[i] = makeBenchRow(written + i)
			}

			if _, err := w.Write(rows[:n]); err != nil {
				tb.Fatalf("write: %v", err)
			}

			written += n
		}

		if err := w.Close(); err != nil {
			tb.Fatalf("close: %v", err)
		}

		benchData = buf.Bytes()
	})

	return benchData
}

// BenchmarkDecode compares parquet-go's reflection-driven GenericReader against
// parquet-go-fast over the same large fixture. Run with -benchmem.
func BenchmarkDecode(b *testing.B) {
	data := largeFixture(b)
	b.Logf("fixture: %d rows, %d bytes", benchFixtureRows, len(data))

	b.Run("parquet-go/GenericReader", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			r := parquet.NewGenericReader[benchRow](bytes.NewReader(data))
			buf := make([]benchRow, 4096)

			for {
				n, err := r.Read(buf)
				if n == 0 {
					_ = r.Close()

					break
				}

				if err != nil {
					_ = r.Close()

					break
				}
			}
		}
	})

	b.Run("parquet-go-fast/UnmarshalBytes", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[benchRow](data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parquet-go-fast/Reader", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			rd, err := parquetfast.NewReader[benchRow](bytes.NewReader(data), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}

			buf := make([]benchRow, 4096)
			for {
				n, err := rd.Read(buf)
				if n == 0 || err != nil {
					break
				}
			}

			_ = rd.Close()
		}
	})
}

// BenchmarkPlanApply isolates the per-row hot path (enum-switch dispatch) from
// file I/O: it decodes one already-read row repeatedly through a compiled plan.
func BenchmarkPlanApply(b *testing.B) {
	data := writeOneRow(b)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		b.Fatal(err)
	}

	plan, err := parquetfast.Compile(reflect.TypeFor[benchRow](), f.Schema(), nil)
	if err != nil {
		b.Fatal(err)
	}

	rows := f.RowGroups()[0].Rows()
	defer func() { _ = rows.Close() }()

	batch := make([]parquet.Row, 1)
	if _, err := rows.ReadRows(batch); err != nil && batch[0] == nil {
		b.Fatal(err)
	}

	row := batch[0].Clone()
	leafVals := make([][]parquet.Value, plan.NumLeaves())

	var dst benchRow

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		plan.Apply(unsafe.Pointer(&dst), row, leafVals)
	}
}

func writeOneRow(b *testing.B) []byte {
	b.Helper()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[benchRow](&buf)
	if _, err := w.Write([]benchRow{makeBenchRow(1)}); err != nil {
		b.Fatal(err)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	return buf.Bytes()
}
