package parquetfast_test

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Sparse + wide + column-heavy-map benchmark — the shape where the raw approach
// wins biggest, mirroring production telemetry rollups: many columns, a
// high-cardinality map-of-structs with []byte histogram blobs, and a long tail
// of optional columns that are entirely null per file (features most rows don't
// use). Domain is generic API-service metrics (unrelated to any real data).
//
// This is where parquet-go-fast's null-column elision + reflection-free decode
// shine: the all-null tail is skipped in the read pipeline entirely, and the
// dense part is decoded without reflection.

type svcHist struct {
	P50  float64 `parquet:"p50"`
	P90  float64 `parquet:"p90"`
	P99  float64 `parquet:"p99"`
	Hist []byte  `parquet:"hist"`
}

type svcEndpoint struct {
	Path    string           `parquet:"path"`
	Hits    int64            `parquet:"hits"`
	Errors  int64            `parquet:"errors"`
	Latency []byte           `parquet:"latency"`
	Codes   map[string]int64 `parquet:"codes"`
	Stats   *svcHist         `parquet:"stats,optional"`

	// Per-endpoint sparse tail — like pqtContainer's GPU/Java/init stats: these
	// optional sub-structs are entirely null across the file, but live INSIDE
	// the high-cardinality endpoints map, so the reflection reader pays for them
	// per map entry (8×/row). This is where null-column elision matters most.
	Gpu  *svcHist `parquet:"gpu,optional"`
	Java *svcHist `parquet:"java,optional"`
	Init *svcHist `parquet:"init,optional"`
}

type serviceRollup struct {
	Service      string                 `parquet:"service"`
	Region       string                 `parquet:"region"`
	Cluster      string                 `parquet:"cluster"`
	Window       int64                  `parquet:"window"`
	RequestCount int64                  `parquet:"request_count"`
	ErrorCount   int64                  `parquet:"error_count"`
	Labels       map[string]string      `parquet:"labels"`
	Endpoints    map[string]svcEndpoint `parquet:"endpoints"`

	// Long tail of optional columns left nil in this fixture — entirely null
	// across the file, the way production records carry features most rows
	// don't use. ~29 leaf columns of dead weight that the reflection reader
	// still decompresses + decodes and the raw reader elides.
	GpuStats   *svcHist                      `parquet:"gpu_stats,optional"`
	TLSStats   *svcHist                      `parquet:"tls_stats,optional"`
	CacheStats *svcHist                      `parquet:"cache_stats,optional"`
	DBStats    *svcHist                      `parquet:"db_stats,optional"`
	QueueStats *svcHist                      `parquet:"queue_stats,optional"`
	Canary     *svcHist                      `parquet:"canary,optional"`
	Geo        map[string]map[string]float64 `parquet:"geo"`
	Debug      *[]byte                       `parquet:"debug,optional"`
}

func init() {
	parquetfast.RegisterStructAlloc[svcHist]()
	parquetfast.RegisterStructValuedMap[string, svcEndpoint](func(v parquet.Value) string {
		return string(v.ByteArray())
	})
}

func makeServiceRollup(i int) serviceRollup {
	eps := make(map[string]svcEndpoint, 8)
	for j := 0; j < 8; j++ {
		path := "/api/v" + string(rune('0'+j%10)) + "/x"
		eps[path] = svcEndpoint{
			Path:    path,
			Hits:    int64(i*j + 1),
			Errors:  int64(j),
			Latency: blob(i+j, 40),
			Codes:   map[string]int64{"200": int64(i), "500": int64(j)},
			Stats:   &svcHist{P50: float64(j), P90: float64(j) * 2, P99: float64(j) * 3, Hist: blob(i*j, 24)},
		}
	}

	return serviceRollup{
		Service:      "svc-" + string(rune('a'+i%26)),
		Region:       []string{"us", "eu", "apac"}[i%3],
		Cluster:      "c" + string(rune('0'+i%10)),
		Window:       1_700_000_000 + int64(i)*3600,
		RequestCount: int64(i * 100),
		ErrorCount:   int64(i % 50),
		Labels:       map[string]string{"team": "t" + string(rune('a'+i%8)), "tier": "gold"},
		Endpoints:    eps,
		// all *Stats / Geo / Debug deliberately left nil → all-null columns
	}
}

func writeServiceFixture(tb testing.TB, n, rowsPerRG int) []byte {
	tb.Helper()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[serviceRollup](&buf,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(int64(rowsPerRG)),
	)

	const batch = 4096

	rows := make([]serviceRollup, batch)

	for written := 0; written < n; {
		k := min(batch, n-written)
		for i := 0; i < k; i++ {
			rows[i] = makeServiceRollup(written + i)
		}

		if _, err := w.Write(rows[:k]); err != nil {
			tb.Fatalf("write: %v", err)
		}

		written += k
	}

	if err := w.Close(); err != nil {
		tb.Fatalf("close: %v", err)
	}

	return buf.Bytes()
}

func BenchmarkSparseWide(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping sparse-wide benchmark in -short mode")
	}

	const n = 200_000

	data := writeServiceFixture(b, n, 50_000) // 4 row groups

	// Count how many leaf columns are all-null (the elided tail).
	f, _ := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	b.Logf("fixture: %d rows, %d bytes, %d leaf columns, GOMAXPROCS=%d",
		n, len(data), len(f.Schema().Columns()), runtime.GOMAXPROCS(0))

	b.Run("parquet-go/GenericReader", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			r := parquet.NewGenericReader[serviceRollup](bytes.NewReader(data))
			out := make([]serviceRollup, n)

			m := 0
			for m < len(out) {
				k, err := r.Read(out[m:])
				m += k

				if err != nil {
					break
				}
			}

			_ = r.Close()
		}
	})

	b.Run("parquet-go-fast/UnmarshalBytes", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[serviceRollup](data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parquet-go-fast/UnmarshalBytes-no-null-skip", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[serviceRollup](data, parquetfast.WithoutNullColumnSkip()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parquet-go-fast/UnmarshalBytes-concurrent", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[serviceRollup](data, parquetfast.WithConcurrency(0)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Verify the fixture really has an all-null tail (so the benchmark measures what
// we claim) and that decoding is correct.
func TestSparseWide_FixtureIsSparseAndDecodes(t *testing.T) {
	data := writeServiceFixture(t, 200, 64)

	f, _ := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	t.Logf("%d leaf columns", len(f.Schema().Columns()))

	got, err := parquetfast.UnmarshalBytes[serviceRollup](data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 200 {
		t.Fatalf("got %d rows", len(got))
	}

	for i := range got {
		if got[i].GpuStats != nil || got[i].DBStats != nil || got[i].Debug != nil || got[i].Geo != nil {
			t.Fatalf("row %d: expected sparse tail to be nil", i)
		}

		if len(got[i].Endpoints) != 8 {
			t.Fatalf("row %d: expected 8 endpoints, got %d", i, len(got[i].Endpoints))
		}
	}
}

// Quick disk-path sanity for UnmarshalFile on the sparse shape.
func TestSparseWide_UnmarshalFile(t *testing.T) {
	data := writeServiceFixture(t, 100, 64)

	path := filepath.Join(t.TempDir(), "svc.parquet")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := parquetfast.UnmarshalFile[serviceRollup](path)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 100 {
		t.Fatalf("got %d rows", len(got))
	}
}
