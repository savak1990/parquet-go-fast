package parquetfast_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Scale tests: generate large multi-row-group files on disk and decode every
// row via the streaming Reader, verifying order-independent aggregates so a
// dropped/duplicated/corrupted row is caught without holding all rows in memory.
//
// Aggregates use integer/byte sums only (commutative + exact) so map-iteration
// order during generation vs decode can't introduce float drift.
//
// These write hundreds of MB and take tens of seconds; `go test -short` skips
// them.

const millionRows = 1_000_000

// ── merchant aggregates ──────────────────────────────────────────────────────

type merchAgg struct {
	rows          int64
	sumOrderCount int64
	products      int64
	sumUnitsSold  int64
	blobByteSum   uint64
}

func accMerchant(a *merchAgg, m merchantDay) {
	a.rows++
	a.sumOrderCount += m.OrderCount

	if m.DailyHisto != nil {
		a.blobByteSum += byteSum(*m.DailyHisto)
	}

	for _, p := range m.Products {
		a.products++
		a.sumUnitsSold += p.UnitsSold

		if p.Stats != nil {
			a.blobByteSum += byteSum(p.Stats.Histogram)
		}
	}
}

func byteSum(b []byte) uint64 {
	var s uint64
	for _, x := range b {
		s += uint64(x)
	}

	return s
}

// writeMerchantFile generates n merchant rows into a parquet file at path,
// returning the expected aggregates computed from the generated data.
func writeMerchantFile(tb testing.TB, path string, n, rowsPerRG int) merchAgg {
	tb.Helper()

	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()

	w := parquet.NewGenericWriter[merchantDay](f,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(int64(rowsPerRG)),
	)

	const batch = 4096

	rows := make([]merchantDay, batch)

	var want merchAgg

	for written := 0; written < n; {
		k := min(batch, n-written)
		for i := 0; i < k; i++ {
			seed := written + i + 1
			m := makeMerchant(seed, seed%6) // cardinality 0..5
			rows[i] = m
			accMerchant(&want, m)
		}

		if _, err := w.Write(rows[:k]); err != nil {
			tb.Fatalf("write: %v", err)
		}

		written += k
	}

	if err := w.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}

	return want
}

func TestMerchantMillionStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-row scale test in -short mode")
	}

	path := filepath.Join(t.TempDir(), "merchants.parquet")
	want := writeMerchantFile(t, path, millionRows, 100_000) // ~10 row groups

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	t.Logf("file: %d bytes on disk for %d rows", fi.Size(), millionRows)

	rd, err := parquetfast.NewReader[merchantDay](f, fi.Size())
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()

	if rd.NumRows() != millionRows {
		t.Fatalf("NumRows = %d, want %d", rd.NumRows(), millionRows)
	}

	var got merchAgg

	buf := make([]merchantDay, 8192)
	for {
		n, err := rd.Read(buf)
		for i := 0; i < n; i++ {
			accMerchant(&got, buf[i])
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}

	if got != want {
		t.Fatalf("aggregate mismatch:\n want %+v\n got  %+v", want, got)
	}

	t.Logf("decoded %d rows, %d products, unitsSold=%d, blobSum=%d",
		got.rows, got.products, got.sumUnitsSold, got.blobByteSum)
}

// ── warehouse aggregates (blob-heavy type) ───────────────────────────────────

type whAgg struct {
	rows        int64
	sumInbound  int64
	blobByteSum uint64
}

func accWarehouse(a *whAgg, w warehouseDay) {
	a.rows++
	a.sumInbound += w.Inbound
	a.blobByteSum += byteSum(w.PickLatency) + byteSum(w.PackLatency) +
		byteSum(w.ShipLatency) + byteSum(w.DwellTime) + byteSum(w.Utilization)

	if w.Stats != nil {
		a.blobByteSum += byteSum(w.Stats.Throughput) + byteSum(w.Stats.Backlog)
	}
}

func writeWarehouseFile(tb testing.TB, path string, n, rowsPerRG int) whAgg {
	tb.Helper()

	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()

	w := parquet.NewGenericWriter[warehouseDay](f,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(int64(rowsPerRG)),
	)

	const batch = 4096

	rows := make([]warehouseDay, batch)

	var want whAgg

	for written := 0; written < n; {
		k := min(batch, n-written)
		for i := 0; i < k; i++ {
			wh := makeWarehouse(written + i + 1)
			rows[i] = wh
			accWarehouse(&want, wh)
		}

		if _, err := w.Write(rows[:k]); err != nil {
			tb.Fatalf("write: %v", err)
		}

		written += k
	}

	if err := w.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}

	return want
}

func TestWarehouseMillionStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-row scale test in -short mode")
	}

	path := filepath.Join(t.TempDir(), "warehouses.parquet")
	want := writeWarehouseFile(t, path, millionRows, 100_000)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	t.Logf("file: %d bytes on disk for %d rows", fi.Size(), millionRows)

	rd, err := parquetfast.NewReader[warehouseDay](f, fi.Size())
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()

	if rd.NumRows() != millionRows {
		t.Fatalf("NumRows = %d, want %d", rd.NumRows(), millionRows)
	}

	var got whAgg

	buf := make([]warehouseDay, 8192)
	for {
		n, err := rd.Read(buf)
		for i := 0; i < n; i++ {
			accWarehouse(&got, buf[i])
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}

	if got != want {
		t.Fatalf("aggregate mismatch:\n want %+v\n got  %+v", want, got)
	}

	t.Logf("decoded %d rows, sumInbound=%d, blobSum=%d", got.rows, got.sumInbound, got.blobByteSum)
}

// ── benchmark over the rich domain type (GenericReader vs Reader) ─────────────

func BenchmarkMerchantDecode(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in -short mode")
	}

	path := filepath.Join(b.TempDir(), "merch_bench.parquet")
	writeMerchantFile(b, path, 100_000, 25_000) // 4 row groups

	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatalf("read fixture: %v", err)
	}

	b.Logf("fixture: %d bytes", len(data))

	b.Run("parquet-go/GenericReader", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			r := parquet.NewGenericReader[merchantDay](bytes.NewReader(data))
			buf := make([]merchantDay, 4096)

			for {
				n, err := r.Read(buf)
				if n == 0 || err != nil {
					_ = r.Close()

					break
				}
			}
		}
	})

	b.Run("parquet-go-fast/Reader", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			rd, err := parquetfast.NewReader[merchantDay](bytes.NewReader(data), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}

			buf := make([]merchantDay, 4096)
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
