package parquetfast_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

func streamDecode[T any](t *testing.T, data []byte, opts ...parquetfast.Option) []T {
	t.Helper()

	rd, err := parquetfast.NewReader[T](bytes.NewReader(data), int64(len(data)), opts...)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()

	return streamAll(t, rd)
}

func TestStreamConcurrent_Unfiltered(t *testing.T) {
	rows := make([]merchantDay, 1000)
	for i := range rows {
		rows[i] = makeMerchant(i+1, i%6)
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(80)) // ~13 row groups

	seq := streamDecode[merchantDay](t, data)
	if len(seq) != len(rows) {
		t.Fatalf("sequential stream got %d rows, want %d", len(seq), len(rows))
	}

	for _, workers := range []int{2, 4, 0 /* GOMAXPROCS */} {
		con := streamDecode[merchantDay](t, data, parquetfast.WithConcurrency(workers))

		if !reflect.DeepEqual(seq, con) {
			for i := range seq {
				if i >= len(con) || !reflect.DeepEqual(seq[i], con[i]) {
					t.Fatalf("concurrent stream(%d) differs at row %d", workers, i)
				}
			}

			t.Fatalf("concurrent stream(%d): length %d vs %d", workers, len(con), len(seq))
		}
	}
}

func TestStreamConcurrent_Filtered(t *testing.T) {
	rows := make([]filterRow, 4000)
	for i := range rows {
		rows[i] = makeFilterRow(i)
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(100)) // 40 row groups

	pred := parquetfast.Where(
		parquetfast.Col("id").Between(int64(250), int64(3750)),
		parquetfast.Col("region").Equal("eu"),
	)

	seq := streamDecode[filterRow](t, data, pred)

	for _, workers := range []int{2, 4, 0} {
		con := streamDecode[filterRow](t, data, pred, parquetfast.WithConcurrency(workers))

		if !reflect.DeepEqual(seq, con) {
			t.Fatalf("concurrent filtered stream(%d) differs (len %d vs %d)", workers, len(con), len(seq))
		}
	}

	// Cross-check against the materialize path.
	mat, err := parquetfast.UnmarshalBytes[filterRow](data, pred)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	if !reflect.DeepEqual(mat, seq) {
		t.Fatalf("stream vs materialize mismatch (len %d vs %d)", len(seq), len(mat))
	}
}

// Varying the caller buffer size must not change results (group boundaries fall
// at arbitrary points relative to dst).
func TestStreamConcurrent_BufferSizes(t *testing.T) {
	rows := make([]scalarRow, 2000)
	for i := range rows {
		rows[i] = scalarRow{S: "r", I64: int64(i), Bs: []byte{byte(i)}}
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(64)) // ~32 groups

	want, err := parquetfast.UnmarshalBytes[scalarRow](data)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	for _, bufSize := range []int{1, 7, 64, 100, 5000} {
		rd, err := parquetfast.NewReader[scalarRow](bytes.NewReader(data), int64(len(data)), parquetfast.WithConcurrency(4))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}

		var got []scalarRow

		buf := make([]scalarRow, bufSize)
		for {
			n, rerr := rd.Read(buf)
			got = append(got, buf[:n]...)

			if rerr != nil {
				break
			}
		}

		_ = rd.Close()

		if !reflect.DeepEqual(want, got) {
			t.Fatalf("bufSize=%d: got %d rows, want %d", bufSize, len(got), len(want))
		}
	}
}

// Closing after a partial read must not deadlock or leak goroutines.
func TestStreamConcurrent_PartialReadThenClose(t *testing.T) {
	rows := make([]merchantDay, 2000)
	for i := range rows {
		rows[i] = makeMerchant(i+1, i%4)
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(50)) // many groups

	rd, err := parquetfast.NewReader[merchantDay](bytes.NewReader(data), int64(len(data)), parquetfast.WithConcurrency(8))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	buf := make([]merchantDay, 10)
	if _, err := rd.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := rd.Close(); err != nil { // should return promptly, no hang
		t.Fatalf("close: %v", err)
	}
}

func BenchmarkStreamConcurrent(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in -short mode")
	}

	rows := make([]merchantDay, 200_000)
	for i := range rows {
		rows[i] = makeMerchant(i+1, i%6)
	}

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[merchantDay](&buf,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(10_000), // 20 row groups
	)
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()

	drain := func(b *testing.B, opts ...parquetfast.Option) {
		b.Helper()
		b.ReportAllocs()

		for b.Loop() {
			rd, err := parquetfast.NewReader[merchantDay](bytes.NewReader(data), int64(len(data)), opts...)
			if err != nil {
				b.Fatal(err)
			}

			dst := make([]merchantDay, 4096)
			for {
				n, rerr := rd.Read(dst)
				if n == 0 || rerr != nil {
					break
				}
			}

			_ = rd.Close()
		}
	}

	b.Run("sequential", func(b *testing.B) { drain(b) })
	b.Run("concurrent", func(b *testing.B) { drain(b, parquetfast.WithConcurrency(0)) })
}
