package parquetfast_test

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// remoteReaderAt simulates object storage: it serves bytes from an in-memory
// buffer via ReadAt (like an S3 GetObject with a Range header) and counts calls
// (round trips) and bytes transferred — the two things that cost money/latency
// on real remote storage.
type remoteReaderAt struct {
	data  []byte
	calls atomic.Int64
	bytes atomic.Int64
}

func (r *remoteReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.calls.Add(1)

	n := copy(p, r.data[off:])
	r.bytes.Add(int64(n))

	if n < len(p) {
		return n, bytesErrEOF
	}

	return n, nil
}

// bytesErrEOF mirrors what bytes.Reader.ReadAt returns for a short read.
var bytesErrEOF = bytesReaderEOF()

func bytesReaderEOF() error {
	_, err := bytes.NewReader(nil).ReadAt(make([]byte, 1), 0)

	return err
}

func TestRemote_DecodeMatchesBytes(t *testing.T) {
	data := writeServiceFixture(t, 500, 100)

	want, err := parquetfast.UnmarshalBytes[serviceRollup](data)
	if err != nil {
		t.Fatalf("bytes decode: %v", err)
	}

	rr := &remoteReaderAt{data: data}

	got, err := parquetfast.Unmarshal[serviceRollup](rr, int64(len(data)))
	if err != nil {
		t.Fatalf("remote decode: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("count mismatch: remote %d vs bytes %d", len(got), len(want))
	}

	t.Logf("full remote decode: %d ReadAt calls, %d bytes of %d", rr.calls.Load(), rr.bytes.Load(), len(data))
}

func TestRemote_ReaderAtFunc(t *testing.T) {
	data := writeServiceFixture(t, 100, 50)

	src := bytes.NewReader(data)
	ra := parquetfast.ReaderAtFunc(func(p []byte, off int64) (int, error) {
		return src.ReadAt(p, off)
	})

	got, err := parquetfast.Unmarshal[serviceRollup](ra, int64(len(data)))
	if err != nil {
		t.Fatalf("decode via ReaderAtFunc: %v", err)
	}

	if len(got) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(got))
	}
}

func TestRemote_ProjectionFetchesFewerBytes(t *testing.T) {
	data := writeServiceFixture(t, 2000, 200)

	// Full struct over remote.
	full := &remoteReaderAt{data: data}
	if _, err := parquetfast.Unmarshal[serviceRollup](full, int64(len(data))); err != nil {
		t.Fatalf("full: %v", err)
	}

	// Narrow projection over remote: only two scalar columns.
	type slim struct {
		Service string `parquet:"service"`
		Window  int64  `parquet:"window"`
	}

	narrow := &remoteReaderAt{data: data}
	if _, err := parquetfast.Unmarshal[slim](narrow, int64(len(data))); err != nil {
		t.Fatalf("narrow: %v", err)
	}

	t.Logf("remote bytes: full=%d, projected=%d (%.1f%%)",
		full.bytes.Load(), narrow.bytes.Load(),
		100*float64(narrow.bytes.Load())/float64(full.bytes.Load()))

	if narrow.bytes.Load() >= full.bytes.Load() {
		t.Fatalf("projection should fetch fewer bytes: %d >= %d", narrow.bytes.Load(), full.bytes.Load())
	}
}

func TestRemote_FilterFetchesFewerBytes(t *testing.T) {
	// Monotonic id across 20 row groups so a range predicate prunes most.
	rows := make([]filterRow, 2000)
	for i := range rows {
		rows[i] = makeFilterRow(i)
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(100))

	full := &remoteReaderAt{data: data}
	if _, err := parquetfast.Unmarshal[filterRow](full, int64(len(data))); err != nil {
		t.Fatalf("full: %v", err)
	}

	filt := &remoteReaderAt{data: data}
	got, err := parquetfast.Unmarshal[filterRow](filt, int64(len(data)),
		parquetfast.Where(parquetfast.Col("id").Between(int64(150), int64(160))))
	if err != nil {
		t.Fatalf("filtered: %v", err)
	}

	t.Logf("remote bytes: full=%d, filtered=%d (%.1f%%)",
		full.bytes.Load(), filt.bytes.Load(),
		100*float64(filt.bytes.Load())/float64(full.bytes.Load()))

	if filt.bytes.Load() >= full.bytes.Load() {
		t.Fatalf("filter should fetch fewer bytes: %d >= %d", filt.bytes.Load(), full.bytes.Load())
	}

	if len(got) != 11 {
		t.Fatalf("expected 11 rows, got %d", len(got))
	}
}

func TestRemote_OptimisticReadReducesRoundTrips(t *testing.T) {
	data := writeServiceFixture(t, 1000, 200)

	type slim struct {
		Service string `parquet:"service"`
	}

	base := &remoteReaderAt{data: data}
	if _, err := parquetfast.Unmarshal[slim](base, int64(len(data))); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	opt := &remoteReaderAt{data: data}
	if _, err := parquetfast.Unmarshal[slim](opt, int64(len(data)),
		parquetfast.WithOptimisticRead(), parquetfast.WithReadBufferSize(1<<20)); err != nil {
		t.Fatalf("optimistic: %v", err)
	}

	t.Logf("ReadAt calls: baseline=%d, optimistic=%d", base.calls.Load(), opt.calls.Load())

	if opt.calls.Load() > base.calls.Load() {
		t.Fatalf("optimistic read should not increase round trips: %d > %d", opt.calls.Load(), base.calls.Load())
	}
}
