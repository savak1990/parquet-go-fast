package parquetfast

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Reader streams rows of a parquet file into caller-sized batches of []T,
// reusing the destination across calls. Prefer it over Unmarshal for large
// files where holding the whole result set in memory is undesirable.
//
// A Reader is not safe for concurrent use; create one per goroutine. The
// underlying *Plan is cached and safe to share, so concurrent Readers over the
// same (type, schema) reuse one plan.
type Reader[T any] struct {
	file     *parquet.File
	plan     *Plan
	rows     parquet.Rows
	schema   *parquet.Schema
	numRows  int64
	leafVals [][]parquet.Value
	batch    []parquet.Row
	done     bool
}

// NewReader opens the parquet file in r (an io.ReaderAt of size bytes) and
// prepares a streaming decoder for T.
func NewReader[T any](r io.ReaderAt, size int64, opts ...Option) (*Reader[T], error) {
	cfg := newConfig(opts)

	f, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	rgs := f.RowGroups()

	var skip []bool
	if cfg.nullColSkip {
		skip = allNullCols(rgs, f.Schema())
	}

	plan, err := Compile(reflect.TypeFor[T](), f.Schema(), skip)
	if err != nil {
		return nil, fmt.Errorf("build plan for %s: %w", reflect.TypeFor[T]().Name(), err)
	}

	return &Reader[T]{
		file:     f,
		plan:     plan,
		rows:     openRows(rgs, skip),
		schema:   f.Schema(),
		numRows:  f.NumRows(),
		leafVals: make([][]parquet.Value, plan.NumLeaves()),
	}, nil
}

// NumRows returns the total row count reported by the file footer.
func (rd *Reader[T]) NumRows() int64 {
	return rd.numRows
}

// Schema returns the parquet schema of the file being read.
func (rd *Reader[T]) Schema() *parquet.Schema {
	return rd.schema
}

// File returns the underlying parquet.File, e.g. for reading key/value metadata
// via File.Lookup.
func (rd *Reader[T]) File() *parquet.File {
	return rd.file
}

// Read decodes up to len(dst) rows into dst, returning the number written. It
// returns io.EOF once the file is exhausted (possibly together with the final
// rows). A typical loop:
//
//	buf := make([]Row, 4096)
//	for {
//	    n, err := rd.Read(buf)
//	    process(buf[:n])
//	    if err == io.EOF { break }
//	    if err != nil { return err }
//	}
func (rd *Reader[T]) Read(dst []T) (int, error) {
	if rd.done {
		return 0, io.EOF
	}

	if len(dst) == 0 {
		return 0, nil
	}

	if cap(rd.batch) < len(dst) {
		rd.batch = make([]parquet.Row, len(dst))
	}

	batch := rd.batch[:len(dst)]

	n, rerr := rd.rows.ReadRows(batch)
	for i := 0; i < n; i++ {
		rd.plan.Apply(unsafe.Pointer(&dst[i]), batch[i], rd.leafVals)
	}

	if rerr != nil {
		if !errors.Is(rerr, io.EOF) {
			return n, fmt.Errorf("read parquet rows: %w", rerr)
		}

		// parquet-go reports io.EOF as soon as any column stream is exhausted,
		// even when sibling columns still hold rows. Only surface EOF once a
		// call makes no progress, so trailing rows are drained first.
		if n == 0 {
			rd.done = true

			return 0, io.EOF
		}
	}

	return n, nil
}

// Close releases the underlying row reader.
func (rd *Reader[T]) Close() error {
	if err := rd.rows.Close(); err != nil {
		return fmt.Errorf("close parquet rows: %w", err)
	}

	return nil
}
