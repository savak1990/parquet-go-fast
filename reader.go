package parquetfast

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
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

	// filtered is non-nil when Where(...) was passed: Read then streams only
	// matching rows, with row-group + page pruning. rows is unused in that mode.
	filtered *filteredReader

	// stream is non-nil when WithConcurrency(n>1) was passed: Read pulls rows
	// from a bounded, order-preserving pool of workers decoding whole row groups.
	stream *streamPipeline[T]
}

// groupResult carries one row group's decoded rows (or an error) to the consumer.
type groupResult[T any] struct {
	rows []T
	err  error
}

// streamPipeline decodes row groups across worker goroutines and delivers them to
// Read in file order, bounded to a small look-ahead window. Not safe for
// concurrent use by multiple callers; the file's io.ReaderAt must allow
// concurrent ReadAt.
type streamPipeline[T any] struct {
	ordered <-chan chan groupResult[T]
	quit    chan struct{}
	once    sync.Once

	cur    []T
	curPos int
	done   bool
}

// startStreamPipeline launches workers that decode each group via decodeOne and a
// dispatcher that hands out groups in file order, pushing each group's result
// channel onto ordered (buffered to bound look-ahead to ~workers groups).
func startStreamPipeline[T any](groups []parquet.RowGroup, workers int, decodeOne func(parquet.RowGroup) ([]T, error)) *streamPipeline[T] {
	ordered := make(chan chan groupResult[T], workers)
	quit := make(chan struct{})

	type task struct {
		rg parquet.RowGroup
		rc chan groupResult[T]
	}

	tasks := make(chan task)

	for w := 0; w < workers; w++ {
		go func() {
			for {
				select {
				case t, ok := <-tasks:
					if !ok {
						return
					}

					rows, err := decodeOne(t.rg)
					select {
					case t.rc <- groupResult[T]{rows: rows, err: err}:
					case <-quit:
						return
					}
				case <-quit:
					return
				}
			}
		}()
	}

	go func() {
		defer close(ordered)

		for _, rg := range groups {
			rc := make(chan groupResult[T], 1)

			select {
			case ordered <- rc: // backpressure: blocks when the window is full
			case <-quit:
				return
			}

			select {
			case tasks <- task{rg: rg, rc: rc}:
			case <-quit:
				return
			}
		}

		close(tasks)
	}()

	return &streamPipeline[T]{ordered: ordered, quit: quit}
}

// read drains the current group then the next ready groups (in file order) into
// dst. Returns io.EOF once all groups are delivered (possibly with final rows).
func (p *streamPipeline[T]) read(dst []T) (int, error) {
	if p.done {
		return 0, io.EOF
	}

	n := 0

	for n < len(dst) {
		if p.curPos >= len(p.cur) {
			rc, ok := <-p.ordered
			if !ok {
				p.done = true

				if n > 0 {
					return n, nil
				}

				return 0, io.EOF
			}

			res := <-rc
			if res.err != nil {
				p.done = true

				return n, res.err
			}

			p.cur = res.rows
			p.curPos = 0

			if len(p.cur) == 0 {
				continue
			}
		}

		c := copy(dst[n:], p.cur[p.curPos:])
		p.curPos += c
		n += c
	}

	return n, nil
}

func (p *streamPipeline[T]) close() {
	p.once.Do(func() { close(p.quit) })
}

// NewReader opens the parquet file in r (an io.ReaderAt of size bytes) and
// prepares a streaming decoder for T.
func NewReader[T any](r io.ReaderAt, size int64, opts ...Option) (*Reader[T], error) {
	cfg := newConfig(opts)

	f, err := parquet.OpenFile(r, size, cfg.fileOptions...)
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

	mask := skip
	if cfg.projection {
		if m := plan.unreferencedMask(); m != nil {
			mask = m
		}
	}

	rd := &Reader[T]{
		file:     f,
		plan:     plan,
		schema:   f.Schema(),
		numRows:  f.NumRows(),
		leafVals: make([][]parquet.Value, plan.NumLeaves()),
	}

	workers := cfg.workers()

	// Where(...) → stream only matching rows, with row-group + page pruning.
	if len(cfg.predicates) > 0 {
		fr, err := newFilteredReader(f, cfg, plan, mask)
		if err != nil {
			return nil, err
		}

		// Concurrent filtered streaming: filter row groups in parallel, deliver
		// matches in file order.
		if workers > 1 && len(fr.groups) > 1 {
			rd.stream = startStreamPipeline(fr.groups, min(workers, len(fr.groups)),
				func(rg parquet.RowGroup) ([]T, error) {
					return filterGroup[T](rg, plan, &fr.root, cfg.batchSize)
				})

			return rd, nil
		}

		rd.filtered = fr

		return rd, nil
	}

	// Concurrent (unfiltered) streaming: decode whole row groups in parallel,
	// deliver in file order. Memory ≈ concurrency × rows-per-row-group, so it is
	// most useful on files with many row groups.
	if workers > 1 && len(rgs) > 1 {
		masked := make([]parquet.RowGroup, len(rgs))
		for i, rg := range rgs {
			masked[i] = NewMaskedRowGroup(rg, mask)
		}

		rd.stream = startStreamPipeline(masked, min(workers, len(masked)),
			func(rg parquet.RowGroup) ([]T, error) {
				return decodeGroup[T](rg, plan, cfg.batchSize)
			})

		return rd, nil
	}

	rd.rows = openRows(rgs, mask)

	return rd, nil
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
	if rd.stream != nil {
		return rd.stream.read(dst)
	}

	if rd.done {
		return 0, io.EOF
	}

	if len(dst) == 0 {
		return 0, nil
	}

	if rd.filtered != nil {
		n, err := filteredRead(rd.filtered, dst)
		if errors.Is(err, io.EOF) {
			rd.done = true

			if n > 0 {
				return n, nil // surface the final rows now, EOF on the next call
			}
		}

		return n, err
	}

	if cap(rd.batch) < len(dst) {
		rd.batch = make([]parquet.Row, len(dst))
	}

	batch := rd.batch[:len(dst)]

	n, rerr := rd.rows.ReadRows(batch)

	// Apply fills present fields and leaves absent fields at the Go zero value;
	// it does not clear stale data. Since dst is reused across Read calls, each
	// slot must be reset first, or maps/slices from a prior row would leak in.
	var zero T

	for i := 0; i < n; i++ {
		dst[i] = zero
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
	if rd.stream != nil {
		rd.stream.close()

		return nil
	}

	if rd.filtered != nil {
		if err := rd.filtered.close(); err != nil {
			return fmt.Errorf("close parquet rows: %w", err)
		}

		return nil
	}

	if err := rd.rows.Close(); err != nil {
		return fmt.Errorf("close parquet rows: %w", err)
	}

	return nil
}
