// Package parquetfast is a high-performance, reflection-free-on-the-hot-path
// parquet decoder.
//
// It compiles a Go struct + parquet schema into a flat plan once (Compile),
// then decodes each row by writing through unsafe.Pointer at precompiled field
// offsets (Plan.Apply) — no schema conversion, no per-row reflection. Compared
// to parquet-go's reflection-driven GenericReader it is markedly faster and
// allocates far less, because the reflection walk happens once per (Go type,
// schema) shape instead of per row.
//
// Quickstart:
//
//	type Row struct {
//	    Name   string            `parquet:"name"`
//	    Count  int64             `parquet:"count"`
//	    Labels map[string]string `parquet:"labels"`
//	}
//
//	rows, err := parquetfast.UnmarshalFile[Row]("data.parquet")
//
// The struct tags are the same `parquet:"..."` tags parquet-go's writer reads,
// so a file written by parquet.GenericWriter[Row] round-trips through
// UnmarshalBytes[Row].
//
// It depends only on github.com/parquet-go/parquet-go (no fork, no replace
// directive) and reads files written by any spec-conformant writer (parquet-go,
// Arrow, Spark, DuckDB, …).
package parquetfast

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// config holds decode options.
type config struct {
	batchSize   int
	nullColSkip bool
	projection  bool
	concurrency int
	predicates  []Predicate
	fileOptions []parquet.FileOption
}

func newConfig(opts []Option) config {
	c := config{batchSize: 16, nullColSkip: true, projection: true, concurrency: 1}
	for _, o := range opts {
		o(&c)
	}

	if c.batchSize < 1 {
		c.batchSize = 16
	}

	return c
}

// workers resolves the requested concurrency to a positive worker count.
// n <= 0 means GOMAXPROCS.
func (c config) workers() int {
	if c.concurrency <= 0 {
		return runtime.GOMAXPROCS(0)
	}

	return c.concurrency
}

// Option customizes decoding.
type Option func(*config)

// WithBatchSize sets the number of rows pulled from parquet-go per ReadRows call
// (default 16, the value benchmarks favored across file sizes).
func WithBatchSize(n int) Option {
	return func(c *config) { c.batchSize = n }
}

// WithoutNullColumnSkip disables the optimization that bypasses parquet-go's
// read pipeline for columns proven 100% null in the file.
func WithoutNullColumnSkip() Option {
	return func(c *config) { c.nullColSkip = false }
}

// WithoutColumnProjection disables column projection. By default, only the
// columns your Go type maps to are read from the file — any other column is
// skipped in the read pipeline (no page fetch, decompression, or decode). Decode
// into a struct with a subset of the file's fields to read just those columns.
// This option turns that off and reads every column (the result is identical;
// only the work differs).
func WithoutColumnProjection() Option {
	return func(c *config) { c.projection = false }
}

// WithConcurrency decodes one file across n worker goroutines, each handling a
// subset of the file's row groups and writing into a disjoint region of the
// result. Default is 1 (sequential). n <= 0 means runtime.GOMAXPROCS.
//
// Speedup scales with the number of row groups: a single-row-group file is
// always decoded sequentially. When n > 1, the io.ReaderAt passed to Unmarshal
// MUST support concurrent ReadAt calls — *os.File and *bytes.Reader do, so
// UnmarshalFile and UnmarshalBytes are always safe.
func WithConcurrency(n int) Option {
	return func(c *config) { c.concurrency = n }
}

// Unmarshal decodes every row of the parquet file in r into a []T. r must be an
// io.ReaderAt of exactly size bytes (e.g. *bytes.Reader, *os.File).
func Unmarshal[T any](r io.ReaderAt, size int64, opts ...Option) ([]T, error) {
	cfg := newConfig(opts)

	f, err := parquet.OpenFile(r, size, cfg.fileOptions...)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	return decodeFile[T](f, cfg)
}

// UnmarshalBytes is Unmarshal over an in-memory parquet file.
func UnmarshalBytes[T any](b []byte, opts ...Option) ([]T, error) {
	return Unmarshal[T](bytes.NewReader(b), int64(len(b)), opts...)
}

// UnmarshalFile opens the parquet file at path, decodes every row into a []T,
// and closes the file. The simplest entry point when you have a file on disk and
// want the whole result set; for very large files, prefer the streaming Reader.
func UnmarshalFile[T any](path string, opts ...Option) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	return Unmarshal[T](f, fi.Size(), opts...)
}

func decodeFile[T any](f *parquet.File, cfg config) ([]T, error) {
	rt := reflect.TypeFor[T]()
	rgs := f.RowGroups()

	var skip []bool
	if cfg.nullColSkip {
		skip = allNullCols(rgs, f.Schema())
	}

	plan, err := Compile(rt, f.Schema(), skip)
	if err != nil {
		return nil, fmt.Errorf("build plan for %s: %w", rt.Name(), err)
	}

	// Column projection: mask every leaf column the plan doesn't read so its
	// pages are never fetched/decompressed/decoded. This subsumes the all-null
	// skip (all-null columns are never referenced). The result is identical; only
	// the work differs.
	mask := skip
	if cfg.projection {
		if m := plan.unreferencedMask(); m != nil {
			mask = m
		}
	}

	// Row filtering: prune row groups by statistics, decode + filter the rest.
	if len(cfg.predicates) > 0 {
		return decodeFiltered[T](f, cfg, plan, mask)
	}

	total := int(f.NumRows())
	out := make([]T, total)

	if total == 0 || len(rgs) == 0 {
		return out[:0], nil
	}

	workers := cfg.workers()
	if workers > 1 && len(rgs) > 1 {
		if err := decodeConcurrent(rgs, plan, mask, out, cfg.batchSize, min(workers, len(rgs))); err != nil {
			return nil, err
		}

		return out, nil
	}

	if err := decodeInto(rgs, plan, mask, out, cfg.batchSize); err != nil {
		return nil, err
	}

	return out, nil
}

// decodeInto opens the (masked, possibly multi-) row-group stream and decodes
// every row into out, verifying the row count.
func decodeInto[T any](rgs []parquet.RowGroup, plan *Plan, skip []bool, out []T, batchSize int) (err error) {
	rows := openRows(rgs, skip)

	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close parquet rows: %w", cerr)
		}
	}()

	read, err := decodeAllRows(rows, plan, out, batchSize)
	if err != nil {
		return err
	}

	if read != len(out) {
		return fmt.Errorf("decoded %d rows but file reports %d (possible file corruption)", read, len(out))
	}

	return nil
}

// openRows returns a row reader over rgs with 100%-null columns masked out.
// Single-row-group files take a direct path; multi-row-group files are combined
// via parquet.MultiRowGroup after each group is masked.
func openRows(rgs []parquet.RowGroup, skip []bool) parquet.Rows {
	if len(rgs) == 1 {
		return NewMaskedRowGroup(rgs[0], skip).Rows()
	}

	masked := make([]parquet.RowGroup, len(rgs))
	for i, rg := range rgs {
		masked[i] = NewMaskedRowGroup(rg, skip)
	}

	return parquet.MultiRowGroup(masked...).Rows()
}

// decodeConcurrent decodes the row groups across `workers` goroutines, each
// pulling the next row group from a shared counter and writing into that group's
// disjoint region of out. Requires the file's io.ReaderAt to support concurrent
// ReadAt (parquet-go reads each row group through an independent SectionReader).
func decodeConcurrent[T any](rgs []parquet.RowGroup, plan *Plan, skip []bool, out []T, batchSize, workers int) error {
	// Per-row-group output offsets.
	offsets := make([]int, len(rgs))
	off := 0

	for i, rg := range rgs {
		offsets[i] = off
		off += int(rg.NumRows())
	}

	var (
		next     atomic.Int64
		failed   atomic.Bool
		errMu    sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)

	record := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()

		failed.Store(true)
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Per-worker scratch, reused across the row groups this worker takes.
			rowBatch := make([]parquet.Row, batchSize)
			leafVals := make([][]parquet.Value, plan.NumLeaves())

			for {
				i := int(next.Add(1)) - 1
				if i >= len(rgs) || failed.Load() {
					return
				}

				want := int(rgs[i].NumRows())
				sub := out[offsets[i] : offsets[i]+want]

				rows := NewMaskedRowGroup(rgs[i], skip).Rows()
				read, err := decodeRowsInto(rows, plan, sub, rowBatch, leafVals)

				if cerr := rows.Close(); cerr != nil && err == nil {
					err = fmt.Errorf("close parquet rows: %w", cerr)
				}

				if err != nil {
					record(err)

					return
				}

				if read != want {
					record(fmt.Errorf("row group %d: decoded %d rows but group reports %d", i, read, want))

					return
				}
			}
		}()
	}

	wg.Wait()

	return firstErr
}

// decodeFiltered prunes row groups + pages that can't match the predicates, then
// decodes the surviving rows and keeps only matching ones (file order). Backs the
// filtered path of Unmarshal / UnmarshalBytes / UnmarshalFile.
func decodeFiltered[T any](f *parquet.File, cfg config, plan *Plan, mask []bool) (result []T, err error) {
	fr, err := newFilteredReader(f, cfg, plan, mask)
	if err != nil {
		return nil, err
	}

	defer func() {
		if cerr := fr.close(); cerr != nil && err == nil {
			err = fmt.Errorf("close parquet rows: %w", cerr)
		}
	}()

	// Upper bound: total rows of the surviving (row-group-pruned) groups.
	upper := 0
	for _, g := range fr.groups {
		upper += int(g.NumRows())
	}

	out := make([]T, upper)
	if upper == 0 {
		return out[:0], nil
	}

	n, rerr := filteredRead(fr, out)
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return nil, rerr
	}

	return out[:n], nil
}

// filteredReader drives predicate-filtered decoding with row-group + page pruning
// and page-aligned seeks. Shared by the materialize-all filtered path and the
// streaming Reader. Not safe for concurrent use.
type filteredReader struct {
	groups   []parquet.RowGroup // row-group-pruned, masked
	preds    []compiledPred
	plan     *Plan
	leafVals [][]parquet.Value
	batch    []parquet.Row

	gi     int          // current group index
	rows   parquet.Rows // current group's reader (nil between groups)
	ranges [][2]int64   // current group's page-pruned row ranges (group-local)
	ri     int          // current range index
	pos    int64        // next group-local row to read
}

// newFilteredReader compiles predicates, prunes row groups by statistics/bloom,
// and masks each surviving group (forcing predicate columns to be read).
func newFilteredReader(f *parquet.File, cfg config, plan *Plan, mask []bool) (*filteredReader, error) {
	preds, err := compilePredicates(f.Schema(), cfg.predicates)
	if err != nil {
		return nil, err
	}

	// Predicate columns must be read even if the destination type omits them.
	if mask != nil {
		m := append([]bool(nil), mask...)
		for _, p := range preds {
			if p.col >= 0 && p.col < len(m) {
				m[p.col] = false
			}
		}

		mask = m
	}

	rgs := f.RowGroups()
	groups := make([]parquet.RowGroup, 0, len(rgs))

	for _, rg := range rgs {
		if keepRowGroup(preds, rg) {
			groups = append(groups, NewMaskedRowGroup(rg, mask))
		}
	}

	batchSize := cfg.batchSize
	if batchSize < 1 {
		batchSize = 16
	}

	return &filteredReader{
		groups:   groups,
		preds:    preds,
		plan:     plan,
		leafVals: make([][]parquet.Value, plan.NumLeaves()),
		batch:    make([]parquet.Row, batchSize),
	}, nil
}

func (fr *filteredReader) close() error {
	if fr.rows == nil {
		return nil
	}

	err := fr.rows.Close()
	fr.rows = nil

	return err
}

// filteredRead fills dst with up to len(dst) matching rows, advancing across
// groups and page-pruned ranges (seeking over pruned pages). Returns io.EOF once
// all surviving groups are exhausted (possibly with the final rows).
func filteredRead[T any](fr *filteredReader, dst []T) (int, error) {
	var zero T

	w := 0

	for w < len(dst) {
		if fr.rows == nil {
			if fr.gi >= len(fr.groups) {
				return w, io.EOF
			}

			rg := fr.groups[fr.gi]
			fr.rows = rg.Rows()
			fr.ranges = candidateRanges(rg, fr.preds)
			fr.ri = 0
			fr.pos = 0
		}

		if fr.ri >= len(fr.ranges) {
			cerr := fr.rows.Close()
			fr.rows = nil
			fr.gi++

			if cerr != nil {
				return w, fmt.Errorf("close parquet rows: %w", cerr)
			}

			continue
		}

		rng := fr.ranges[fr.ri]

		// Seek over pruned pages to the start of this range (forward only).
		if fr.pos < rng[0] {
			if err := fr.rows.SeekToRow(rng[0]); err != nil {
				return w, fmt.Errorf("seek to row %d: %w", rng[0], err)
			}

			fr.pos = rng[0]
		}

		remain := rng[1] - fr.pos
		if remain <= 0 {
			fr.ri++

			continue
		}

		// Bound the read so we never decode past the range or overfill dst —
		// every row read is processed, so no rows are dropped on a full dst.
		want := len(fr.batch)
		if int64(want) > remain {
			want = int(remain)
		}

		if want > len(dst)-w {
			want = len(dst) - w
		}

		n, rerr := fr.rows.ReadRows(fr.batch[:want])
		for i := 0; i < n; i++ {
			fr.pos++

			unflattenRow(fr.batch[i], fr.leafVals)

			if matchAll(fr.preds, fr.leafVals) {
				dst[w] = zero // reset (dst may be a reused streaming buffer)
				fr.plan.applyDecoded(unsafe.Pointer(&dst[w]), fr.leafVals)
				w++
			}
		}

		if fr.pos >= rng[1] {
			fr.ri++
		}

		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return w, fmt.Errorf("read parquet rows: %w", rerr)
			}

			if n == 0 {
				fr.ri = len(fr.ranges) // group exhausted; advance to the next
			}
		}
	}

	return w, nil
}

// keepRowGroup reports whether rg survives all predicates (AND).
func keepRowGroup(preds []compiledPred, rg parquet.RowGroup) bool {
	for i := range preds {
		if !preds[i].keepRowGroup(rg) {
			return false
		}
	}

	return true
}

// matchAll reports whether the row's values satisfy every predicate. A predicate
// column with no value for the row (or a null) never matches.
func matchAll(preds []compiledPred, leafVals [][]parquet.Value) bool {
	for i := range preds {
		p := &preds[i]

		if p.col < 0 || p.col >= len(leafVals) {
			return false
		}

		vs := leafVals[p.col]
		if len(vs) == 0 || !p.matchRow(vs[0]) {
			return false
		}
	}

	return true
}

// decodeAllRows pulls rows in batches and applies plan to each, writing in place
// into out. Returns the number of rows decoded.
func decodeAllRows[T any](rows parquet.Rows, plan *Plan, out []T, batchSize int) (int, error) {
	rowBatch := make([]parquet.Row, batchSize)
	leafVals := make([][]parquet.Value, plan.NumLeaves())

	return decodeRowsInto(rows, plan, out, rowBatch, leafVals)
}

// decodeRowsInto pulls rows in batches using caller-provided scratch buffers and
// applies plan to each, writing in place into out. Returns the number decoded.
// Workers reuse their own buffers across row groups via this entry point.
func decodeRowsInto[T any](rows parquet.Rows, plan *Plan, out []T, rowBatch []parquet.Row, leafVals [][]parquet.Value) (int, error) {
	read := 0

	for {
		n, rerr := rows.ReadRows(rowBatch)
		for i := 0; i < n; i++ {
			if read+i >= len(out) {
				return read + i, fmt.Errorf("decoded rows exceed pre-allocated buffer of %d (possibly corrupted file)", len(out))
			}

			plan.Apply(unsafe.Pointer(&out[read+i]), rowBatch[i], leafVals)
		}

		read += n

		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return read, fmt.Errorf("read parquet rows: %w", rerr)
			}

			// rowGroupRows.ReadRows reports io.EOF as soon as ANY column's value
			// stream is exhausted, even when sibling columns still hold rows for
			// this batch and beyond. Treat EOF as terminal only when the call
			// made no progress, so trailing rows are drained instead of dropped.
			if n == 0 {
				return read, nil
			}
		}
	}
}

// allNullCols returns a bitmap of leaf columns proven 100% null across every row
// group, or nil if none qualify.
func allNullCols(rgs []parquet.RowGroup, schema *parquet.Schema) []bool {
	if len(rgs) == 0 {
		return nil
	}

	numLeaves := len(schema.Columns())
	skip := make([]bool, numLeaves)

	for col := range numLeaves {
		all := true

		for _, rg := range rgs {
			if !isColumnAllNull(rg, col) {
				all = false

				break
			}
		}

		skip[col] = all
	}

	if !anyTrue(skip) {
		return nil
	}

	return skip
}

func isColumnAllNull(rg parquet.RowGroup, col int) bool {
	chunks := rg.ColumnChunks()
	if col >= len(chunks) {
		return false
	}

	cc := chunks[col]
	if fcc, ok := cc.(*parquet.FileColumnChunk); ok {
		return fcc.NullCount() >= fcc.NumValues()
	}

	ci, err := cc.ColumnIndex()
	if err != nil || ci == nil {
		return false
	}

	var nulls int64
	for p := range ci.NumPages() {
		nulls += ci.NullCount(p)
	}

	return nulls >= cc.NumValues()
}

func anyTrue(b []bool) bool {
	for _, v := range b {
		if v {
			return true
		}
	}

	return false
}
