package parquetfast

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// This file holds the execution side of predicate filtering: running a filtered
// decode (materialize and streaming) over the row-group + page pruning that
// filter.go computes. filter.go owns the predicate model; this owns the loops.

// decodeFiltered prunes row groups + pages that can't match the predicates, then
// decodes the surviving rows and keeps only matching ones (file order). Backs the
// filtered path of Unmarshal / UnmarshalBytes / UnmarshalFile. Each surviving row
// group is filtered via filterGroup, which takes the columnar late-materialization
// path when eligible and the row+seek path otherwise.
func decodeFiltered[T any](f *parquet.File, cfg config, plan *Plan, mask []bool) ([]T, error) {
	fr, err := newFilteredReader(f, cfg, plan, mask)
	if err != nil {
		return nil, err
	}

	out := make([]T, 0)

	for i := range fr.groups {
		res, ferr := filterGroup[T](fr.groups[i], plan, &fr.root, cfg.batchSize)
		if ferr != nil {
			return nil, ferr
		}

		out = append(out, res...)
	}

	return out, nil
}

// decodeFilteredConcurrent filters across workers: each surviving row group is
// filtered independently into its own result slice, then the per-group results
// are concatenated in file order. Parallelism is per row group, so a single
// surviving group runs effectively sequentially.
func decodeFilteredConcurrent[T any](f *parquet.File, cfg config, plan *Plan, mask []bool, workers int) ([]T, error) {
	fr, err := newFilteredReader(f, cfg, plan, mask)
	if err != nil {
		return nil, err
	}

	groups := fr.groups
	if len(groups) == 0 {
		return []T{}, nil
	}

	results := make([][]T, len(groups))
	errs := make([]error, len(groups))

	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)

	w := min(workers, len(groups))
	for k := 0; k < w; k++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				i := int(next.Add(1)) - 1
				if i >= len(groups) {
					return
				}

				results[i], errs[i] = filterGroup[T](groups[i], plan, &fr.root, cfg.batchSize)
			}
		}()
	}

	wg.Wait()

	total := 0

	for i := range results {
		if errs[i] != nil {
			return nil, errs[i]
		}

		total += len(results[i])
	}

	// Concatenate per-group results in file order (copies struct headers only).
	out := make([]T, 0, total)
	for i := range results {
		out = append(out, results[i]...)
	}

	return out, nil
}

// filterGroup decodes one (masked) row group with page pruning and returns its
// matching rows in order. Each call uses its own reader and scratch, so groups
// can be filtered concurrently (the file's io.ReaderAt must allow concurrent
// ReadAt).
func filterGroup[T any](rg parquet.RowGroup, plan *Plan, root *compiledPredicate, batchSize int) (_ []T, err error) {
	ranges := candidateRanges(rg, root)
	if len(ranges) == 0 {
		return nil, nil
	}

	n := int(rg.NumRows())

	// Columnar late-materialization path: when the output is scalar-only and page
	// pruning didn't shrink the scan much (covered ≥ ~10% of the group), evaluate
	// the predicate over typed column buffers and materialize matches — far faster
	// than the boxed row path. The row+seek path below stays for heavily-pruned
	// (e.g. sorted-column) scans and for compound output / non-numeric predicates.
	if plan.scalarOnly() {
		covered := int64(0)
		for _, rng := range ranges {
			covered += rng[1] - rng[0]
		}

		if covered*10 >= int64(n) {
			if res, handled, cerr := filterGroupColumnar[T](rg, plan, root, n); handled {
				return res, cerr
			}
		}
	}

	upper := 0
	for _, rng := range ranges {
		upper += int(rng[1] - rng[0])
	}

	out := make([]T, upper)

	rows := rg.Rows()

	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close parquet rows: %w", cerr)
		}
	}()

	leafVals := make([][]parquet.Value, plan.NumLeaves())
	batch := make([]parquet.Row, batchSize)

	var zero T

	w := 0
	pos := int64(0)

	for _, rng := range ranges {
		if pos < rng[0] {
			if serr := rows.SeekToRow(rng[0]); serr != nil {
				return nil, fmt.Errorf("seek to row %d: %w", rng[0], serr)
			}

			pos = rng[0]
		}

		for pos < rng[1] {
			want := batchSize
			if int64(want) > rng[1]-pos {
				want = int(rng[1] - pos)
			}

			n, rerr := rows.ReadRows(batch[:want])
			for i := 0; i < n; i++ {
				pos++

				unflattenRow(batch[i], leafVals)

				if root.matchNode(leafVals) {
					out[w] = zero
					plan.applyDecoded(unsafe.Pointer(&out[w]), leafVals)
					w++
				}
			}

			if rerr != nil {
				if !errors.Is(rerr, io.EOF) {
					return nil, fmt.Errorf("read parquet rows: %w", rerr)
				}

				if n == 0 {
					return out[:w], nil
				}
			}
		}
	}

	return out[:w], nil
}

// filteredReader drives predicate-filtered decoding with row-group + page pruning
// and page-aligned seeks. Shared by the materialize-all filtered path and the
// streaming Reader. Not safe for concurrent use.
type filteredReader struct {
	groups   []parquet.RowGroup // row-group-pruned, masked
	root     compiledPredicate
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
	root, err := compileRoot(f.Schema(), cfg.predicates)
	if err != nil {
		return nil, err
	}

	// Predicate columns must be read even if the destination type omits them.
	if mask != nil {
		m := append([]bool(nil), mask...)
		for _, c := range root.leafCols(nil) {
			if c >= 0 && c < len(m) {
				m[c] = false
			}
		}

		mask = m
	}

	rgs := f.RowGroups()
	groups := make([]parquet.RowGroup, 0, len(rgs))

	for _, rg := range rgs {
		if root.keepRowGroup(rg) {
			groups = append(groups, NewMaskedRowGroup(rg, mask))
		}
	}

	batchSize := cfg.batchSize
	if batchSize < 1 {
		batchSize = 16
	}

	return &filteredReader{
		groups:   groups,
		root:     root,
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
			fr.ranges = candidateRanges(rg, &fr.root)
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

			if fr.root.matchNode(fr.leafVals) {
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
