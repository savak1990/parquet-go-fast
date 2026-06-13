package parquetfast

import (
	"sync"

	"github.com/parquet-go/parquet-go"
)

// Shared machinery for compounds that iterate over per-entry sub-sections of the
// row — struct lists and struct-valued maps. The pattern:
//
//  1. At plan-build, classify the inner subtree's leaves as scalar (1 value per
//     outer entry) vs nested (variable cardinality per entry) via
//     collectSubtreeInfo.
//  2. At decode, the outer compound iterates entries by counting key/sentinel
//     values, fills a per-entry leafVals buffer (pooled), and runs the inner
//     Plan's applyDecoded against each entry's sub-struct.

const (
	pqtKeyValueSeg = "key_value"
	pqtKeySeg      = "key"
	pqtValueSeg    = "value"
	pqtListSeg     = "list"
	pqtElementSeg  = "element"

	scalarColsHint = 32
	nestedColsHint = 8
)

// subtreeInfo partitions the leaves under a value-subtree into scalar vs nested
// columns, relative to an outer repetition level.
//
// scalarCols carry exactly one parquet.Value per outer entry — accessible via a
// zero-allocation sub-slice (vs[i:i+1]).
// nestedCols have higher max-rep than the outer entry — variable cardinality per
// entry, must be split by repetition level via splitByRep.
type subtreeInfo struct {
	scalarCols []int
	nestedCols []int
}

// collectSubtreeInfo walks every leaf whose path starts with prefix and
// classifies it as "scalar" or "nested" relative to outerRep — the rep level of
// the outer compound's entry boundary. The result lets the per-entry decode loop
// use the cheap zero-alloc sub-slice path for scalar columns and reserve the
// splitByRep path for nested columns.
//
// Cold path (plan-build); two slice allocations sized via small capacity hints.
func collectSubtreeInfo(schema *parquet.Schema, prefix []string, numLeaves, outerRep int, skipCol []bool) subtreeInfo {
	info := subtreeInfo{
		scalarCols: make([]int, 0, scalarColsHint),
		nestedCols: make([]int, 0, nestedColsHint),
	}

	for _, p := range schema.Columns() {
		if !pathStartsWith(p, prefix) {
			continue
		}

		leaf, ok := schema.Lookup(p...)
		if !ok || !validColumn(leaf.ColumnIndex, numLeaves) {
			continue
		}

		if leaf.ColumnIndex < len(skipCol) && skipCol[leaf.ColumnIndex] {
			continue
		}

		if leaf.MaxRepetitionLevel > outerRep {
			info.nestedCols = append(info.nestedCols, leaf.ColumnIndex)
		} else {
			info.scalarCols = append(info.scalarCols, leaf.ColumnIndex)
		}
	}

	return info
}

// splitByRep splits a column's values for one row into per-outer-entry
// sub-slices, using rep-level boundaries to detect entry transitions.
//
//	vals     — values for a single leaf column for one row, in writer order.
//	boundary — the outer compound's entry-rep level. A value with
//	           RepetitionLevel <= boundary starts a new outer entry.
//
// Returns N sub-slices viewing into vals (no copy). Pre-sized via a counting
// scan so groups doesn't grow geometrically.
func splitByRep(vals []parquet.Value, boundary int) [][]parquet.Value {
	if len(vals) == 0 {
		return nil
	}

	n := 1

	for i := 1; i < len(vals); i++ {
		if vals[i].RepetitionLevel() <= boundary {
			n++
		}
	}

	groups := make([][]parquet.Value, 0, n)
	start := 0

	for i := 1; i < len(vals); i++ {
		if vals[i].RepetitionLevel() <= boundary {
			groups = append(groups, vals[start:i:i])
			start = i
		}
	}

	return append(groups, vals[start:len(vals):len(vals)])
}

// splitNestedLeaves splits only the nested columns; scalar columns are accessed
// directly via sub-slices. Returns nil when there are no nested cols.
func splitNestedLeaves(leafVals [][]parquet.Value, boundary, numLeaves int, nestedCols []int) [][][]parquet.Value {
	if len(nestedCols) == 0 {
		return nil
	}

	out := make([][][]parquet.Value, numLeaves)
	for _, col := range nestedCols {
		if col >= numLeaves || len(leafVals[col]) == 0 {
			continue
		}

		out[col] = splitByRep(leafVals[col], boundary)
	}

	return out
}

func resetEntryLeafVals(buf [][]parquet.Value, cols []int) {
	for _, col := range cols {
		if col < len(buf) {
			buf[col] = nil
		}
	}
}

func fillEntryScalarLeaves(buf, leafVals [][]parquet.Value, scalarCols []int, i int) {
	for _, col := range scalarCols {
		vs := leafVals[col]
		if i < len(vs) {
			buf[col] = vs[i : i+1 : i+1]
		}
	}
}

func fillEntryLeafVals(buf [][]parquet.Value, splits [][][]parquet.Value, cols []int, i int) {
	for _, col := range cols {
		if col >= len(splits) {
			continue
		}

		perEntry := splits[col]
		if i < len(perEntry) {
			buf[col] = perEntry[i]
		}
	}
}

// entryLeafValsPool reuses scratch [][]parquet.Value buffers across all entries
// of all compound handlers in a row. Without it, every compound would allocate a
// fresh per-entry leafVals slice (numLeaves wide) per outer entry.
var entryLeafValsPool = sync.Pool{
	New: func() any {
		s := make([][]parquet.Value, 0)

		return &s
	},
}

func getEntryLeafValsBuf(numLeaves int) *[][]parquet.Value {
	bp, ok := entryLeafValsPool.Get().(*[][]parquet.Value)
	if !ok {
		s := make([][]parquet.Value, numLeaves)

		return &s
	}

	if cap(*bp) < numLeaves {
		*bp = make([][]parquet.Value, numLeaves)
	} else {
		*bp = (*bp)[:numLeaves]
		for i := range *bp {
			(*bp)[i] = nil
		}
	}

	return bp
}

func putEntryLeafValsBuf(bp *[][]parquet.Value) {
	entryLeafValsPool.Put(bp)
}

// subtreeIterCtx bundles the per-row context needed by applyPerEntrySubPlan: the
// leaf classification, the outer rep level, and the schema's leaf count.
// Captured once at plan-build by each compound that drives per-entry iteration.
type subtreeIterCtx struct {
	info      subtreeInfo
	outerRep  int
	numLeaves int
}

// applyPerEntrySubPlan drives the per-entry iteration loop shared by typed
// struct-list and typed struct-valued-map fillers: pulls a pooled scratch
// buffer, splits nested cols once, and for each i in [0, n) populates the
// per-entry buf and invokes entryFn(buf, i).
//
// entryFn is responsible for invoking sub.applyDecoded against the entry's
// destination pointer and any post-decode bookkeeping (map insert, slice
// indexing, null-key skip).
func applyPerEntrySubPlan(leafVals [][]parquet.Value, ctx subtreeIterCtx, n int, entryFn func(buf [][]parquet.Value, i int)) {
	nestedSplits := splitNestedLeaves(leafVals, ctx.outerRep, ctx.numLeaves, ctx.info.nestedCols)
	bufp := getEntryLeafValsBuf(ctx.numLeaves)
	buf := *bufp

	for i := range n {
		resetEntryLeafVals(buf, ctx.info.scalarCols)
		resetEntryLeafVals(buf, ctx.info.nestedCols)
		fillEntryScalarLeaves(buf, leafVals, ctx.info.scalarCols, i)

		if nestedSplits != nil {
			fillEntryLeafVals(buf, nestedSplits, ctx.info.nestedCols, i)
		}

		entryFn(buf, i)
	}

	putEntryLeafValsBuf(bufp)
}

func appendPath(prefix []string, more ...string) []string {
	out := make([]string, 0, len(prefix)+len(more))
	out = append(out, prefix...)
	out = append(out, more...)

	return out
}

func pathStartsWith(p, prefix []string) bool {
	if len(p) < len(prefix) {
		return false
	}

	for i := range prefix {
		if p[i] != prefix[i] {
			return false
		}
	}

	return true
}

// repLevelOfPath returns the parquet repetition level of the node at path — the
// number of REPEATED nodes from root to path, including the node itself. Returns
// -1 if any segment of path doesn't resolve (schema-evolution case).
func repLevelOfPath(schema *parquet.Schema, path []string) int {
	var node parquet.Node = schema

	count := 0

	for _, seg := range path {
		next, found := childField(node, seg)
		if !found {
			return -1
		}

		if next.Repeated() {
			count++
		}

		node = next
	}

	return count
}

// defLevelOfPath returns the parquet definition level of the node at path — the
// number of Optional/Repeated nodes from root to path, including the node
// itself. Returns -1 if any segment of path doesn't resolve.
func defLevelOfPath(schema *parquet.Schema, path []string) int {
	var node parquet.Node = schema

	count := 0

	for _, seg := range path {
		next, found := childField(node, seg)
		if !found {
			return -1
		}

		if next.Optional() || next.Repeated() {
			count++
		}

		node = next
	}

	return count
}

func childField(node parquet.Node, name string) (parquet.Field, bool) {
	for _, f := range node.Fields() {
		if f.Name() == name {
			return f, true
		}
	}

	return nil, false
}
