package parquetfast

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Row filtering with predicate pushdown.
//
// Build predicates with Col(...) and compose them with And/Or; pass the result
// via Where(...). Decoding then:
//   - prunes whole row groups whose column statistics (min/max, null count) or
//     bloom filter prove they cannot match — those pages are never fetched,
//     decompressed, or decoded;
//   - prunes individual pages within a surviving group via the page index, and
//     seeks past the rest;
//   - returns only the rows that match.
//
// A predicate column does not need to be a field of the destination type — you
// can filter on a column you don't decode.

type predOp uint8

const (
	opEq predOp = iota
	opNe
	opLt
	opLe
	opGt
	opGe
	opBetween
)

type predKind uint8

const (
	predLeaf predKind = iota
	predAnd
	predOr
	predNot
)

// Predicate is a row filter: a leaf column comparison (built with Col(...)) or a
// boolean combination (And/Or) of predicates.
type Predicate struct {
	kind predKind

	// leaf
	path []string
	op   predOp
	lo   any
	hi   any // opBetween only

	// composite
	children []Predicate
}

// ColRef references a column by its parquet path, for building leaf predicates.
type ColRef struct{ path []string }

// Col references a column by parquet path: Col("region") or Col("a", "b") for a
// nested field.
func Col(path ...string) ColRef { return ColRef{path: append([]string(nil), path...)} }

func (c ColRef) leaf(op predOp, lo, hi any) Predicate {
	return Predicate{kind: predLeaf, path: c.path, op: op, lo: lo, hi: hi}
}

// Equal matches rows where the column equals v.
func (c ColRef) Equal(v any) Predicate { return c.leaf(opEq, v, nil) }

// NotEqual matches rows where the column does not equal v. (NULL never matches.)
func (c ColRef) NotEqual(v any) Predicate { return c.leaf(opNe, v, nil) }

// Less matches rows where the column is < v.
func (c ColRef) Less(v any) Predicate { return c.leaf(opLt, v, nil) }

// LessOrEqual matches rows where the column is <= v.
func (c ColRef) LessOrEqual(v any) Predicate { return c.leaf(opLe, v, nil) }

// Greater matches rows where the column is > v.
func (c ColRef) Greater(v any) Predicate { return c.leaf(opGt, v, nil) }

// GreaterOrEqual matches rows where the column is >= v.
func (c ColRef) GreaterOrEqual(v any) Predicate { return c.leaf(opGe, v, nil) }

// Between matches rows where lo <= column <= hi (inclusive).
func (c ColRef) Between(lo, hi any) Predicate { return c.leaf(opBetween, lo, hi) }

// And matches rows satisfying all of preds. Nestable with Or.
func And(preds ...Predicate) Predicate { return Predicate{kind: predAnd, children: preds} }

// Or matches rows satisfying any of preds. Nestable with And.
func Or(preds ...Predicate) Predicate { return Predicate{kind: predOr, children: preds} }

// Not matches rows that do NOT satisfy p. It is normalized at compile time by
// pushing the negation down to the leaves (De Morgan), so pruning still applies.
// Note NULL values never match a value predicate, so Not(Col(x).Equal(v)) is
// x != v over non-null rows (NULLs are excluded), matching SQL semantics.
func Not(p Predicate) Predicate { return Predicate{kind: predNot, children: []Predicate{p}} }

// Where keeps only rows matching the given predicates (multiple are ANDed; use
// And/Or to nest). Row groups and pages that cannot match are skipped without
// reading their data. Add WithConcurrency(n) to filter row groups in parallel.
// NULL column values never match a value predicate.
func Where(preds ...Predicate) Option {
	return func(c *config) { c.predicates = append(c.predicates, preds...) }
}

// compiledPredicate is a predicate tree resolved against a concrete file schema.
type compiledPredicate struct {
	kind predKind

	// leaf
	col int
	typ parquet.Type
	op  predOp
	lo  parquet.Value
	hi  parquet.Value

	// composite
	children []compiledPredicate
}

// compileRoot compiles the top-level predicate list (ANDed) into one tree,
// after normalizing away Not nodes.
func compileRoot(schema *parquet.Schema, preds []Predicate) (compiledPredicate, error) {
	root := Predicate{kind: predAnd, children: preds}
	if len(preds) == 1 {
		root = preds[0]
	}

	return compilePredicate(schema, normalize(root))
}

// normalize returns an equivalent predicate tree with no Not nodes (negation
// pushed down to the leaves via De Morgan).
func normalize(p Predicate) Predicate {
	switch p.kind {
	case predNot:
		return negate(p.children[0])
	case predAnd, predOr:
		children := make([]Predicate, len(p.children))
		for i := range p.children {
			children[i] = normalize(p.children[i])
		}

		return Predicate{kind: p.kind, children: children}
	default:
		return p
	}
}

// negate returns the De Morgan negation of p as a normalized (Not-free) tree.
func negate(p Predicate) Predicate {
	switch p.kind {
	case predNot:
		return normalize(p.children[0]) // !!x = x
	case predAnd: // !(a AND b ...) = !a OR !b ...
		return Predicate{kind: predOr, children: negateEach(p.children)}
	case predOr: // !(a OR b ...) = !a AND !b ...
		return Predicate{kind: predAnd, children: negateEach(p.children)}
	default:
		return negateLeaf(p)
	}
}

func negateEach(in []Predicate) []Predicate {
	out := make([]Predicate, len(in))
	for i := range in {
		out[i] = negate(in[i])
	}

	return out
}

// negateLeaf flips a leaf comparison to its complement.
func negateLeaf(p Predicate) Predicate {
	c := ColRef{path: p.path}

	switch p.op {
	case opEq:
		return c.NotEqual(p.lo)
	case opNe:
		return c.Equal(p.lo)
	case opLt:
		return c.GreaterOrEqual(p.lo)
	case opLe:
		return c.Greater(p.lo)
	case opGt:
		return c.LessOrEqual(p.lo)
	case opGe:
		return c.Less(p.lo)
	case opBetween: // !(lo <= x <= hi) = x < lo OR x > hi
		return Or(c.Less(p.lo), c.Greater(p.hi))
	}

	return p
}

func compilePredicate(schema *parquet.Schema, p Predicate) (compiledPredicate, error) {
	if p.kind != predLeaf {
		children := make([]compiledPredicate, len(p.children))

		for i, c := range p.children {
			cc, err := compilePredicate(schema, c)
			if err != nil {
				return compiledPredicate{}, err
			}

			children[i] = cc
		}

		return compiledPredicate{kind: p.kind, children: children}, nil
	}

	leaf, ok := schema.Lookup(p.path...)
	if !ok {
		return compiledPredicate{}, fmt.Errorf("parquet-go-fast: filter column %q not found in schema", strings.Join(p.path, "."))
	}

	typ := leaf.Node.Type()

	lo, err := toValue(typ, p.lo)
	if err != nil {
		return compiledPredicate{}, fmt.Errorf("parquet-go-fast: filter on %q: %w", strings.Join(p.path, "."), err)
	}

	cp := compiledPredicate{kind: predLeaf, col: leaf.ColumnIndex, typ: typ, op: p.op, lo: lo}

	if p.op == opBetween {
		hi, err := toValue(typ, p.hi)
		if err != nil {
			return compiledPredicate{}, fmt.Errorf("parquet-go-fast: filter on %q: %w", strings.Join(p.path, "."), err)
		}

		cp.hi = hi
	}

	return cp, nil
}

// leafCols appends every leaf column referenced by the tree.
func (cp *compiledPredicate) leafCols(out []int) []int {
	if cp.kind == predLeaf {
		return append(out, cp.col)
	}

	for i := range cp.children {
		out = cp.children[i].leafCols(out)
	}

	return out
}

// keepRowGroup reports whether rg might contain a row matching the tree.
// AND keeps only if every child keeps; OR keeps if any child keeps.
func (cp *compiledPredicate) keepRowGroup(rg parquet.RowGroup) bool {
	switch cp.kind {
	case predAnd:
		for i := range cp.children {
			if !cp.children[i].keepRowGroup(rg) {
				return false
			}
		}

		return true
	case predOr:
		for i := range cp.children {
			if cp.children[i].keepRowGroup(rg) {
				return true
			}
		}

		return false
	default:
		return cp.leafKeepRowGroup(rg)
	}
}

// matchNode evaluates the tree against one row's per-column values.
func (cp *compiledPredicate) matchNode(leafVals [][]parquet.Value) bool {
	switch cp.kind {
	case predAnd:
		for i := range cp.children {
			if !cp.children[i].matchNode(leafVals) {
				return false
			}
		}

		return true
	case predOr:
		for i := range cp.children {
			if cp.children[i].matchNode(leafVals) {
				return true
			}
		}

		return false
	default:
		if cp.col < 0 || cp.col >= len(leafVals) {
			return false
		}

		vs := leafVals[cp.col]
		if len(vs) == 0 {
			return false
		}

		return cp.matchRow(vs[0])
	}
}

// rangesFor returns the group-local row ranges that could contain a matching
// row. AND intersects children's ranges; OR unions them; a leaf with no page
// index contributes the whole group (no narrowing).
func (cp *compiledPredicate) rangesFor(rg parquet.RowGroup, groupRows int64) [][2]int64 {
	switch cp.kind {
	case predAnd:
		cur := [][2]int64{{0, groupRows}}

		for i := range cp.children {
			cur = intersectRanges(cur, cp.children[i].rangesFor(rg, groupRows))
			if len(cur) == 0 {
				return cur
			}
		}

		return cur
	case predOr:
		var cur [][2]int64

		for i := range cp.children {
			cur = unionRanges(cur, cp.children[i].rangesFor(rg, groupRows))
		}

		return cur
	default:
		if pr, ok := cp.pageRanges(rg, groupRows); ok {
			return pr
		}

		return [][2]int64{{0, groupRows}}
	}
}

// candidateRanges returns the group-local row ranges that could contain a row
// matching the predicate tree.
func candidateRanges(rg parquet.RowGroup, root *compiledPredicate) [][2]int64 {
	return root.rangesFor(rg, rg.NumRows())
}

// leafKeepRowGroup is the leaf-level row-group test using column statistics
// (min/max, null count) and — for equality — the bloom filter.
func (cp *compiledPredicate) leafKeepRowGroup(rg parquet.RowGroup) bool {
	chunks := rg.ColumnChunks()
	if cp.col >= len(chunks) {
		return true
	}

	chunk := chunks[cp.col]

	fcc, ok := chunk.(*parquet.FileColumnChunk)
	if !ok {
		return true // no access to stats — can't prune
	}

	// Entirely null → no non-null value can satisfy a value predicate.
	if nv := fcc.NumValues(); nv > 0 && fcc.NullCount() >= nv {
		return false
	}

	minV, maxV, ok := fcc.Bounds()
	if !ok {
		// No min/max stats; bloom can still prune an equality miss.
		if cp.op == opEq {
			return cp.bloomKeep(chunk)
		}

		return true
	}

	switch cp.op {
	case opEq:
		if cp.typ.Compare(cp.lo, minV) < 0 || cp.typ.Compare(cp.lo, maxV) > 0 {
			return false
		}

		return cp.bloomKeep(chunk)
	case opNe:
		// Prune only if every value equals lo (min == max == lo).
		return cp.typ.Compare(minV, cp.lo) != 0 || cp.typ.Compare(maxV, cp.lo) != 0
	case opLt:
		return cp.typ.Compare(minV, cp.lo) < 0
	case opLe:
		return cp.typ.Compare(minV, cp.lo) <= 0
	case opGt:
		return cp.typ.Compare(maxV, cp.lo) > 0
	case opGe:
		return cp.typ.Compare(maxV, cp.lo) >= 0
	case opBetween:
		// Overlap: hi >= min AND lo <= max.
		return cp.typ.Compare(cp.hi, minV) >= 0 && cp.typ.Compare(cp.lo, maxV) <= 0
	}

	return true
}

func (cp *compiledPredicate) bloomKeep(chunk parquet.ColumnChunk) bool {
	bf := chunk.BloomFilter()
	if bf == nil {
		return true
	}

	present, err := bf.Check(cp.lo)
	if err != nil {
		return true // treat bloom errors as "can't prune"
	}

	return present
}

// pageCanMatch reports whether page p of the leaf's column might contain a
// matching value, using the page index's per-page min/max and null flag.
func (cp *compiledPredicate) pageCanMatch(ci parquet.ColumnIndex, p int) bool {
	if ci.NullPage(p) {
		return false // all-null page can't satisfy a value predicate
	}

	minV := ci.MinValue(p)
	maxV := ci.MaxValue(p)

	switch cp.op {
	case opEq:
		return cp.typ.Compare(cp.lo, minV) >= 0 && cp.typ.Compare(cp.lo, maxV) <= 0
	case opNe:
		return cp.typ.Compare(minV, cp.lo) != 0 || cp.typ.Compare(maxV, cp.lo) != 0
	case opLt:
		return cp.typ.Compare(minV, cp.lo) < 0
	case opLe:
		return cp.typ.Compare(minV, cp.lo) <= 0
	case opGt:
		return cp.typ.Compare(maxV, cp.lo) > 0
	case opGe:
		return cp.typ.Compare(maxV, cp.lo) >= 0
	case opBetween:
		return cp.typ.Compare(cp.hi, minV) >= 0 && cp.typ.Compare(cp.lo, maxV) <= 0
	}

	return true
}

// pageRanges returns the group-local row ranges of pages in the leaf's column
// that might match. ok is false when no page index is available.
func (cp *compiledPredicate) pageRanges(rg parquet.RowGroup, groupRows int64) ([][2]int64, bool) {
	chunks := rg.ColumnChunks()
	if cp.col < 0 || cp.col >= len(chunks) {
		return nil, false
	}

	cc, ok := chunks[cp.col].(*parquet.FileColumnChunk)
	if !ok {
		return nil, false
	}

	ci, err := cc.ColumnIndex()
	if err != nil || ci == nil {
		return nil, false
	}

	oi, err := cc.OffsetIndex()
	if err != nil || oi == nil {
		return nil, false
	}

	np := ci.NumPages()
	if np == 0 || oi.NumPages() != np {
		return nil, false
	}

	ranges := make([][2]int64, 0, np)

	for p := 0; p < np; p++ {
		if !cp.pageCanMatch(ci, p) {
			continue
		}

		first := oi.FirstRowIndex(p)

		last := groupRows
		if p+1 < np {
			last = oi.FirstRowIndex(p + 1)
		}

		ranges = append(ranges, [2]int64{first, last})
	}

	return mergeRanges(ranges), true
}

// mergeRanges merges adjacent/overlapping ranges (input ordered by start).
func mergeRanges(in [][2]int64) [][2]int64 {
	if len(in) == 0 {
		return nil
	}

	out := make([][2]int64, 0, len(in))
	out = append(out, in[0])

	for _, r := range in[1:] {
		last := &out[len(out)-1]
		if r[0] <= last[1] {
			if r[1] > last[1] {
				last[1] = r[1]
			}
		} else {
			out = append(out, r)
		}
	}

	return out
}

// intersectRanges intersects two sorted, disjoint range lists.
func intersectRanges(a, b [][2]int64) [][2]int64 {
	var out [][2]int64

	i, j := 0, 0
	for i < len(a) && j < len(b) {
		lo := max(a[i][0], b[j][0])
		hi := min(a[i][1], b[j][1])

		if lo < hi {
			out = append(out, [2]int64{lo, hi})
		}

		if a[i][1] < b[j][1] {
			i++
		} else {
			j++
		}
	}

	return out
}

// unionRanges returns the sorted, coalesced union of two range lists.
func unionRanges(a, b [][2]int64) [][2]int64 {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	merged := make([][2]int64, 0, len(a)+len(b))
	merged = append(merged, a...)
	merged = append(merged, b...)
	sort.Slice(merged, func(i, j int) bool { return merged[i][0] < merged[j][0] })

	return mergeRanges(merged)
}

// matchRow reports whether a single (non-null) row value satisfies the leaf.
func (cp *compiledPredicate) matchRow(v parquet.Value) bool {
	if v.IsNull() {
		return false
	}

	switch cp.op {
	case opEq:
		return cp.typ.Compare(v, cp.lo) == 0
	case opNe:
		return cp.typ.Compare(v, cp.lo) != 0
	case opLt:
		return cp.typ.Compare(v, cp.lo) < 0
	case opLe:
		return cp.typ.Compare(v, cp.lo) <= 0
	case opGt:
		return cp.typ.Compare(v, cp.lo) > 0
	case opGe:
		return cp.typ.Compare(v, cp.lo) >= 0
	case opBetween:
		return cp.typ.Compare(v, cp.lo) >= 0 && cp.typ.Compare(v, cp.hi) <= 0
	}

	return false
}

// toValue converts a Go comparison value to a parquet.Value matching the
// column's physical type, so Type.Compare works against stored values.
func toValue(typ parquet.Type, v any) (parquet.Value, error) {
	if t, ok := v.(time.Time); ok {
		return timeToValue(typ, t)
	}

	switch typ.Kind() {
	case parquet.Boolean:
		b, ok := v.(bool)
		if !ok {
			return parquet.Value{}, fmt.Errorf("expected bool, got %T", v)
		}

		return parquet.BooleanValue(b), nil
	case parquet.Int32:
		n, ok := asInt64(v)
		if !ok {
			return parquet.Value{}, fmt.Errorf("expected integer, got %T", v)
		}

		return parquet.Int32Value(int32(n)), nil
	case parquet.Int64:
		n, ok := asInt64(v)
		if !ok {
			return parquet.Value{}, fmt.Errorf("expected integer, got %T", v)
		}

		return parquet.Int64Value(n), nil
	case parquet.Float:
		f, ok := asFloat(v)
		if !ok {
			return parquet.Value{}, fmt.Errorf("expected number, got %T", v)
		}

		return parquet.FloatValue(float32(f)), nil
	case parquet.Double:
		f, ok := asFloat(v)
		if !ok {
			return parquet.Value{}, fmt.Errorf("expected number, got %T", v)
		}

		return parquet.DoubleValue(f), nil
	case parquet.ByteArray:
		b, ok := asBytes(v)
		if !ok {
			return parquet.Value{}, fmt.Errorf("expected string or []byte, got %T", v)
		}

		return parquet.ByteArrayValue(b), nil
	default:
		return parquet.Value{}, fmt.Errorf("filtering not supported for column kind %s", typ.Kind())
	}
}

func timeToValue(typ parquet.Type, t time.Time) (parquet.Value, error) {
	kind, ok := timeKindFor(typ, false)
	if !ok {
		return parquet.Value{}, fmt.Errorf("column is not a time/date type")
	}

	switch kind {
	case kindTimeMillis:
		return parquet.Int64Value(t.UnixMilli()), nil
	case kindTimeMicros:
		return parquet.Int64Value(t.UnixMicro()), nil
	case kindTimeDate:
		return parquet.Int32Value(int32(t.Unix() / secondsPerDay)), nil
	default: // kindTimeNanos
		return parquet.Int64Value(t.UnixNano()), nil
	}
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	}

	return 0, false
}

func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}

	if n, ok := asInt64(v); ok {
		return float64(n), true
	}

	return 0, false
}

func asBytes(v any) ([]byte, bool) {
	switch x := v.(type) {
	case string:
		return []byte(x), true
	case []byte:
		return x, true
	}

	return nil, false
}
