package parquetfast

import (
	"fmt"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Row filtering with predicate pushdown.
//
// Build a predicate with Col(...) and pass it via Where(...). Decoding then:
//   - prunes whole row groups whose column statistics (min/max, null count) or
//     bloom filter prove they cannot match — those pages are never fetched,
//     decompressed, or decoded;
//   - decodes the surviving row groups and returns only the rows that match all
//     predicates.
//
// The predicate column does not need to be a field of the destination type — you
// can filter on a column you don't decode.

type predOp uint8

const (
	opEq predOp = iota
	opLt
	opLe
	opGt
	opGe
	opBetween
)

// Predicate is a single-column row filter. Build it via Col(...).
type Predicate struct {
	path []string
	op   predOp
	lo   any
	hi   any // opBetween only
}

// ColRef references a column by its parquet path, for building predicates.
type ColRef struct{ path []string }

// Col references a column by parquet path: Col("region") or Col("a", "b") for a
// nested field.
func Col(path ...string) ColRef { return ColRef{path: append([]string(nil), path...)} }

// Equal matches rows where the column equals v.
func (c ColRef) Equal(v any) Predicate { return Predicate{c.path, opEq, v, nil} }

// Less matches rows where the column is < v.
func (c ColRef) Less(v any) Predicate { return Predicate{c.path, opLt, v, nil} }

// LessOrEqual matches rows where the column is <= v.
func (c ColRef) LessOrEqual(v any) Predicate { return Predicate{c.path, opLe, v, nil} }

// Greater matches rows where the column is > v.
func (c ColRef) Greater(v any) Predicate { return Predicate{c.path, opGt, v, nil} }

// GreaterOrEqual matches rows where the column is >= v.
func (c ColRef) GreaterOrEqual(v any) Predicate { return Predicate{c.path, opGe, v, nil} }

// Between matches rows where lo <= column <= hi (inclusive).
func (c ColRef) Between(lo, hi any) Predicate { return Predicate{c.path, opBetween, lo, hi} }

// Where keeps only rows matching ALL given predicates. Row groups that cannot
// match are skipped without reading their pages. Combine with the usual options.
//
// Filtering currently runs sequentially (WithConcurrency is ignored when Where
// is set). NULL column values never match a value predicate.
func Where(preds ...Predicate) Option {
	return func(c *config) { c.predicates = append(c.predicates, preds...) }
}

// compiledPred is a predicate resolved against a concrete file schema.
type compiledPred struct {
	col int
	typ parquet.Type
	op  predOp
	lo  parquet.Value
	hi  parquet.Value
}

func compilePredicates(schema *parquet.Schema, preds []Predicate) ([]compiledPred, error) {
	out := make([]compiledPred, 0, len(preds))

	for _, p := range preds {
		leaf, ok := schema.Lookup(p.path...)
		if !ok {
			return nil, fmt.Errorf("parquet-go-fast: filter column %q not found in schema", strings.Join(p.path, "."))
		}

		typ := leaf.Node.Type()

		lo, err := toValue(typ, p.lo)
		if err != nil {
			return nil, fmt.Errorf("parquet-go-fast: filter on %q: %w", strings.Join(p.path, "."), err)
		}

		cp := compiledPred{col: leaf.ColumnIndex, typ: typ, op: p.op, lo: lo}

		if p.op == opBetween {
			hi, err := toValue(typ, p.hi)
			if err != nil {
				return nil, fmt.Errorf("parquet-go-fast: filter on %q: %w", strings.Join(p.path, "."), err)
			}

			cp.hi = hi
		}

		out = append(out, cp)
	}

	return out, nil
}

// keepRowGroup reports whether rg might contain a matching row, using column
// statistics (min/max, null count) and — for equality — the bloom filter.
// Returns true (keep) whenever it can't prove the group can't match.
func (cp compiledPred) keepRowGroup(rg parquet.RowGroup) bool {
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

func (cp compiledPred) bloomKeep(chunk parquet.ColumnChunk) bool {
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

// pageCanMatch reports whether page p of the predicate's column might contain a
// matching value, using the page index's per-page min/max and null flag.
func (cp compiledPred) pageCanMatch(ci parquet.ColumnIndex, p int) bool {
	if ci.NullPage(p) {
		return false // all-null page can't satisfy a value predicate
	}

	minV := ci.MinValue(p)
	maxV := ci.MaxValue(p)

	switch cp.op {
	case opEq:
		return cp.typ.Compare(cp.lo, minV) >= 0 && cp.typ.Compare(cp.lo, maxV) <= 0
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

// pageRanges returns the group-local row ranges [start,end) of pages in the
// predicate's column that might match, using the column's page index. The second
// result is false when no page index is available (so no page-level narrowing is
// possible and the caller should not narrow on this predicate).
func (cp compiledPred) pageRanges(rg parquet.RowGroup, groupRows int64) ([][2]int64, bool) {
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

// candidateRanges returns the group-local row ranges that could contain a row
// matching ALL predicates, intersecting each predicate's surviving page ranges.
// Predicates whose column has no page index contribute no narrowing.
func candidateRanges(rg parquet.RowGroup, preds []compiledPred) [][2]int64 {
	groupRows := rg.NumRows()
	cur := [][2]int64{{0, groupRows}}

	for i := range preds {
		pr, ok := preds[i].pageRanges(rg, groupRows)
		if !ok {
			continue
		}

		cur = intersectRanges(cur, pr)
		if len(cur) == 0 {
			return cur
		}
	}

	return cur
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

// matchRow reports whether a single (non-null) row value satisfies the predicate.
func (cp compiledPred) matchRow(v parquet.Value) bool {
	if v.IsNull() {
		return false
	}

	switch cp.op {
	case opEq:
		return cp.typ.Compare(v, cp.lo) == 0
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
