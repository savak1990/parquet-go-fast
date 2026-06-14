package parquetfast_test

import "testing"

// These tests verify the documented workarounds for the unsupported
// mixed-nesting shapes: introduce a named struct between the two repetition
// levels so the decoder has a dispatch boundary. Each pairs an unsupported shape
// with the supported rewrite and proves the rewrite round-trips.

// map[K][]V  →  map[K]struct{ inner []V }
type wrapPrimSlice struct {
	Vals []int64 `parquet:"vals"`
}

type rowMapOfPrimSlice struct {
	Name string                   `parquet:"name"`
	M    map[string]wrapPrimSlice `parquet:"m"`
}

func TestWorkaround_MapOfPrimitiveSlice(t *testing.T) {
	t.Parallel()

	rows := []rowMapOfPrimSlice{
		{Name: "a", M: map[string]wrapPrimSlice{
			"x": {Vals: []int64{1, 2, 3}},
			"y": {Vals: []int64{9}},
		}},
		{Name: "b", M: map[string]wrapPrimSlice{
			"z": {Vals: []int64{4, 5}},
		}},
	}

	roundtrip(t, rows)
}

// []map[K]V  →  []struct{ inner map[K]V }
type wrapMap struct {
	M map[string]int64 `parquet:"m"`
}

type rowSliceOfMap struct {
	Name  string    `parquet:"name"`
	Items []wrapMap `parquet:"items"`
}

func TestWorkaround_SliceOfMap(t *testing.T) {
	t.Parallel()

	rows := []rowSliceOfMap{
		{Name: "a", Items: []wrapMap{
			{M: map[string]int64{"p": 1, "q": 2}},
			{M: map[string]int64{"r": 3}},
		}},
		{Name: "b", Items: []wrapMap{
			{M: map[string]int64{"s": 4}},
		}},
	}

	roundtrip(t, rows)
}

// map[K][]Struct  →  map[K]struct{ inner []Struct }
type leafStruct struct {
	A string `parquet:"a"`
	B int64  `parquet:"b"`
}

type wrapStructSlice struct {
	Items []leafStruct `parquet:"items"`
}

type rowMapOfStructSlice struct {
	Name string                     `parquet:"name"`
	M    map[string]wrapStructSlice `parquet:"m"`
}

func TestWorkaround_MapOfStructSlice(t *testing.T) {
	t.Parallel()

	rows := []rowMapOfStructSlice{
		{Name: "a", M: map[string]wrapStructSlice{
			"x": {Items: []leafStruct{{A: "p", B: 1}, {A: "q", B: 2}}},
			"y": {Items: []leafStruct{{A: "r", B: 3}}},
		}},
		{Name: "b", M: map[string]wrapStructSlice{
			"z": {Items: []leafStruct{{A: "s", B: 4}}},
		}},
	}

	roundtrip(t, rows)
}

// map[K1]map[K2]Struct  →  map[K1]struct{ inner map[K2]Struct }
type wrapStructMap struct {
	Inner map[string]leafStruct `parquet:"inner"`
}

type rowMapOfStructMap struct {
	Name string                   `parquet:"name"`
	M    map[string]wrapStructMap `parquet:"m"`
}

func TestWorkaround_MapOfStructValuedMap(t *testing.T) {
	t.Parallel()

	rows := []rowMapOfStructMap{
		{Name: "a", M: map[string]wrapStructMap{
			"x": {Inner: map[string]leafStruct{"p": {A: "p", B: 1}, "q": {A: "q", B: 2}}},
			"y": {Inner: map[string]leafStruct{"r": {A: "r", B: 3}}},
		}},
		{Name: "b", M: map[string]wrapStructMap{
			"z": {Inner: map[string]leafStruct{"s": {A: "s", B: 4}}},
		}},
	}

	roundtrip(t, rows)
}
