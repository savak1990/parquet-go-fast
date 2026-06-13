package parquetfast

import (
	"hash/fnv"
	"reflect"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// A Plan is unique per (Go type, parquet schema, skip-column set). It is cached
// at runtime once built, so the reflection-heavy build runs once per shape
// rather than per Unmarshal call.

const planCacheCap = 1024 // ~25 KB per cache item (small)

type (
	// planKey identifies a cached plan by (Go type, parquet schema hash,
	// 100%-null column bitmap hash).
	planKey struct {
		rt       reflect.Type
		hash     uint64
		skipHash uint64
	}

	// planCache is a content-keyed, bounded, thread-safe cache of compiled plans.
	planCache struct {
		mu sync.Mutex
		m  map[planKey]*Plan
	}
)

var rawPlanCache = planCache{m: make(map[planKey]*Plan, planCacheCap)}

func (c *planCache) get(key planKey) (*Plan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.m[key]

	return p, ok
}

func (c *planCache) put(key planKey, plan *Plan) *Plan {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.m[key]; ok {
		return existing
	}

	if len(c.m) >= planCacheCap {
		for k := range c.m {
			delete(c.m, k)

			break
		}
	}

	c.m[key] = plan

	return plan
}

// skipHash returns a stable 64-bit hash of a 100%-null column bitmap. Returns 0
// when skip is empty/nil — the common case for files where every column carries
// at least one non-null value.
func skipHash(skip []bool) uint64 {
	if !anyTrue(skip) {
		return 0
	}

	h := fnv.New64a()
	buf := make([]byte, (len(skip)+7)/8)

	for i, v := range skip {
		if v {
			buf[i/8] |= 1 << (i % 8)
		}
	}

	_, _ = h.Write(buf)

	return h.Sum64()
}

// schemaHash returns a stable 64-bit hash uniquely identifying a parquet schema
// by its leaf paths and their max definition/repetition levels.
func schemaHash(schema *parquet.Schema) uint64 {
	h := fnv.New64a()

	for _, path := range schema.Columns() {
		for _, seg := range path {
			_, _ = h.Write([]byte(seg))
			_, _ = h.Write([]byte{0x00}) // segment separator
		}

		if leaf, ok := schema.Lookup(path...); ok {
			_, _ = h.Write([]byte{byte(leaf.MaxDefinitionLevel), byte(leaf.MaxRepetitionLevel)})
		}

		_, _ = h.Write([]byte{0xFF}) // path terminator
	}

	return h.Sum64()
}
