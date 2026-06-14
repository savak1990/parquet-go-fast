package parquetfast_test

import (
	"bytes"
	"io"
	"sync/atomic"
)

// instrumentedReaderAt wraps an io.ReaderAt and counts ReadAt calls (round trips)
// and bytes returned — the two costs that dominate on remote object storage. It
// serves three test needs at once: an S3/object-storage stand-in, a byte meter
// for projection/filter pushdown, and a round-trip meter for optimistic reads.
// ReadAt is safe for concurrent use, so it works under WithConcurrency.
type instrumentedReaderAt struct {
	ra    io.ReaderAt
	calls atomic.Int64
	bytes atomic.Int64
}

// newRemoteReaderAt serves data from memory like an S3 GetObject with a Range
// header — bytes.Reader.ReadAt returns io.EOF on a short tail read, matching
// what a real ranged GET reports at end-of-object.
func newRemoteReaderAt(data []byte) *instrumentedReaderAt {
	return &instrumentedReaderAt{ra: bytes.NewReader(data)}
}

func (c *instrumentedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.calls.Add(1)

	n, err := c.ra.ReadAt(p, off)
	c.bytes.Add(int64(n))

	return n, err
}
