package parquetfast

import (
	"github.com/parquet-go/parquet-go"
)

// Reading from remote storage (S3, GCS, …).
//
// parquet.OpenFile reads the footer first and then only the byte ranges of the
// pages it actually needs, via io.ReaderAt.ReadAt. So if you back the reader
// with ranged GETs, a decode fetches only the bytes it uses — and with column
// projection (decode a narrow struct) and Where(...) filtering (prune row
// groups), that's a small fraction of a large file. The library stays
// dependency-free; you supply the transport.

// ReaderAtFunc adapts a function to io.ReaderAt. It is the integration point for
// object stores: implement ReadAt as a ranged GET. For example, with the AWS SDK
// for Go v2:
//
//	ra := parquetfast.ReaderAtFunc(func(p []byte, off int64) (int, error) {
//	    out, err := s3c.GetObject(ctx, &s3.GetObjectInput{
//	        Bucket: aws.String(bucket),
//	        Key:    aws.String(key),
//	        Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)),
//	    })
//	    if err != nil { return 0, err }
//	    defer out.Body.Close()
//	    return io.ReadFull(out.Body, p)
//	})
//	rows, err := parquetfast.Unmarshal[T](ra, objectSize,
//	    parquetfast.WithOptimisticRead(), parquetfast.WithReadBufferSize(4<<20))
//
// ReadAt must be safe for concurrent use if you pass WithConcurrency(n>1)
// (S3 GetObject is); otherwise single-goroutine use is fine.
type ReaderAtFunc func(p []byte, off int64) (int, error)

// ReadAt implements io.ReaderAt.
func (f ReaderAtFunc) ReadAt(p []byte, off int64) (int, error) { return f(p, off) }

// WithFileOptions forwards parquet-go file options to OpenFile — the escape hatch
// for any read tuning, e.g. parquet.SkipBloomFilters or parquet.PrefetchBloomFilters.
func WithFileOptions(opts ...parquet.FileOption) Option {
	return func(c *config) { c.fileOptions = append(c.fileOptions, opts...) }
}

// WithReadBufferSize sets the buffer size parquet-go uses for reads. On remote
// storage a larger value (e.g. 4<<20) means fewer, larger range requests.
func WithReadBufferSize(n int) Option {
	return func(c *config) { c.fileOptions = append(c.fileOptions, parquet.ReadBufferSize(n)) }
}

// WithOptimisticRead reads the file's footer region in a single request at open,
// cutting round trips on high-latency (remote) storage.
func WithOptimisticRead() Option {
	return func(c *config) { c.fileOptions = append(c.fileOptions, parquet.OptimisticRead(true)) }
}

// WithAsyncReads prefetches pages in the background, overlapping page-fetch
// latency with decoding — useful when reading from a high-latency backend.
func WithAsyncReads() Option {
	return func(c *config) { c.fileOptions = append(c.fileOptions, parquet.FileReadMode(parquet.ReadModeAsync)) }
}
