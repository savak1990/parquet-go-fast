package parquetfast_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// All and Chan are streaming wrappers over Read; the correctness gate is the same
// as concurrent decode — they must yield exactly the sequential result, in order,
// regardless of worker count. Run with -race.

func TestReaderAll_MatchesSequential(t *testing.T) {
	t.Parallel()

	buf := multiRGMerchants(t, 1000, 80) // ~13 row groups

	seq, err := parquetfast.UnmarshalBytes[merchantDay](buf)
	if err != nil {
		t.Fatalf("sequential: %v", err)
	}

	for _, workers := range []int{1, 4, 0 /* GOMAXPROCS */} {
		rd, err := parquetfast.NewReader[merchantDay](bytes.NewReader(buf), int64(len(buf)),
			parquetfast.WithConcurrency(workers))
		if err != nil {
			t.Fatalf("workers=%d: new reader: %v", workers, err)
		}

		var got []merchantDay
		// odd batch size so batches straddle row-group boundaries
		for batch, err := range rd.All(73) {
			if err != nil {
				t.Fatalf("workers=%d: All: %v", workers, err)
			}

			got = append(got, batch...) // copies the structs out of the reused buffer
		}

		_ = rd.Close()

		if !reflect.DeepEqual(seq, got) {
			t.Fatalf("workers=%d: All differs from sequential (len %d vs %d)", workers, len(got), len(seq))
		}
	}
}

func TestReaderAll_EarlyBreak(t *testing.T) {
	t.Parallel()

	buf := multiRGMerchants(t, 1000, 80)

	rd, err := parquetfast.NewReader[merchantDay](bytes.NewReader(buf), int64(len(buf)),
		parquetfast.WithConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rd.Close() }()

	count := 0
	for batch, err := range rd.All(50) {
		if err != nil {
			t.Fatal(err)
		}

		count += len(batch)
		if count >= 100 {
			break // stop early: yield must return false and All must unwind cleanly
		}
	}

	if count < 100 {
		t.Fatalf("expected to reach 100 rows before break, got %d", count)
	}
	// Close on a partially-consumed concurrent pipeline must not hang (deferred).
}

func TestReaderChan_OrderUnderConcurrency(t *testing.T) {
	t.Parallel()

	buf := multiRGMerchants(t, 1000, 80)

	seq, err := parquetfast.UnmarshalBytes[merchantDay](buf)
	if err != nil {
		t.Fatalf("sequential: %v", err)
	}

	for _, workers := range []int{1, 4, 8, 0 /* GOMAXPROCS */} {
		rd, err := parquetfast.NewReader[merchantDay](bytes.NewReader(buf), int64(len(buf)),
			parquetfast.WithConcurrency(workers))
		if err != nil {
			t.Fatalf("workers=%d: new reader: %v", workers, err)
		}

		ch, wait := rd.Chan(context.Background(), 64)

		var got []merchantDay
		for batch := range ch {
			got = append(got, batch...)
		}

		if err := wait(); err != nil {
			t.Fatalf("workers=%d: wait: %v", workers, err)
		}

		_ = rd.Close()

		if !reflect.DeepEqual(seq, got) {
			t.Fatalf("workers=%d: Chan differs from sequential (len %d vs %d)", workers, len(got), len(seq))
		}
	}
}

func TestReaderChan_Cancellation(t *testing.T) {
	t.Parallel()

	buf := multiRGMerchants(t, 5000, 50) // ~100 row groups, so plenty remains when we cancel

	rd, err := parquetfast.NewReader[merchantDay](bytes.NewReader(buf), int64(len(buf)),
		parquetfast.WithConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rd.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, wait := rd.Chan(ctx, 16)

	got := 0
	for batch := range ch {
		got += len(batch)
		if got >= 100 {
			cancel() // stop consuming; the producer should observe ctx and stop

			break
		}
	}

	// Not draining: the producer blocks on a full channel, observes ctx.Done, and
	// reports the cancellation through wait.
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
