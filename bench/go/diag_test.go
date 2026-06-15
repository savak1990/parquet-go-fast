package bench

import (
	"testing"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// TestFilterCountDiagnostic isolates whether a filter row-count discrepancy is a
// predicate-pushdown bug (pushdown != decode-then-filter) or a decode/semantic
// difference (pushdown == decode-then-filter, but != SQL engines).
func TestFilterCountDiagnostic(t *testing.T) {
	data := readFile(t)

	all, err := parquetfast.UnmarshalBytes[Taxi](data)
	if err != nil {
		t.Fatal(err)
	}

	// What our decoded data sees, filtered in plain Go.
	var goGt50, goGe50, goNonNullLE0 int

	for i := range all {
		if all[i].TripDistance > 50 {
			goGt50++
		}

		if all[i].TripDistance >= 50 {
			goGe50++
		}
	}

	// Our predicate pushdown for the same condition.
	pushed, err := parquetfast.UnmarshalBytes[TaxiProjection](data,
		parquetfast.Where(parquetfast.Col("trip_distance").Greater(float64(50))))
	if err != nil {
		t.Fatal(err)
	}

	_ = goNonNullLE0

	t.Logf("total rows           = %d", len(all))
	t.Logf("go-filter  > 50      = %d", goGt50)
	t.Logf("go-filter >= 50      = %d", goGe50)
	t.Logf("pushdown   > 50      = %d", len(pushed))
}
