package iceberg

import (
	"math"
	"strings"
	"testing"

	iceberglib "github.com/apache/iceberg-go"
)

func TestToInt64RejectsUnsignedOverflow(t *testing.T) {
	got, err := toInt64(uint64(math.MaxInt64))
	if err != nil {
		t.Fatalf("max signed value returned error: %v", err)
	}
	if got != math.MaxInt64 {
		t.Fatalf("value = %d, want %d", got, int64(math.MaxInt64))
	}

	_, err = toInt64(uint64(math.MaxInt64) + 1)
	if err == nil {
		t.Fatal("unsigned overflow returned nil error")
	}
	if !strings.Contains(err.Error(), "rebuild the table with decimal(20,0)") {
		t.Fatalf("overflow error = %q", err)
	}

	_, err = toInt64(float64(uint64(1) << 63))
	if err == nil {
		t.Fatal("float64 overflow returned nil error")
	}
}

func TestLiteralForUnsignedValueSupportsDecimalAndRejectsLongOverflow(t *testing.T) {
	value := uint64(math.MaxInt64) + 1
	lit, err := literalForUnsignedValue(value, iceberglib.DecimalTypeOf(20, 0))
	if err != nil {
		t.Fatalf("decimal literal: %v", err)
	}
	if got := lit.String(); got != "9223372036854775808" {
		t.Fatalf("decimal literal = %q", got)
	}

	_, err = literalForUnsignedValue(value, iceberglib.PrimitiveTypes.Int64)
	if err == nil {
		t.Fatal("long overflow returned nil error")
	}
}
