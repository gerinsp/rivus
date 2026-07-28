package core

import (
	"testing"
	"time"
)

func TestMySQLServerTimeFromUnixMicrosPreservesInstantAcrossTimezones(t *testing.T) {
	jakarta, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}
	want := time.Date(2026, time.July, 28, 11, 37, 9, 123456000, jakarta)

	got := mysqlServerTimeFromUnixMicros(want.UnixMicro())
	if !got.Equal(want) {
		t.Fatalf("server time = %s, want same instant as %s", got, want)
	}
	if got.UnixMicro() != want.UnixMicro() {
		t.Fatalf("server unix micros = %d, want %d", got.UnixMicro(), want.UnixMicro())
	}
}
