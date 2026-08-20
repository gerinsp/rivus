package iceberg

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestDynamicSparkResourceProfileUsesWorkloadAndConfiguredFloor(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		work       compactionWorkload
		want       string
	}{
		{name: "ordinary rewrite", configured: "small", work: compactionWorkload{SelectedBytes: 256 * 1024 * 1024, SelectedFiles: 100}, want: "small"},
		{name: "medium bytes", configured: "small", work: compactionWorkload{SelectedBytes: sparkMediumInputBytes + 1}, want: "medium"},
		{name: "medium files", configured: "small", work: compactionWorkload{SelectedFiles: sparkMediumInputFiles + 1}, want: "medium"},
		{name: "medium deletes", configured: "small", work: compactionWorkload{EqualityDeletes: sparkMediumDeleteFiles + 1}, want: "medium"},
		{name: "large bytes", configured: "small", work: compactionWorkload{SelectedBytes: sparkLargeInputBytes + 1}, want: "large"},
		{name: "large files", configured: "small", work: compactionWorkload{SelectedFiles: sparkLargeInputFiles + 1}, want: "large"},
		{name: "large deletes", configured: "small", work: compactionWorkload{PositionDeletes: sparkLargeDeleteFiles + 1}, want: "large"},
		{name: "xlarge bytes", configured: "small", work: compactionWorkload{SelectedBytes: sparkXLargeInputBytes + 1}, want: "xlarge"},
		{name: "xlarge files", configured: "small", work: compactionWorkload{SelectedFiles: sparkXLargeInputFiles + 1}, want: "xlarge"},
		{name: "xlarge deletes", configured: "small", work: compactionWorkload{PositionDeletes: sparkXLargeDeleteFiles + 1}, want: "xlarge"},
		{name: "configured floor", configured: "xlarge", work: compactionWorkload{SelectedBytes: 1}, want: "xlarge"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := dynamicSparkResourceProfile(tt.configured, tt.work, meta.IcebergMaintenanceTask{AttemptCount: 1})
			if got != tt.want {
				t.Fatalf("profile = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDynamicSparkResourceProfileEscalatesHeapRetries(t *testing.T) {
	work := compactionWorkload{SelectedBytes: 128 * 1024 * 1024}
	tests := []struct {
		attempt int
		want    string
	}{
		{attempt: 2, want: "medium"},
		{attempt: 3, want: "large"},
		{attempt: 4, want: "xlarge"},
	}
	for _, tt := range tests {
		got, reason := dynamicSparkResourceProfile("small", work, meta.IcebergMaintenanceTask{
			AttemptCount: tt.attempt,
			LastError:    "java.lang.OutOfMemoryError: Java heap space",
		})
		if got != tt.want {
			t.Fatalf("attempt %d profile = %q, want %q", tt.attempt, got, tt.want)
		}
		if reason != "previous Spark attempt exhausted executor heap" {
			t.Fatalf("attempt %d reason = %q", tt.attempt, reason)
		}
	}
}

func TestDynamicSparkResourceProfileDoesNotEscalateUnrelatedRetry(t *testing.T) {
	got, _ := dynamicSparkResourceProfile("small", compactionWorkload{}, meta.IcebergMaintenanceTask{
		AttemptCount: 4,
		LastError:    "runner API temporarily unavailable",
	})
	if got != "small" {
		t.Fatalf("profile = %q, want small", got)
	}
}
