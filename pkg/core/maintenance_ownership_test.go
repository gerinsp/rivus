package core

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
)

func TestMaintenanceOwnershipReleaseRules(t *testing.T) {
	tests := []struct {
		name    string
		mode    config.JobMode
		status  JobStatus
		deleted bool
		want    bool
	}{
		{name: "paused streaming", mode: config.JobModeLatest, status: JobStatusPaused, want: false},
		{name: "stopped streaming", mode: config.JobModeLatest, status: JobStatusStopped, want: true},
		{name: "failed streaming", mode: config.JobModeLatest, status: JobStatusFailed, want: true},
		{name: "failed snapshot", mode: config.JobModeSnapshotOnly, status: JobStatusFailed, want: false},
		{name: "stopped snapshot", mode: config.JobModeSnapshotOnly, status: JobStatusStopped, want: false},
		{name: "completed snapshot", mode: config.JobModeSnapshotOnly, status: JobStatusDone, want: true},
		{name: "deleted snapshot", mode: config.JobModeSnapshotOnly, status: JobStatusFailed, deleted: true, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldReleaseMaintenanceOwnership(tt.mode, tt.status, tt.deleted); got != tt.want {
				t.Fatalf("release = %t, want %t", got, tt.want)
			}
		})
	}
}
