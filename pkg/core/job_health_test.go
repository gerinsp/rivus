package core

import (
	"strings"
	"testing"
	"time"
)

func TestDeriveJobHealthMarksPurgedCheckpointCritical(t *testing.T) {
	health := deriveJobHealth(JobStatusRunning, &JobProgress{CDCBinlogStatus: "purged"}, time.Now(), time.Now())
	if health.Status != JobHealthCritical {
		t.Fatalf("health = %+v, want CRITICAL", health)
	}
}

func TestDeriveJobHealthMarksBackpressureDegraded(t *testing.T) {
	health := deriveJobHealth(JobStatusRunning, &JobProgress{Summary: "Waiting for sink flush"}, time.Now(), time.Now())
	if health.Status != JobHealthDegraded {
		t.Fatalf("health = %+v, want DEGRADED", health)
	}
}

func TestDeriveJobHealthDoesNotMarkNormalCheckpointWaitDegraded(t *testing.T) {
	health := deriveJobHealth(JobStatusRunning, &JobProgress{
		Phase:             "streaming",
		Summary:           "CDC streaming",
		CheckpointPending: true,
	}, time.Now(), time.Now())
	if health.Status != JobHealthHealthy {
		t.Fatalf("health = %+v, want HEALTHY", health)
	}
}

func TestDeriveJobHealthMarksOldActiveStateStale(t *testing.T) {
	now := time.Now()
	health := deriveJobHealth(JobStatusRunning, &JobProgress{Phase: "streaming"}, now.Add(-3*time.Minute), now)
	if health.Status != JobHealthStale || !strings.Contains(health.Detail, "3m") {
		t.Fatalf("health = %+v, want STALE with age", health)
	}
}

func TestDeriveJobHealthDoesNotLabelStoppedJobStale(t *testing.T) {
	health := deriveJobHealth(JobStatusStopped, nil, time.Now().Add(-time.Hour), time.Now())
	if health.Status != "" {
		t.Fatalf("health = %+v, want empty for stopped job", health)
	}
}

func TestSnapshotProgressReleasesSlotOnCDCEvidence(t *testing.T) {
	progress := &JobProgress{Phase: "snapshot", CDCCheckpointFile: "mysql-bin.000183"}
	if !snapshotProgressReleasesSlot(progress) {
		t.Fatal("CDC evidence should release snapshot slot even when phase is stale")
	}
}
