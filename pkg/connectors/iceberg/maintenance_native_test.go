package iceberg

import (
	"context"
	"crypto/sha256"
	"testing"
)

func TestCompactionReleasesSetupFilesystemContextBeforeExecution(t *testing.T) {
	setupCtx, setupCancel := context.WithCancel(context.Background())
	contextWasCancelled := false

	executeAfterMaintenanceSetup(setupCancel, func() nativeTaskOutcome {
		contextWasCancelled = setupCtx.Err() == context.Canceled
		return nativeTaskOutcome{}
	})

	if !contextWasCancelled {
		t.Fatal("compaction should release its setup context before loading a fresh table")
	}
}

func TestDeleteOnlyCompactionRequiresApplicableDeleteFiles(t *testing.T) {
	cases := []struct {
		name     string
		triggers compactionTriggers
		work     compactionWorkload
		wantSkip bool
	}{
		{
			name:     "position deletes remain globally but planner selects none",
			triggers: compactionTriggers{PositionDelete: true},
			work:     compactionWorkload{SelectedDataFiles: 1},
			wantSkip: true,
		},
		{
			name:     "equality deletes remain globally but planner selects none",
			triggers: compactionTriggers{EqualityDelete: true},
			work:     compactionWorkload{SelectedDataFiles: 1},
			wantSkip: true,
		},
		{
			name:     "applicable position delete is selected",
			triggers: compactionTriggers{PositionDelete: true},
			work:     compactionWorkload{SelectedDataFiles: 1, SelectedDeleteFiles: 1, PositionDeletes: 1},
		},
		{
			name:     "small files independently require compaction",
			triggers: compactionTriggers{PositionDelete: true, SmallFileCount: true},
			work:     compactionWorkload{SelectedDataFiles: 50},
		},
		{
			name: "no delete trigger",
			work: compactionWorkload{SelectedDataFiles: 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deleteOnlyCompactionHasNoApplicableDeletes(tc.triggers, tc.work); got != tc.wantSkip {
				t.Fatalf("skip=%v, want %v", got, tc.wantSkip)
			}
		})
	}
}

func TestCleanupReleasesSetupContextBeforeExecution(t *testing.T) {
	setupCtx, setupCancel := context.WithCancel(context.Background())
	contextWasCancelled := false

	executeAfterMaintenanceSetup(setupCancel, func() nativeTaskOutcome {
		contextWasCancelled = setupCtx.Err() == context.Canceled
		return nativeTaskOutcome{}
	})

	if !contextWasCancelled {
		t.Fatal("cleanup should not retain its completed setup context")
	}
}

func TestOrphanBucketWriterFlushesRecordsAndClosesOnce(t *testing.T) {
	tempDir := t.TempDir()
	records := []orphanDiskRecord{
		{Path: "s3://warehouse/ns/table/data/a.parquet"},
		{Path: "s3://warehouse/ns/table/data/b.parquet"},
		{Path: "s3://warehouse/ns/table/data/c.parquet"},
	}

	w := newOrphanBucketWriter(tempDir, "reference")
	for _, record := range records {
		if err := w.Append(record); err != nil {
			t.Fatalf("Append(%q) error = %v", record.Path, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := w.Append(orphanDiskRecord{Path: "s3://warehouse/ns/table/data/late.parquet"}); err == nil {
		t.Fatal("Append() after Close() succeeded")
	}

	for _, record := range records {
		bucket := int(sha256.Sum256([]byte(record.Path))[0])
		indexed, err := loadReferenceBucket(tempDir, bucket)
		if err != nil {
			t.Fatalf("loadReferenceBucket(%d) error = %v", bucket, err)
		}
		if _, ok := indexed[record.Path]; !ok {
			t.Fatalf("record %q was not flushed to bucket %d", record.Path, bucket)
		}
	}
}
