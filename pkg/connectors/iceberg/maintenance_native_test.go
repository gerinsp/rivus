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
