//go:build integration

package core

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/google/uuid"
)

func TestFreshSnapshotRealMySQLPurgedBinlogRetainsReceipts(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima recovery fixture opt-in")
	}
	const dsn = "root:recovery-test-only@tcp(127.0.0.1:49317)/recovery_meta?parseTime=true&timeout=5s"
	for _, durable := range []bool{false, true} {
		for _, status := range []string{meta.IcebergCommitPrepared, meta.IcebergCommitCommitted} {
			name := "all/" + status
			if durable {
				name = "master/" + status
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				offsets, err := meta.NewMySQLOffsetStore(dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer offsets.Close()
				registry, err := meta.NewMySQLJobStore(dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer registry.Close()
				if err := offsets.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if err := registry.Init(ctx); err != nil {
					t.Fatal(err)
				}
				cfg := newTestJobConfig("purged_receipt_" + strings.ReplaceAll(uuid.NewString(), "-", ""))
				cfg.Source = &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"addr": "127.0.0.1:49316", "user": "root", "password": "recovery-test-only", "database": "source_test"}}
				cfg.Sink.Type = "iceberg_native"
				manager := NewJobManager(nil, WithJobStore(registry))
				if durable {
					manager = NewJobManager(nil, WithJobStore(registry), WithControlPlaneRole())
				}
				job := manager.newManagedJob(cfg)
				job.metaStore = offsets
				key := job.ensureMetaKey()
				job.setStatus(JobStatusFailed)
				manager.jobs[cfg.ID] = job
				manager.executionRoles[cfg.ID] = meta.JobExecutionRoleStreaming
				before := meta.PersistedJob{ID: cfg.ID, SubmissionID: job.SubmissionID(), Config: cfg, MetaKey: key, DesiredState: meta.DesiredStateStopped, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: string(JobStatusFailed)}
				if err := registry.SaveJob(ctx, before); err != nil {
					t.Fatal(err)
				}
				operation := uuid.NewString()
				defer func() {
					offsets.DeleteIcebergCommit(context.Background(), key, operation)
					offsets.DeleteJobState(context.Background(), key)
					registry.DeleteJob(context.Background(), cfg.ID)
				}()
				// File 000000 precedes every fixture binlog. This exercises the
				// real missing-history decision without purging fixture logs.
				checkpoint := meta.Offset{BinlogFile: "mysql-bin.000000", BinlogPos: 4}
				if err := offsets.SaveOffset(ctx, key, checkpoint); err != nil {
					t.Fatal(err)
				}
				if err := offsets.BeginIcebergCommit(ctx, meta.IcebergCommitIntent{JobID: cfg.ID, StateKey: key, OperationID: operation, Kind: "cdc", Operation: "append", SourceEndFile: checkpoint.BinlogFile, SourceEndPos: 42}); err != nil {
					t.Fatal(err)
				}
				if status == meta.IcebergCommitCommitted {
					if err := offsets.MarkIcebergCommitCommitted(ctx, key, operation); err != nil {
						t.Fatal(err)
					}
				}
				if observed, err := job.durableCheckpointBinlogStatus(ctx); err != nil || observed != "purged" {
					t.Fatalf("binlog status=%s error=%v", observed, err)
				}
				_, err = manager.RequestResubmit(cfg.ID)
				if err == nil || !strings.Contains(err.Error(), operation) || !strings.Contains(err.Error(), "fresh snapshot BLOCKED") {
					t.Fatalf("resubmit returned %v", err)
				}
				if !strings.Contains(err.Error(), "saved MySQL binlog has been purged") {
					t.Fatalf("resubmit hid the missing source history: %v", err)
				}
				records, err := registry.LoadJobs(ctx)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, after := range records {
					if after.ID == cfg.ID {
						found = true
						if after.LastStatus != before.LastStatus || after.DesiredState != before.DesiredState || after.ExecutionRole != before.ExecutionRole || after.SubmissionID != before.SubmissionID || after.ResumeRequested || after.LeaseOwner != "" {
							t.Fatalf("blocked resubmit changed registry: %+v", after)
						}
					}
				}
				if !found || job.GetStatus() != JobStatusFailed {
					t.Fatal("blocked resubmit changed or lost job")
				}
				off, err := offsets.GetOffset(ctx, key)
				if err != nil || off == nil || off.BinlogFile != checkpoint.BinlogFile || off.BinlogPos != checkpoint.BinlogPos {
					t.Fatalf("checkpoint changed: %+v error=%v", off, err)
				}
				intents, err := offsets.ListIcebergCommits(ctx, key)
				if err != nil || len(intents) != 1 || intents[0].OperationID != operation || intents[0].Status != status {
					t.Fatalf("receipt changed: %+v error=%v", intents, err)
				}
			})
		}
	}
}

func TestPurgedBinlogResubmitArchivesConfirmedReceiptsForConfiguredRebuild(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const dsn = "root:recovery-test-only@tcp(127.0.0.1:49317)/recovery_meta?parseTime=true&timeout=5s"
	offsets, err := meta.NewMySQLOffsetStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer offsets.Close()
	registry, err := meta.NewMySQLJobStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := offsets.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := registry.Init(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := newTestJobConfig("purged_rebuild_" + strings.ReplaceAll(uuid.NewString(), "-", ""))
	cfg.Source = &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"addr": "127.0.0.1:49316", "user": "root", "password": "recovery-test-only", "database": "source_test", "table": "orders"}}
	cfg.Sink = &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{"snapshot_write_mode": "auto", "snapshot_truncate_tables": []string{"source_test.orders"}}}
	manager := NewJobManager(nil, WithJobStore(registry), WithControlPlaneRole())
	job := manager.newManagedJob(cfg)
	job.metaStore = offsets
	key := job.ensureMetaKey()
	job.setStatus(JobStatusFailed)
	manager.mu.Lock()
	manager.jobs[cfg.ID] = job
	manager.executionRoles[cfg.ID] = meta.JobExecutionRoleStreaming
	manager.mu.Unlock()
	before := meta.PersistedJob{ID: cfg.ID, SubmissionID: job.SubmissionID(), Config: cfg, MetaKey: key, DesiredState: meta.DesiredStateStopped, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: string(JobStatusFailed)}
	if err := registry.SaveJob(ctx, before); err != nil {
		t.Fatal(err)
	}
	operation := uuid.NewString()
	defer func() {
		offsets.DeleteIcebergCommit(context.Background(), key, operation)
		offsets.DeleteJobState(context.Background(), key)
		registry.DeleteJob(context.Background(), cfg.ID)
	}()
	checkpoint := meta.Offset{BinlogFile: "mysql-bin.000000", BinlogPos: 4}
	if err := offsets.SaveOffset(ctx, key, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := offsets.BeginIcebergCommit(ctx, meta.IcebergCommitIntent{JobID: cfg.ID, StateKey: key, OperationID: operation, Kind: "cdc", Operation: "append", SourceTable: "source_test.orders", SourceEndFile: checkpoint.BinlogFile, SourceEndPos: 42}); err != nil {
		t.Fatal(err)
	}
	if err := offsets.MarkIcebergCommitCommitted(ctx, key, operation); err != nil {
		t.Fatal(err)
	}
	if observed, err := job.durableCheckpointBinlogStatus(ctx); err != nil || observed != "purged" {
		t.Fatalf("status=%s error=%v", observed, err)
	}
	if _, err := manager.RequestResubmit(cfg.ID); err != nil {
		t.Fatalf("configured rebuild remained blocked: %v", err)
	}
	if job.GetStatus() != JobStatusQueued {
		t.Fatalf("status=%s", job.GetStatus())
	}
	if intents, err := offsets.ListIcebergCommits(ctx, key); err != nil || len(intents) != 0 {
		t.Fatalf("active intents=%v error=%v", intents, err)
	}
	if off, err := offsets.GetOffset(ctx, key); err != nil || off != nil {
		t.Fatalf("old checkpoint=%v error=%v", off, err)
	}
	records, err := registry.LoadJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.ID == cfg.ID {
			if record.SubmissionID == before.SubmissionID || record.ExecutionRole != meta.JobExecutionRoleSnapshot || record.ResumeRequested || record.DesiredState != meta.DesiredStateRunning || record.LastStatus != string(JobStatusQueued) || record.Config.Metadata[config.IcebergRebuildFullResetKey] != record.SubmissionID {
				t.Fatalf("invalid rebuild registration: %+v", record)
			}
			return
		}
	}
	t.Fatal("rebuild lost durable registry row")
}
