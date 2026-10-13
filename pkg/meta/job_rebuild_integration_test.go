//go:build integration

package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/google/uuid"
)

func TestJobRebuildRealMySQLArchivesAtomicallyAndFencesOldSubmission(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima fixture opt-in")
	}
	for _, protocol := range []string{"native", "filtered-trino"} {
		for _, fault := range []string{"success", "PREPARED", "active lease", "stale submission", "stale state key", "another state key", "archive failure", "reset failure", "unconfigured reset", "duplicate request"} {
			t.Run(protocol+"/"+fault, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				db, err := sql.Open("mysql", fenceIntegrationDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				offsets, err := NewMySQLOffsetStore(fenceIntegrationDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer offsets.Close()
				registry, err := NewMySQLJobStore(fenceIntegrationDSN)
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
				id := "rebuild_" + strings.ReplaceAll(uuid.NewString(), "-", "")
				key, submission, op := id+"_state", uuid.NewString(), uuid.NewString()
				cfg := &config.JobConfig{ID: id, Mode: config.JobModeInitial, Source: &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"database": "app", "table": "orders"}}, Sink: &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{"snapshot_write_mode": "truncate-append"}}}
				marker, writeMode := config.IcebergRebuildFullResetKey, config.IcebergRebuildSnapshotWriteMode
				if protocol == "filtered-trino" {
					marker, writeMode = config.IcebergRebuildScopedKey, config.IcebergRebuildScopedWriteMode
					cfg.Source.Config["table_configs"] = map[string]any{"app.orders": map[string]any{"filter": "`created_at` >= '2026-09-01 00:00:00'"}}
					cfg.Sink.Config = map[string]any{"snapshot_write_mode": "auto", "snapshot_replace_delete_executor": "trino", "trino_delete": map[string]any{"uri": "http://127.0.0.1:49319", "catalog": "fixture"}, "snapshot_replace_filters": map[string]any{"app.orders": map[string]any{"column": "created_at", "op": ">=", "value": "2026-09-01 00:00:00"}}, "snapshot_truncate_exclude_tables": []string{"app.orders"}}
				}
				if fault == "unconfigured reset" {
					cfg.Sink.Config["snapshot_write_mode"] = "append"
				}
				before := PersistedJob{ID: id, SubmissionID: submission, MetaKey: key, Config: cfg, DesiredState: DesiredStateStopped, ExecutionRole: JobExecutionRoleStreaming, LastStatus: "FAILED"}
				if err := registry.SaveJob(ctx, before); err != nil {
					t.Fatal(err)
				}
				defer func() {
					db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", id)
					offsets.DeleteJobState(context.Background(), key)
					registry.DeleteJob(context.Background(), id)
					db.Exec("DELETE FROM job_recovery_archives WHERE job_id=?", id)
				}()
				initial := Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 4}
				if err := offsets.SaveOffset(ctx, key, initial); err != nil {
					t.Fatal(err)
				}
				if err := offsets.SaveSnapshotStart(ctx, key, initial); err != nil {
					t.Fatal(err)
				}
				if err := offsets.MarkSnapshotDone(ctx, key); err != nil {
					t.Fatal(err)
				}
				if err := offsets.SaveSnapshotProgress(ctx, key, "app.orders", 10, `{"id":10}`); err != nil {
					t.Fatal(err)
				}
				intentKey := key
				if fault == "another state key" {
					intentKey = id + "_old_state"
				}
				intent := IcebergCommitIntent{JobID: id, StateKey: intentKey, OperationID: op, Kind: "cdc", Operation: "append", ReplayKeys: `["legacy-replay-evidence"]`, SourceTable: "app.orders", SourceStartFile: initial.BinlogFile, SourceStartPos: 30, SourceEndFile: initial.BinlogFile, SourceEndPos: 42}
				if err := offsets.BeginIcebergCommit(ctx, intent); err != nil {
					t.Fatal(err)
				}
				if fault != "PREPARED" {
					if err := offsets.MarkIcebergCommitCommitted(ctx, intentKey, op); err != nil {
						t.Fatal(err)
					}
				}
				requestedSubmission, requestedKey := submission, key
				if fault == "active lease" {
					if _, err := db.ExecContext(ctx, "UPDATE job_registry SET lease_owner='old-owner',lease_until=UTC_TIMESTAMP(6)+INTERVAL 30 SECOND WHERE job_id=?", id); err != nil {
						t.Fatal(err)
					}
				}
				if fault == "stale submission" {
					requestedSubmission = uuid.NewString()
				}
				if fault == "stale state key" {
					requestedKey = id + "_wrong"
				}
				if fault == "archive failure" || fault == "reset failure" {
					trigger := "fail_" + id
					stmt := "CREATE TRIGGER " + trigger + " BEFORE INSERT ON job_recovery_archives FOR EACH ROW BEGIN IF NEW.job_id='" + id + "' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected archive failure'; END IF; END"
					if fault == "reset failure" {
						stmt = "CREATE TRIGGER " + trigger + " BEFORE DELETE ON job_offsets FOR EACH ROW BEGIN IF OLD.job_id='" + key + "' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected reset failure'; END IF; END"
					}
					if _, err := db.ExecContext(ctx, stmt); err != nil {
						t.Fatal(err)
					}
					defer db.Exec("DROP TRIGGER IF EXISTS " + trigger)
				}
				result, err := registry.RequestJobRebuild(ctx, offsets, id, requestedSubmission, requestedKey)
				var archiveCount int
				if e := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_recovery_archives WHERE job_id=?", id).Scan(&archiveCount); e != nil {
					t.Fatal(e)
				}
				if fault != "success" && fault != "duplicate request" {
					if err == nil || result.ArchiveID != "" || archiveCount != 0 {
						t.Fatalf("unsafe rebuild result=%+v error=%v archives=%d", result, err, archiveCount)
					}
					off, e := offsets.GetOffset(ctx, key)
					if e != nil || off == nil || off.BinlogPos != 4 {
						t.Fatalf("failed rebuild changed checkpoint: %v %v", off, e)
					}
					intents, e := offsets.ListIcebergCommits(ctx, intentKey)
					if e != nil || len(intents) != 1 {
						t.Fatal("failed rebuild lost receipt")
					}
					st, e := offsets.GetSnapshotState(ctx, key)
					if e != nil || !st.CompletionConfirmed() {
						t.Fatal("failed rebuild lost snapshot state")
					}
					progress, e := offsets.GetSnapshotProgress(ctx, key)
					if e != nil || progress == nil || progress.NextOffset != 10 {
						t.Fatal("failed rebuild lost snapshot progress")
					}
					var afterSubmission, afterStatus, afterDesired string
					if e := db.QueryRowContext(ctx, "SELECT submission_id,last_status,desired_state FROM job_registry WHERE job_id=?", id).Scan(&afterSubmission, &afterStatus, &afterDesired); e != nil {
						t.Fatal(e)
					}
					if afterSubmission != submission || afterStatus != "FAILED" || afterDesired != "STOPPED" {
						t.Fatal("failed rebuild changed registry")
					}
					return
				}
				if err != nil || result.SubmissionID == submission || archiveCount != 1 {
					t.Fatalf("rebuild=%+v error=%v archives=%d", result, err, archiveCount)
				}
				var payload string
				if err := db.QueryRowContext(ctx, "SELECT payload_json FROM job_recovery_archives WHERE archive_id=?", result.ArchiveID).Scan(&payload); err != nil {
					t.Fatal(err)
				}
				var archive struct {
					Config config.JobConfig
					States []struct {
						Key      string
						Offset   *Offset
						Snapshot *SnapshotState
						Progress *SnapshotProgress
						Intents  []IcebergCommitIntent
					}
				}
				if err := json.Unmarshal([]byte(payload), &archive); err != nil {
					t.Fatal(err)
				}
				if archive.Config.ID != id || len(archive.States) != 2 || archive.States[0].Offset.BinlogPos != 4 || !archive.States[0].Snapshot.CompletionConfirmed() || archive.States[0].Progress.NextOffset != 10 || len(archive.States[0].Intents) != 1 || archive.States[0].Intents[0].OperationID != op || archive.States[0].Intents[0].ReplayKeys != intent.ReplayKeys {
					t.Fatal("archive omitted recovery evidence")
				}
				if off, e := offsets.GetOffset(ctx, key); e != nil || off != nil {
					t.Fatal("fresh rebuild kept old checkpoint")
				}
				if st, e := offsets.GetSnapshotState(ctx, key); e != nil || st != nil {
					t.Fatal("fresh rebuild kept old snapshot state")
				}
				if progress, e := offsets.GetSnapshotProgress(ctx, key); e != nil || progress != nil {
					t.Fatal("fresh rebuild kept old snapshot progress")
				}
				if fault == "duplicate request" {
					if _, e := registry.RequestJobRebuild(ctx, offsets, id, submission, key); e == nil {
						t.Fatal("duplicate rebuild replaced queued generation")
					}
				}
				// A new process sees the queued snapshot and claims the new generation.
				claimed, e := registry.ClaimJobs(ctx, JobExecutionRoleSnapshot, "new-owner", 100, 30*time.Second)
				if e != nil {
					t.Fatal(e)
				}
				found := false
				for _, record := range claimed {
					if record.ID == id {
						found = true
						if record.SubmissionID != result.SubmissionID || record.ResumeRequested || record.LastStatus != "QUEUED" || record.Config.Metadata[marker] != record.SubmissionID {
							t.Fatal("restart did not preserve fresh snapshot generation")
						}
						if record.Config.Sink.Config["snapshot_write_mode"] != writeMode {
							t.Fatal("queued rebuild can be interpreted as an ordinary truncate snapshot by an old worker")
						}
						if protocol == "filtered-trino" {
							if rolling, ok := record.Config.Sink.Config["snapshot_rolling_enabled"].(bool); !ok || rolling {
								t.Fatal("scoped rebuild persisted rolling source checkpoints")
							}
							expected, _ := json.Marshal(cfg.Source.Config["table_configs"])
							actual, _ := json.Marshal(record.Config.Source.Config["table_configs"])
							if string(expected) != string(actual) {
								t.Fatal("rebuild changed source date filters")
							}
							if record.Config.Metadata[config.IcebergRebuildFullResetKey] != "" {
								t.Fatal("scoped rebuild permitted full-reset marker")
							}
						}
					}
				}
				if !found {
					t.Fatal("rebuild not claimable by snapshot worker")
				}
				stale, e := registry.FenceWorkerOffsets(offsets, WorkerOwnership{JobID: id, SubmissionID: submission, Owner: "old-owner", StateKey: key})
				if e != nil {
					t.Fatal(e)
				}
				if e := stale.SaveOffset(ctx, key, Offset{BinlogFile: initial.BinlogFile, BinlogPos: 99}); !errors.Is(e, ErrWorkerOwnershipLost) {
					t.Fatalf("stale checkpoint write=%v", e)
				}
				if e := stale.(IcebergCommitIntentStore).BeginIcebergCommit(ctx, intent); !errors.Is(e, ErrWorkerOwnershipLost) {
					t.Fatalf("stale intent write=%v", e)
				}
			})
		}
	}
}
