//go:build integration

package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/google/uuid"
)

func TestNativeRebuildUpgradeRealPreservesEpochCursorAndReceipts(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated metadata fixture opt-in")
	}
	for _, fault := range []string{"success", "PREPARED", "active-owner", "stale-epoch", "foreign-receipt", "missing-query", "completed"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			base, err := NewMySQLOffsetStore(fenceIntegrationDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer base.Close()
			registry, err := NewMySQLJobStore(fenceIntegrationDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer registry.Close()
			if err := base.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.Init(ctx); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("mysql", fenceIntegrationDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			id, sid, op := "native_upgrade_"+strings.ReplaceAll(uuid.NewString(), "-", ""), uuid.NewString(), uuid.NewString()
			cfg := &config.JobConfig{ID: id, Mode: config.JobModeInitial, Metadata: map[string]string{config.IcebergRebuildScopedKey: sid}, Source: &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"database": "app", "table": "orders", "table_configs": map[string]any{"app.orders": map[string]any{"filter": "`created_at` >= '2026-09-01'"}}}}, Sink: &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{"snapshot_write_mode": config.IcebergRebuildScopedWriteMode, "snapshot_replace_delete_executor": "trino", "trino_delete": map[string]any{"uri": "http://127.0.0.1:49319"}, "snapshot_replace_filters": map[string]any{"app.orders": map[string]any{"column": "created_at", "op": ">=", "value": "2026-09-01"}}}}}
			if err := registry.SaveJob(ctx, PersistedJob{ID: id, SubmissionID: sid, MetaKey: id, Config: cfg, DesiredState: DesiredStateStopped, ExecutionRole: JobExecutionRoleSnapshot, LastStatus: "STOPPED"}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", id)
				base.DeleteJobState(context.Background(), id)
				registry.DeleteJob(context.Background(), id)
				db.Exec("DELETE FROM job_recovery_archives WHERE job_id=?", id)
			}()
			if err := base.SaveSnapshotStart(ctx, id, Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 4}); err != nil {
				t.Fatal(err)
			}
			if err := base.SaveSnapshotProgress(ctx, id, "app.orders", 200000, `{"id":200000}`); err != nil {
				t.Fatal(err)
			}
			intent := IcebergCommitIntent{JobID: id, StateKey: id, OperationID: op, Kind: "rebuild", Operation: "trino-merge", SourceTable: "app.orders", BatchID: "rebuild:" + sid + ":app.orders:merge:" + strings.Repeat("a", 64), TargetNamespace: "bronze", TargetTable: "orders", TargetUUID: uuid.NewString(), ReplayKeys: strings.Repeat("a", 64)}
			if fault == "foreign-receipt" {
				intent.BatchID = "rebuild:foreign:app.orders:merge"
			}
			if err := base.BeginIcebergCommit(ctx, intent); err != nil {
				t.Fatal(err)
			}
			if fault != "missing-query" {
				if err := base.RecordIcebergTrinoQuery(ctx, id, op, "fixture-query"); err != nil {
					t.Fatal(err)
				}
				intent.TrinoQueryID = "fixture-query"
			}
			intent.Status = IcebergCommitPrepared
			if fault != "PREPARED" {
				if err := base.MarkIcebergCommitCommitted(ctx, id, op); err != nil {
					t.Fatal(err)
				}
				intent.Status = IcebergCommitCommitted
			}
			requested := sid
			if fault == "stale-epoch" {
				requested = "another-epoch"
			}
			if fault == "active-owner" {
				db.Exec("UPDATE job_registry SET lease_owner='other' WHERE job_id=?", id)
			}
			if fault == "completed" {
				base.MarkSnapshotDone(ctx, id)
			}
			beforeState, _ := base.GetSnapshotState(ctx, id)
			beforeProgress, _ := base.GetSnapshotProgress(ctx, id)
			beforeIntents, _ := base.ListIcebergCommits(ctx, id)
			updated, err := registry.UpgradeJobNativeRebuild(ctx, base, id, requested, id)
			allowed := fault == "success" || fault == "PREPARED"
			if allowed && err != nil || !allowed && err == nil {
				t.Fatalf("upgrade error=%v allowed=%v", err, allowed)
			}
			if allowed {
				if updated.Sink.Config["snapshot_write_mode"] != config.IcebergRebuildScopedNativeWriteMode || updated.Metadata[config.IcebergRebuildScopedKey] != sid {
					t.Fatal("epoch/write mode wrong")
				}
				if _, err := registry.UpgradeJobNativeRebuild(ctx, base, id, sid, id); err != nil {
					t.Fatal(err)
				}
			}
			afterState, _ := base.GetSnapshotState(ctx, id)
			afterProgress, _ := base.GetSnapshotProgress(ctx, id)
			afterIntents, _ := base.ListIcebergCommits(ctx, id)
			encode := func(v any) string { b, _ := json.Marshal(v); return string(b) }
			if encode(beforeState) != encode(afterState) || encode(beforeProgress) != encode(afterProgress) || encode(beforeIntents) != encode(afterIntents) {
				t.Fatal("upgrade changed source state or receipt outcomes")
			}
			var gotSID, owner, desired, role, status string
			db.QueryRow("SELECT submission_id,COALESCE(lease_owner,''),desired_state,execution_role,last_status FROM job_registry WHERE job_id=?", id).Scan(&gotSID, &owner, &desired, &role, &status)
			if gotSID != sid || desired != "STOPPED" || role != "SNAPSHOT" || status != "STOPPED" {
				t.Fatal("upgrade changed lifecycle")
			}
			var archives int
			db.QueryRow("SELECT COUNT(*) FROM job_recovery_archives WHERE job_id=?", id).Scan(&archives)
			if allowed && archives != 1 || !allowed && archives != 0 {
				t.Fatalf("archive count=%d", archives)
			}
		})
	}
}
