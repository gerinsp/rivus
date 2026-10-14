//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

func TestScopedNativeRealSIGKILLResumesWithoutResetOrDuplicate(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated Trino fixture opt-in")
	}
	for _, stage := range []string{"after-catalog", "after-mark"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
			defer cancel()
			ns := "scope_crash_native_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			input := scopedCrashInput{Namespace: ns, JobID: ns, SubmissionID: uuid.NewString(), Owner: "native-crash", Stage: stage, Phase: "native"}
			client := &Sink{cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}}}
			scopedQuery(t, client, `CREATE SCHEMA fixture.`+ns, "")
			scopedQuery(t, client, `CREATE TABLE fixture.`+ns+`.orders (id BIGINT,status VARCHAR,created_at TIMESTAMP(6)) WITH(format_version=2)`, ns)
			scopedQuery(t, client, `INSERT INTO fixture.`+ns+`.orders VALUES (1,'outside',TIMESTAMP '2026-08-01 00:00:00'),(2,'stale',TIMESTAMP '2026-09-02 00:00:00'),(3,'moves',TIMESTAMP '2026-08-01 00:00:00'),(4,'null-date',NULL)`, ns)
			t.Cleanup(func() {
				c, stop := context.WithTimeout(context.Background(), 15*time.Second)
				defer stop()
				client.execTrinoStatement(c, `DROP TABLE IF EXISTS fixture.`+ns+`.orders`, ns)
				client.execTrinoStatement(c, `DROP SCHEMA IF EXISTS fixture.`+ns, "")
			})
			base, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer base.Close()
			registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
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
			db, err := sql.Open("mysql", recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			t.Cleanup(func() {
				db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", ns)
				base.DeleteJobState(context.Background(), ns)
				registry.DeleteJob(context.Background(), ns)
			})
			if err := registry.SaveJob(ctx, meta.PersistedJob{ID: ns, SubmissionID: input.SubmissionID, MetaKey: ns, Config: &config.JobConfig{ID: ns}, DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleSnapshot, LastStatus: "RUNNING"}); err != nil {
				t.Fatal(err)
			}
			if _, err := registry.ClaimJobs(ctx, meta.JobExecutionRoleSnapshot, input.Owner, 100, 2*time.Minute); err != nil {
				t.Fatal(err)
			}
			fenced, err := registry.FenceWorkerOffsets(base, meta.WorkerOwnership{JobID: ns, StateKey: ns, SubmissionID: input.SubmissionID, Owner: input.Owner})
			if err != nil {
				t.Fatal(err)
			}
			cat := scopedRealCatalog(t)
			s := scopedCrashSink(t, input, cat, fenced)
			if err := s.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(input)
			path := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestScopedTrinoCrashWorker$", "-test.v")
			cmd.Env = append(os.Environ(), "RIVUS_SCOPED_CRASH_INPUT="+path)
			logs, runErr := cmd.CombinedOutput()
			exit, ok := runErr.(*exec.ExitError)
			if !ok {
				t.Fatalf("expected real SIGKILL: %v %s", runErr, logs)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("unexpected exit: %v %s", runErr, logs)
			}
			intents, err := base.ListIcebergCommits(ctx, ns)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, i := range intents {
				if i.Kind == "snapshot" {
					found = true
					if !meta.IsScopedNativeSnapshotIntent(i, ns, ns, input.SubmissionID) || (i.Status == meta.IcebergCommitCommitted) != (stage == "after-mark") {
						t.Fatalf("wrong native crash receipt: %+v", i)
					}
				}
			}
			if !found {
				t.Fatal("crash lost native range receipt")
			}
			restarted := scopedCrashSink(t, input, cat, fenced)
			restarted.scopedNativeWrites = true
			if err := restarted.checkIcebergRecovery(ctx); err != nil {
				t.Fatal(err)
			}
			state := restarted.states["app.orders"]
			before := state.table.MetadataLocation()
			if err := restarted.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
				t.Fatal(err)
			}
			if before != state.table.MetadataLocation() {
				t.Fatal("restart repeated DELETE scope")
			}
			restarted.cfg.SnapshotBatchSize = 10000
			rows := []map[string]interface{}{{"id": int64(3), "status": "prefix", "created_at": time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)}, {"id": int64(5), "status": "suffix", "created_at": time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}}
			if err := restarted.handleSnapshotBatch(ctx, model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", Rows: rows}); err != nil {
				t.Fatal(err)
			}
			result := scopedQuery(t, client, `SELECT id,status FROM fixture.`+ns+`.orders ORDER BY id`, ns)
			if len(result) != 4 {
				t.Fatalf("lost/duplicated rows: %v", result)
			}
			for i, id := range []string{"1", "3", "4", "5"} {
				if string(result[i][0]) != id {
					t.Fatalf("unexpected row: %v", result)
				}
			}
			if string(result[1][1]) != `"prefix"` || string(result[3][1]) != `"suffix"` {
				t.Fatal("prefix/suffix payload changed")
			}
			var native int
			db.QueryRowContext(ctx, "SELECT COUNT(*) FROM iceberg_commit_intents WHERE job_id=? AND kind='snapshot'", ns).Scan(&native)
			if native != 2 {
				t.Fatalf("accepted prefix rewritten after crash: %d commits", native)
			}
		})
	}
}
