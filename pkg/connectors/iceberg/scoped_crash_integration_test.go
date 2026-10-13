//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

type scopedCrashInput struct{ Namespace, JobID, SubmissionID, Owner, Stage, Phase, Statement string }

type scopedCrashStore struct {
	*meta.FencedOffsetStore
	stage string
}

func (s *scopedCrashStore) MarkIcebergCommitCommitted(ctx context.Context, key, id string) error {
	err := s.FencedOffsetStore.MarkIcebergCommitCommitted(ctx, key, id)
	if err == nil && s.stage == "after-mark" {
		killRecoveryWorker()
	}
	return err
}

func scopedCrashSink(t *testing.T, input scopedCrashInput, cat icecatalog.Catalog, store meta.OffsetStore) *Sink {
	t.Helper()
	tbl, err := cat.LoadTable(context.Background(), icetable.Identifier{input.Namespace, "orders"})
	if err != nil {
		t.Fatal(err)
	}
	state := &tableState{sourceKey: "app.orders", targetNamespace: input.Namespace, targetTable: "orders", table: tbl, sourceSchema: &model.TableSchema{SchemaName: "app", TableName: "orders", Columns: []model.TableColumn{{Name: "id", DataType: "bigint", IsPK: true}, {Name: "status", DataType: "varchar"}, {Name: "created_at", DataType: "datetime"}}}}
	return &Sink{jobID: input.JobID, stateKey: input.JobID, scopedRebuildToken: input.SubmissionID, catalog: cat, offsetSto: store, cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}, Overrides: map[string]config.IcebergTarget{"app.orders": {Namespace: input.Namespace, Table: "orders"}}, SnapshotReplaceFilters: map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "created_at", Op: ">=", Value: "2026-09-01 00:00:00"}}}, states: map[string]*tableState{"app.orders": state}}
}

// Only the parent invokes this helper, against hard-coded loopback fixture ports.
func TestScopedTrinoCrashWorker(t *testing.T) {
	path := os.Getenv("RIVUS_SCOPED_CRASH_INPUT")
	if path == "" {
		t.Skip("subprocess helper")
	}
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Fatal("fixture opt-in required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var input scopedCrashInput
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(input.Namespace, "scope_crash_") {
		t.Fatal("unexpected fixture namespace")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
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
	store, err := registry.FenceWorkerOffsets(base, meta.WorkerOwnership{JobID: input.JobID, StateKey: input.JobID, SubmissionID: input.SubmissionID, Owner: input.Owner})
	if err != nil {
		t.Fatal(err)
	}
	crash := &scopedCrashStore{FencedOffsetStore: store.(*meta.FencedOffsetStore), stage: input.Stage}
	s := scopedCrashSink(t, input, scopedRealCatalog(t), crash)
	if input.Stage == "after-terminal" {
		http.DefaultClient = &http.Client{Transport: &scopedResponseLoss{base: http.DefaultTransport, stage: "kill-terminal"}}
	}
	if err := s.executeRebuildStatement(ctx, s.states["app.orders"], input.Phase, input.Statement); err != nil {
		t.Fatal(err)
	}
	t.Fatal("worker did not reach its intended SIGKILL barrier")
}

func TestScopedTrinoRealSIGKILLAndRestartPreservesCommittedPrefix(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated Trino fixture opt-in")
	}
	for _, phase := range []string{"reset", "merge"} {
		for _, stage := range []string{"after-terminal", "after-mark"} {
			t.Run(phase+"/"+stage, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
				defer cancel()
				ns := "scope_crash_" + strings.ReplaceAll(uuid.NewString(), "-", "")
				input := scopedCrashInput{Namespace: ns, JobID: ns, SubmissionID: uuid.NewString(), Owner: "scoped-crash", Stage: stage, Phase: phase}
				client := &Sink{cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}}}
				scopedQuery(t, client, `CREATE SCHEMA fixture.`+ns, "")
				scopedQuery(t, client, `CREATE TABLE fixture.`+ns+`.orders (id BIGINT,status VARCHAR,created_at TIMESTAMP(6)) WITH (format_version=2)`, ns)
				scopedQuery(t, client, `INSERT INTO fixture.`+ns+`.orders VALUES (1,'outside',TIMESTAMP '2026-08-01 00:00:00'),(2,'stale',TIMESTAMP '2026-09-02 00:00:00'),(3,'moves',TIMESTAMP '2026-08-01 00:00:00'),(4,'null-date',NULL)`, ns)
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
					defer stop()
					client.execTrinoStatement(cleanup, `DROP TABLE IF EXISTS fixture.`+ns+`.orders`, ns)
					client.execTrinoStatement(cleanup, `DROP SCHEMA IF EXISTS fixture.`+ns, "")
				})
				cat := scopedRealCatalog(t)
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
				defer func() {
					db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", ns)
					base.DeleteJobState(context.Background(), ns)
					registry.DeleteJob(context.Background(), ns)
				}()
				if err := registry.SaveJob(ctx, meta.PersistedJob{ID: ns, SubmissionID: input.SubmissionID, Config: &config.JobConfig{ID: ns}, MetaKey: ns, DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "RUNNING"}); err != nil {
					t.Fatal(err)
				}
				if _, err := registry.ClaimJobs(ctx, meta.JobExecutionRoleStreaming, input.Owner, 100, 2*time.Minute); err != nil {
					t.Fatal(err)
				}
				fenced, err := registry.FenceWorkerOffsets(base, meta.WorkerOwnership{JobID: ns, StateKey: ns, SubmissionID: input.SubmissionID, Owner: input.Owner})
				if err != nil {
					t.Fatal(err)
				}
				s := scopedCrashSink(t, input, cat, fenced)
				rows := []map[string]interface{}{{"id": int64(3), "status": "prefix", "created_at": time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)}}
				if phase == "reset" {
					input.Statement, _, err = s.snapshotReplaceFilterTrinoSQL(s.states["app.orders"])
				} else {
					if err := s.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
						t.Fatal(err)
					}
					queries, compileErr := s.snapshotMergeTrinoQueries(s.states["app.orders"], rows)
					err = compileErr
					if err == nil {
						input.Statement = queries[0]
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "input.json")
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestScopedTrinoCrashWorker$", "-test.v")
				cmd.Env = append(os.Environ(), "RIVUS_SCOPED_CRASH_INPUT="+path)
				logs, runErr := cmd.CombinedOutput()
				exit, ok := runErr.(*exec.ExitError)
				if !ok {
					t.Fatalf("expected SIGKILL: %v %s", runErr, logs)
				}
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatalf("unexpected subprocess exit: %v %s", runErr, logs)
				}
				intents, err := base.ListIcebergCommits(ctx, ns)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, intent := range intents {
					if intent.Operation == "trino-"+phase {
						found = true
						if intent.TrinoQueryID == "" {
							t.Fatal("query identity not durable before crash")
						}
						if (intent.Status == meta.IcebergCommitCommitted) != (stage == "after-mark") {
							t.Fatalf("wrong crash state: %+v", intent)
						}
					}
				}
				if !found {
					t.Fatal("crash lost durable intent")
				}
				restarted := scopedCrashSink(t, input, cat, fenced)
				if err := restarted.checkIcebergRecovery(ctx); err != nil {
					t.Fatal(err)
				}
				before, _ := cat.LoadTable(ctx, icetable.Identifier{ns, "orders"})
				if err := restarted.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
					t.Fatal(err)
				}
				after, _ := cat.LoadTable(ctx, icetable.Identifier{ns, "orders"})
				if before.MetadataLocation() != after.MetadataLocation() {
					t.Fatal("restart repeated reset and destroyed committed prefix")
				}
				rows = append(rows, map[string]interface{}{"id": int64(5), "status": "suffix", "created_at": time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)})
				if _, err := restarted.flushScopedSnapshot(ctx, restarted.states["app.orders"], rows); err != nil {
					t.Fatal(err)
				}
				result := scopedQuery(t, client, `SELECT id,status FROM fixture.`+ns+`.orders ORDER BY id`, ns)
				if len(result) != 4 {
					t.Fatalf("lost/duplicated rows after process restart: %s", fmt.Sprint(result))
				}
				for i, id := range []string{"1", "3", "4", "5"} {
					if string(result[i][0]) != id {
						t.Fatalf("wrong restart row %d", i)
					}
				}
				if off, err := base.GetOffset(ctx, ns); err != nil || off != nil {
					t.Fatalf("unsafe checkpoint after restart: %+v %v", off, err)
				}
				t.Log("actual SIGKILL verified; fresh process retained query identity, reconciled success and continued without repeating target reset")
			})
		}
	}
}
