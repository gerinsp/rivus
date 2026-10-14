//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	mysqlsource "github.com/gerinsp/rivus/pkg/connectors/mysql"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

func TestScopedTrinoRealPurgedResubmitSnapshotsAndHandsOffCDC(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated Trino fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	id := "pipeline_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	emptyTable := id + "_empty"
	source, err := sql.Open("mysql", recoverySourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for _, name := range []string{id, emptyTable} {
		if _, err := source.ExecContext(ctx, "CREATE TABLE "+name+" (id BIGINT PRIMARY KEY,status VARCHAR(32),created_at DATETIME(6))"); err != nil {
			t.Fatal(err)
		}
		defer source.Exec("DROP TABLE " + name)
	}
	if _, err := source.ExecContext(ctx, "INSERT INTO "+id+" VALUES (3,'moved','2026-09-02 00:00:00.123456'),(5,'fresh','2026-09-03 00:00:00'),(6,'source-outside','2026-08-01 00:00:00')"); err != nil {
		t.Fatal(err)
	}
	client := &Sink{cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}}}
	scopedQuery(t, client, `CREATE SCHEMA fixture.`+id, "")
	for _, table := range []string{"orders", "empty"} {
		scopedQuery(t, client, `CREATE TABLE fixture.`+id+`.`+table+` (id BIGINT,status VARCHAR,created_at TIMESTAMP(6)) WITH (format_version=2)`, id)
		scopedQuery(t, client, `INSERT INTO fixture.`+id+`.`+table+` VALUES (1,'outside',TIMESTAMP '2026-08-01 00:00:00'),(2,'stale',TIMESTAMP '2026-09-02 00:00:00'),(3,'old-moved',TIMESTAMP '2026-08-01 00:00:00'),(4,'null-date',NULL)`, id)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		for _, table := range []string{"orders", "empty"} {
			client.execTrinoStatement(cleanup, `DROP TABLE IF EXISTS fixture.`+id+`.`+table, id)
		}
		client.execTrinoStatement(cleanup, `DROP SCHEMA IF EXISTS fixture.`+id, "")
	})
	cat := scopedRealCatalog(t)
	store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { registry.Close() })
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := registry.Init(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	t.Cleanup(func() {
		db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", id)
		store.DeleteJobState(context.Background(), id)
		registry.DeleteJob(context.Background(), id)
		db.Exec("DELETE FROM job_recovery_archives WHERE job_id=?", id)
	})
	old := meta.IcebergCommitIntent{OperationID: uuid.NewString(), JobID: id, StateKey: id, BatchID: "accepted-old", Kind: "cdc", Status: meta.IcebergCommitPrepared, SourceTable: "source_test." + id, TargetNamespace: id, TargetTable: "orders"}
	if err := store.BeginIcebergCommit(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkIcebergCommitCommitted(ctx, id, old.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOffset(ctx, id, meta.Offset{BinlogFile: "mysql-bin.000000", BinlogPos: 4}); err != nil {
		t.Fatal(err)
	}
	rolling := false
	sinkCfg := config.IcebergConfig{Warehouse: "fixture", DefaultNamespace: id, SnapshotWriteMode: "auto", SnapshotRollingEnabled: &rolling, SnapshotBatchSize: 2, BatchSize: 1, CDCDeleteExecutor: cdcDeleteExecutorEquality, SnapshotReplaceDeleteExecutor: "trino", TrinoDelete: client.cfg.TrinoDelete,
		Overrides:                     map[string]config.IcebergTarget{"source_test." + id: {Namespace: id, Table: "orders"}, "source_test." + emptyTable: {Namespace: id, Table: "empty"}},
		SnapshotReplaceFilters:        map[string]config.IcebergSnapshotReplaceFilterConfig{"source_test." + id: {Column: "created_at", Op: ">=", Value: "2026-09-01 00:00:00"}, "source_test." + emptyTable: {Column: "created_at", Op: ">=", Value: "2026-09-01 00:00:00"}},
		SnapshotTruncateExcludeTables: []string{"source_test." + id, "source_test." + emptyTable}}
	b, err := json.Marshal(sinkCfg)
	if err != nil {
		t.Fatal(err)
	}
	raw := map[string]any{}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	cfg := &config.JobConfig{ID: id, Mode: config.JobModeInitial, BufferSize: 128, Retry: config.RetryPolicy{MaxAttempts: 1}, Meta: config.MetaConfig{MySQLDSN: recoveryMetaDSN}, Source: &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"addr": "127.0.0.1:49316", "user": "root", "password": "recovery-test-only", "tables": []string{"source_test." + id, "source_test." + emptyTable}, "snapshot_batch_size": 2, "table_configs": map[string]any{id: map[string]any{"filter": "`created_at` >= '2026-09-01 00:00:00'"}, emptyTable: map[string]any{"filter": "`created_at` >= '2026-09-01 00:00:00'"}}}}, Sink: &config.ConnectorSpec{Type: "iceberg_native", Config: raw}}
	oldSID := uuid.NewString()
	if err := registry.SaveJob(ctx, meta.PersistedJob{ID: id, SubmissionID: oldSID, Config: cfg, MetaKey: id, DesiredState: meta.DesiredStateStopped, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "FAILED"}); err != nil {
		t.Fatal(err)
	}
	master := core.NewJobManager(connector.NewRegistry(), core.WithJobStore(registry), core.WithControlPlaneRole())
	t.Cleanup(func() { master.Shutdown(context.Background()) })
	if err := master.RestorePersistedJobs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := master.RequestResubmit(id); err != nil {
		t.Fatal(err)
	}
	var archived int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_recovery_archives WHERE job_id=?", id).Scan(&archived); err != nil || archived != 1 {
		t.Fatalf("archive missing count=%d err=%v", archived, err)
	}
	reg := connector.NewRegistry()
	mysqlsource.Register(reg)
	reg.RegisterSink("iceberg_native", func(jctx connector.JobContext, raw any) (connector.Sink, error) {
		if _, ok := jctx.MetaStore.(*meta.FencedOffsetStore); !ok {
			return nil, fmt.Errorf("missing worker fence")
		}
		workerCfg, err := decodeIcebergConfig(raw)
		if err != nil {
			return nil, err
		}
		native := workerCfg.SnapshotWriteMode == config.IcebergRebuildScopedNativeWriteMode
		workerCfg, err = rebuildSnapshotConfig(jctx, workerCfg)
		if err != nil {
			return nil, err
		}
		s := &Sink{jobID: jctx.JobID, stateKey: jctx.MetaKey, catalog: cat, offsetSto: jctx.MetaStore, cfg: workerCfg, retry: jctx.Retry, progress: jctx.ReportProgress, states: map[string]*tableState{}, sourceSchemas: map[string]*model.TableSchema{}}
		if jctx.Metadata[config.IcebergRebuildScopedKey] == jctx.SubmissionID {
			s.scopedRebuildToken = jctx.SubmissionID
			s.scopedNativeWrites = native
		}
		s.equalityCommitter = rivusEqualityCommitter{sink: s}
		return s, nil
	})
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	startWorker := func(role core.WorkerRole, name string) {
		worker := core.NewJobManager(reg, core.WithJobStore(registry), core.WithWorkerRole(role), core.WithWorkerID(name), core.WithWorkerTiming(50*time.Millisecond, 10*time.Second))
		done := make(chan error, 1)
		go func() { done <- worker.RunWorker(workerCtx) }()
		deferCleanup := func() {
			stop()
			worker.Shutdown(context.Background())
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
		t.Cleanup(deferCleanup)
	}
	wait := func(description string, check func() bool) {
		t.Helper()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			if check() {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("timeout %s", description)
			case <-ticker.C:
			}
		}
	}
	startWorker(core.WorkerRoleSnapshot, "scoped-snapshot")
	wait("snapshot/CDC handoff", func() bool {
		records, err := registry.LoadJobs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			if r.ID == id {
				if r.LastStatus == "FAILED" {
					t.Fatalf("snapshot failed: %+v", r.Errors)
				}
				return r.ExecutionRole == meta.JobExecutionRoleStreaming && r.LeaseOwner == "" && r.LastStatus == "STOPPED"
			}
		}
		return false
	})
	state, err := store.GetSnapshotState(ctx, id)
	if err != nil || !state.CompletionConfirmed() {
		t.Fatalf("unconfirmed snapshot handoff: %+v %v", state, err)
	}
	rows := scopedQuery(t, client, `SELECT id,status FROM fixture.`+id+`.orders ORDER BY id`, id)
	if len(rows) != 4 {
		t.Fatalf("snapshot lost/duplicated rows: %s", fmt.Sprint(rows))
	}
	for i, want := range []string{"1", "3", "4", "5"} {
		if string(rows[i][0]) != want {
			t.Fatalf("wrong filtered snapshot row %d", i)
		}
	}
	emptyRows := scopedQuery(t, client, `SELECT id FROM fixture.`+id+`.empty ORDER BY id`, id)
	if len(emptyRows) != 3 || string(emptyRows[0][0]) != "1" || string(emptyRows[1][0]) != "3" || string(emptyRows[2][0]) != "4" {
		t.Fatalf("empty source did not reset only filtered scope: %s", fmt.Sprint(emptyRows))
	}
	startWorker(core.WorkerRoleStreaming, "scoped-streaming")
	if _, err := source.ExecContext(ctx, "INSERT INTO "+id+" VALUES (7,'cdc','2026-09-04 00:00:00')"); err != nil {
		t.Fatal(err)
	}
	var end meta.Offset
	var a, c, d any
	if err := source.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&end.BinlogFile, &end.BinlogPos, &a, &c, &d); err != nil {
		t.Fatal(err)
	}
	wait("CDC checkpoint after scoped snapshot", func() bool {
		off, err := store.GetOffset(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return off != nil && binlogOffsetCovers(off.BinlogFile, off.BinlogPos, end.BinlogFile, end.BinlogPos)
	})
	rows = scopedQuery(t, client, `SELECT id FROM fixture.`+id+`.orders ORDER BY id`, id)
	if len(rows) != 5 || string(rows[4][0]) != "7" {
		t.Fatalf("CDC handoff lost/duplicated rows: %s", fmt.Sprint(rows))
	}
	if intents, err := store.ListIcebergCommits(ctx, id); err != nil || len(intents) != 0 {
		t.Fatalf("confirmed receipts not cleaned: %d %v", len(intents), err)
	}
	t.Log("real MySQL purged-checkpoint admission, atomic archive, filtered Trino reset/native data, empty source, snapshot acknowledgment and native CDC handoff verified")
}
