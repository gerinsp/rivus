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

func TestRecoveryTrinoRealIsolatedTargetKeepsUnknownAndHandsOffCDC(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated Trino fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	id := "isolated_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	emptyTable := id + "_empty"
	oldID := id + "_old"
	oldNS := id + "_old_target"
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
	scopedQuery(t, client, `CREATE SCHEMA fixture.`+oldNS, "")
	scopedQuery(t, client, `CREATE TABLE fixture.`+oldNS+`.orders (id BIGINT,status VARCHAR,created_at TIMESTAMP(6)) WITH (format_version=2)`, oldNS)
	scopedQuery(t, client, `INSERT INTO fixture.`+oldNS+`.orders VALUES (99,'old-preserved',TIMESTAMP '2026-08-01 00:00:00')`, oldNS)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		for _, table := range []string{"orders", "empty"} {
			client.execTrinoStatement(cleanup, `DROP TABLE IF EXISTS fixture.`+id+`.`+table, id)
		}
		client.execTrinoStatement(cleanup, `DROP SCHEMA IF EXISTS fixture.`+id, "")
		client.execTrinoStatement(cleanup, `DROP TABLE IF EXISTS fixture.`+oldNS+`.orders`, oldNS)
		client.execTrinoStatement(cleanup, `DROP SCHEMA IF EXISTS fixture.`+oldNS, "")
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

		db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", oldID)
		store.DeleteJobState(context.Background(), oldID)
		registry.DeleteJob(context.Background(), oldID)
		var key string
		if db.QueryRow("SELECT meta_key FROM job_registry WHERE job_id=?", id).Scan(&key) == nil {
			db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", key)
			store.DeleteJobState(context.Background(), key)
		}
		registry.DeleteJob(context.Background(), id)
	})
	old := meta.IcebergCommitIntent{OperationID: uuid.NewString(), JobID: oldID, StateKey: oldID, BatchID: "unknown-old", Kind: "cdc", Status: meta.IcebergCommitPrepared, SourceTable: "source_test." + id, TargetNamespace: oldNS, TargetTable: "orders"}
	if err := store.BeginIcebergCommit(ctx, old); err != nil {
		t.Fatal(err)
	}

	if err := store.SaveOffset(ctx, oldID, meta.Offset{BinlogFile: "mysql-bin.000000", BinlogPos: 4}); err != nil {
		t.Fatal(err)
	}
	rolling := true
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
	if err := registry.SaveJob(ctx, meta.PersistedJob{ID: oldID, SubmissionID: oldSID, Config: &config.JobConfig{ID: oldID}, MetaKey: oldID, DesiredState: meta.DesiredStateStopped, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "FAILED"}); err != nil {
		t.Fatal(err)
	}
	master := core.NewJobManager(connector.NewRegistry(), core.WithJobStore(registry), core.WithControlPlaneRole())
	t.Cleanup(func() { master.Shutdown(context.Background()) })
	if err := master.RestorePersistedJobs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := master.Submit(cfg); err != nil {
		t.Fatal(err)
	}
	var key string
	if err := db.QueryRowContext(ctx, "SELECT meta_key FROM job_registry WHERE job_id=?", id).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key == oldID || key == "" {
		t.Fatal("recovery checkpoint not isolated")
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
		workerCfg, err = rebuildSnapshotConfig(jctx, workerCfg)
		if err != nil {
			return nil, err
		}
		s := &Sink{jobID: jctx.JobID, stateKey: jctx.MetaKey, catalog: cat, offsetSto: jctx.MetaStore, cfg: workerCfg, retry: jctx.Retry, progress: jctx.ReportProgress, states: map[string]*tableState{}, sourceSchemas: map[string]*model.TableSchema{}}
		if len(jctx.Metadata) != 0 {
			return nil, fmt.Errorf("normal recovery unexpectedly requires protocol metadata")
		}
		s.equalityCommitter = rivusEqualityCommitter{sink: s}
		if snapshotRollingEnabled(workerCfg) {
			dir, err := prepareSnapshotSpoolDirectory(t.TempDir(), jctx.JobID, jctx.MetaKey)
			if err != nil {
				return nil, err
			}
			s.snapshotSpoolDir = dir
		}
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
	state, err := store.GetSnapshotState(ctx, key)
	if err != nil || !state.CompletionConfirmed() {
		t.Fatalf("unconfirmed snapshot handoff: %+v %v", state, err)
	}
	rows := scopedQuery(t, client, `SELECT id,status FROM fixture.`+id+`.orders ORDER BY id`, id)
	if len(rows) != 2 {
		t.Fatalf("snapshot lost/duplicated rows: %s", fmt.Sprint(rows))
	}
	for i, want := range []string{"3", "5"} {
		if string(rows[i][0]) != want {
			t.Fatalf("wrong filtered snapshot row %d", i)
		}
	}
	emptyRows := scopedQuery(t, client, `SELECT id FROM fixture.`+id+`.empty ORDER BY id`, id)
	if len(emptyRows) != 0 {
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
		off, err := store.GetOffset(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		return off != nil && binlogOffsetCovers(off.BinlogFile, off.BinlogPos, end.BinlogFile, end.BinlogPos)
	})
	rows = scopedQuery(t, client, `SELECT id FROM fixture.`+id+`.orders ORDER BY id`, id)
	if len(rows) != 3 || string(rows[2][0]) != "7" {
		t.Fatalf("CDC handoff lost/duplicated rows: %s", fmt.Sprint(rows))
	}
	if intents, err := store.ListIcebergCommits(ctx, key); err != nil || len(intents) != 0 {
		t.Fatalf("confirmed receipts not cleaned: %d %v", len(intents), err)
	}
	oldIntents, err := store.ListIcebergCommits(ctx, oldID)
	if err != nil || len(oldIntents) != 1 || oldIntents[0].OperationID != old.OperationID || oldIntents[0].Status != meta.IcebergCommitPrepared {
		t.Fatalf("old UNKNOWN evidence changed: %+v %v", oldIntents, err)
	}
	oldOffset, err := store.GetOffset(ctx, oldID)
	if err != nil || oldOffset == nil || oldOffset.BinlogFile != "mysql-bin.000000" || oldOffset.BinlogPos != 4 {
		t.Fatalf("old checkpoint changed: %+v %v", oldOffset, err)
	}
	oldRows := scopedQuery(t, client, `SELECT id,status FROM fixture.`+oldNS+`.orders`, oldNS)
	if len(oldRows) != 1 || string(oldRows[0][0]) != "99" || string(oldRows[0][1]) != `"old-preserved"` {
		t.Fatalf("old target changed: %s", fmt.Sprint(oldRows))
	}
	var archived int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_recovery_archives WHERE job_id=?", oldID).Scan(&archived); err != nil || archived != 0 {
		t.Fatalf("UNKNOWN job archived: %d %v", archived, err)
	}
	t.Log("real MySQL metadata/source + Trino/JDBC/files: separate normal job, September filter, rolling snapshot, empty source, sink-confirmed completion, CDC, and unchanged old UNKNOWN/checkpoint/target verified")
}
