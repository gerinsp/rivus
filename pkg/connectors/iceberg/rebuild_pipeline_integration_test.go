//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	mysqlsource "github.com/gerinsp/rivus/pkg/connectors/mysql"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

func TestJobRebuildRealPipelineResetsEmptyTargetSnapshotsAndHandsOffCDC(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	id := "rebuild_pipeline_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	emptyTable := id + "_empty"
	source, err := sql.Open("mysql", recoverySourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for _, table := range []string{id, emptyTable} {
		if _, err := source.ExecContext(ctx, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY,status VARCHAR(32))"); err != nil {
			t.Fatal(err)
		}
		defer source.Exec("DROP TABLE " + table)
	}
	if _, err := source.ExecContext(ctx, "INSERT INTO "+id+" VALUES (1,'pending')"); err != nil {
		t.Fatal(err)
	}
	store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := registry.Init(ctx); err != nil {
		t.Fatal(err)
	}
	metaDB, err := sql.Open("mysql", recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer metaDB.Close()
	defer func() {
		metaDB.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", id)
		store.DeleteJobState(context.Background(), id)
		registry.DeleteJob(context.Background(), id)
		metaDB.Exec("DELETE FROM job_recovery_archives WHERE job_id=?", id)
	}()
	cat := recoveryCatalog(t)
	orders, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	warehouse := filepath.Dir(filepath.Dir(orders.Location()))
	empty, err := cat.CreateTable(ctx, icetable.Identifier{"bronze", "empty"}, orders.Schema(), icecatalog.WithProperties(orders.Properties()))
	if err != nil {
		t.Fatal(err)
	}
	reader, release, err := buildRecordReader(empty.Schema(), []map[string]interface{}{{"id": int64(98), "status": "stale"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = empty.Append(ctx, reader, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	old := recoverySink(t, cat, store)
	old.stateKey = id
	old.jobID = id
	oldEvent := eventAt(99, 42)
	oldEvent.SourceOffset.BinlogFile = "mysql-bin.000000"
	oldEvent.TraceID = "mysql-bin.000000:42:insert:0"
	if err := old.handleEvent(ctx, oldEvent); err != nil {
		t.Fatal(err)
	}
	if err := old.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOffset(ctx, id, meta.Offset{BinlogFile: "mysql-bin.000000", BinlogPos: 4}); err != nil {
		t.Fatal(err)
	}
	rolling := false
	icfg := config.IcebergConfig{DefaultNamespace: "bronze", SnapshotWriteMode: "truncate-append", SnapshotRollingEnabled: &rolling, SnapshotBatchSize: 10, BatchSize: 1, DeleteConcurrency: 1, CDCDeleteExecutor: cdcDeleteExecutorEquality, Overrides: map[string]config.IcebergTarget{"source_test." + id: {Namespace: "bronze", Table: "orders"}, "source_test." + emptyTable: {Namespace: "bronze", Table: "empty"}}}
	b, err := json.Marshal(icfg)
	if err != nil {
		t.Fatal(err)
	}
	sinkConfig := map[string]any{}
	if err := json.Unmarshal(b, &sinkConfig); err != nil {
		t.Fatal(err)
	}
	cfg := &config.JobConfig{ID: id, Mode: config.JobModeInitial, BufferSize: 128, Meta: config.MetaConfig{MySQLDSN: recoveryMetaDSN}, Retry: config.RetryPolicy{MaxAttempts: 1}, Source: &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"addr": "127.0.0.1:49316", "user": "root", "password": "recovery-test-only", "tables": []string{"source_test." + id, "source_test." + emptyTable}, "snapshot_batch_size": 10}}, Sink: &config.ConnectorSpec{Type: "iceberg_native", Config: sinkConfig}}
	submission := uuid.NewString()
	if err := registry.SaveJob(ctx, meta.PersistedJob{ID: id, SubmissionID: submission, Config: cfg, MetaKey: id, DesiredState: meta.DesiredStateStopped, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "FAILED"}); err != nil {
		t.Fatal(err)
	}
	master := core.NewJobManager(connector.NewRegistry(), core.WithJobStore(registry), core.WithControlPlaneRole())
	if err := master.RestorePersistedJobs(ctx); err != nil {
		t.Fatal(err)
	}
	defer master.Shutdown(context.Background())
	if _, err := master.RequestResubmit(id); err != nil {
		t.Fatal(err)
	}
	reg := connector.NewRegistry()
	mysqlsource.Register(reg)
	reg.RegisterSink("iceberg_native", func(jctx connector.JobContext, raw any) (connector.Sink, error) {
		if _, ok := jctx.MetaStore.(*meta.FencedOffsetStore); !ok {
			return nil, fmt.Errorf("worker did not install ownership fence")
		}
		workerCfg, err := decodeIcebergConfig(raw)
		if err != nil {
			return nil, err
		}
		workerCfg, err = rebuildSnapshotConfig(jctx, workerCfg)
		if err != nil {
			return nil, err
		}
		s := &Sink{jobID: jctx.JobID, stateKey: jctx.MetaKey, catalog: cat, offsetSto: jctx.MetaStore, cfg: workerCfg, retry: jctx.Retry, progress: jctx.ReportProgress, states: make(map[string]*tableState), sourceSchemas: make(map[string]*model.TableSchema)}
		s.authoritativeRebuild = jctx.SubmissionID != "" && jctx.Metadata[config.IcebergRebuildFullResetKey] == jctx.SubmissionID
		s.equalityCommitter = rivusEqualityCommitter{sink: s}
		return s, nil
	})
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	snapshot := core.NewJobManager(reg, core.WithJobStore(registry), core.WithWorkerRole(core.WorkerRoleSnapshot), core.WithWorkerID("rebuild-snapshot"), core.WithWorkerTiming(50*time.Millisecond, 5*time.Second))
	snapshotDone := make(chan error, 1)
	go func() { snapshotDone <- snapshot.RunWorker(workerCtx) }()
	defer func() {
		stopWorkers()
		snapshot.Shutdown(context.Background())
		if err := <-snapshotDone; err != nil {
			t.Error(err)
		}
	}()
	wait := func(description string, check func() bool) {
		t.Helper()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			if check() {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("timeout: %s", description)
			case <-ticker.C:
			}
		}
	}
	wait("fresh snapshot to hand off CDC", func() bool {
		records, err := registry.LoadJobs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if record.ID == id {
				if record.LastStatus == "FAILED" {
					t.Fatalf("snapshot failed: %+v", record.Errors)
				}
				return record.ExecutionRole == meta.JobExecutionRoleStreaming && record.LeaseOwner == "" && record.LastStatus == "STOPPED"
			}
		}
		return false
	})
	f := &automaticIntegrationFixture{input: automaticWorkerInput{recoveryWorkerInput: recoveryWorkerInput{Warehouse: warehouse}}}
	assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending"}, 3)
	empty, err = cat.LoadTable(ctx, icetable.Identifier{"bronze", "empty"})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := countIcebergTableRows(ctx, empty); err != nil || count != 0 {
		t.Fatalf("empty source retained old target rows=%d error=%v", count, err)
	}
	state, err := store.GetSnapshotState(ctx, id)
	if err != nil || !state.CompletionConfirmed() {
		t.Fatal("snapshot not sink-confirmed before CDC handoff")
	}
	streaming := core.NewJobManager(reg, core.WithJobStore(registry), core.WithWorkerRole(core.WorkerRoleStreaming), core.WithWorkerID("rebuild-streaming"), core.WithWorkerTiming(50*time.Millisecond, 5*time.Second))
	streamingDone := make(chan error, 1)
	go func() { streamingDone <- streaming.RunWorker(workerCtx) }()
	defer func() {
		stopWorkers()
		streaming.Shutdown(context.Background())
		if err := <-streamingDone; err != nil {
			t.Error(err)
		}
	}()
	if _, err := source.ExecContext(ctx, "INSERT INTO "+id+" VALUES (2,'next')"); err != nil {
		t.Fatal(err)
	}
	var end meta.Offset
	var a, c, d any
	if err := source.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&end.BinlogFile, &end.BinlogPos, &a, &c, &d); err != nil {
		t.Fatal(err)
	}
	wait("CDC checkpoint after rebuilt snapshot", func() bool {
		off, err := store.GetOffset(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return off != nil && binlogOffsetCovers(off.BinlogFile, off.BinlogPos, end.BinlogFile, end.BinlogPos)
	})
	assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending", 2: "next"}, 4)
	if intents, err := store.ListIcebergCommits(ctx, id); err != nil || len(intents) != 0 {
		t.Fatalf("CDC receipt cleanup=%d error=%v", len(intents), err)
	}
	t.Log("master purged resubmit archived accepted receipt; snapshot worker cleared empty/populated targets; confirmed snapshot handed off to fenced CDC worker")
}
