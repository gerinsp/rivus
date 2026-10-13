//go:build integration

package iceberg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	icecatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	mysqlsource "github.com/gerinsp/rivus/pkg/connectors/mysql"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

const recoveryMetaDSN = "root:recovery-test-only@tcp(127.0.0.1:49317)/recovery_meta?parseTime=true&timeout=5s"
const recoverySourceDSN = "root:recovery-test-only@tcp(127.0.0.1:49316)/source_test?parseTime=true&timeout=5s"

func killRecoveryWorker() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL); select {} }

type crashCatalog struct {
	icecatalog.Catalog
	stage string
}

func (c *crashCatalog) LoadTable(ctx context.Context, id icetable.Identifier) (*icetable.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return icetable.New(id, tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}
func (c *crashCatalog) CommitTable(ctx context.Context, id icetable.Identifier, req []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	if c.stage == "before_catalog" {
		killRecoveryWorker()
	}
	m, loc, err := c.Catalog.CommitTable(ctx, id, req, updates)
	if err == nil && c.stage == "after_catalog" {
		killRecoveryWorker()
	}
	return m, loc, err
}

type crashStore struct {
	*meta.MySQLOffsetStore
	stage string
}

func (s *crashStore) MarkIcebergCommitCommitted(ctx context.Context, key, id string) error {
	err := s.MySQLOffsetStore.MarkIcebergCommitCommitted(ctx, key, id)
	if err == nil && s.stage == "after_mark" {
		killRecoveryWorker()
	}
	return err
}
func (s *crashStore) SaveOffset(ctx context.Context, key string, off meta.Offset) error {
	err := s.MySQLOffsetStore.SaveOffset(ctx, key, off)
	if err == nil && s.stage == "after_checkpoint" {
		killRecoveryWorker()
	}
	return err
}

type recoveryWorkerInput struct {
	StateKey, Warehouse string
	Event, Checkpoint   model.Event
	ExtraEvent          *model.Event
	LaggingCheckpoint   *model.SourceOffset
}

func integrationSink(t *testing.T, cat icecatalog.Catalog, store meta.OffsetStore, input recoveryWorkerInput) *Sink {
	t.Helper()
	sink := recoverySink(t, cat, store)
	sink.stateKey = input.StateKey
	sink.jobID = input.StateKey
	sourceKey := tableKey(input.Event.Schema, input.Event.Table)
	state := sink.states["app.orders"]
	state.sourceKey = sourceKey
	state.sourceSchema.SchemaName = input.Event.Schema
	state.sourceSchema.TableName = input.Event.Table
	sink.states = map[string]*tableState{sourceKey: state}
	sink.sourceSchemas = map[string]*model.TableSchema{sourceKey: state.sourceSchema}
	sink.cfg.Overrides = map[string]config.IcebergTarget{sourceKey: {Namespace: "bronze", Table: "orders"}}
	return sink
}

// Invoked only as a child process. The parent asserts termination by SIGKILL,
// never treating an ordinary test failure as a successful crash injection.
func TestRecoveryCrashWorker(t *testing.T) {
	inputPath := os.Getenv("RIVUS_RECOVERY_CHILD_INPUT")
	if inputPath == "" {
		t.Skip("subprocess helper")
	}
	data, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	var input recoveryWorkerInput
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err = decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]interface{}{input.Event.Data, input.Event.OldData}
	if input.ExtraEvent != nil {
		rows = append(rows, input.ExtraEvent.Data, input.ExtraEvent.OldData)
	}
	for _, row := range rows {
		if n, ok := row["id"].(json.Number); ok {
			value, err := n.Int64()
			if err != nil {
				t.Fatal(err)
			}
			row["id"] = value
		}
	}

	store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cat, err := hadoop.NewCatalog("recovery", input.Warehouse, nil)
	if err != nil {
		t.Fatal(err)
	}
	stage := os.Getenv("RIVUS_RECOVERY_CRASH_STAGE")
	sink := integrationSink(t, &crashCatalog{Catalog: cat, stage: stage}, &crashStore{MySQLOffsetStore: store, stage: stage}, input)
	ctx := context.Background()
	if err := sink.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sink.handleEvent(ctx, input.Event); err != nil {
		t.Fatal(err)
	}
	if input.ExtraEvent != nil {
		if err := sink.handleEvent(ctx, *input.ExtraEvent); err != nil {
			t.Fatal(err)
		}
	}
	if input.LaggingCheckpoint != nil {
		sink.rememberOffset(input.LaggingCheckpoint)
	}
	if err := sink.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sink.handleEvent(ctx, input.Checkpoint); err != nil {
		t.Fatal(err)
	}
	t.Fatal("worker reached end without injected SIGKILL")
}

func sourceCDCEvents(t *testing.T, store meta.OffsetStore, stateKey, table string, wantID int64) ([]model.Event, model.Event) {
	t.Helper()
	source, err := mysqlsource.NewSource(stateKey, stateKey, config.MySQLConfig{Addr: "127.0.0.1:49316", User: "root", Password: "recovery-test-only", Database: "source_test", Tables: []string{"source_test." + table}}, config.RetryPolicy{MaxAttempts: 1}, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out := make(chan model.Event, 128)
	done := make(chan error, 1)
	go func() { done <- source.RunFull(ctx, out, config.JobModeResume) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("source did not stop")
		}
	}()
	var events []model.Event
	found := false
	for {
		select {
		case err := <-done:
			t.Fatalf("source stopped before checkpoint: %v", err)
		case <-ctx.Done():
			t.Fatal("timeout reading real MySQL binlog")
		case ev := <-out:
			// This CDC fixture starts from an already-confirmed empty snapshot.
			// Resume now repeats its completion handshake before binlog replay.
			if ev.Type == model.EventTypeSnapshotComplete && ev.Ack != nil {
				ev.Ack <- nil
				close(ev.Ack)
			}
			if ev.Type == model.EventTypeInsert {
				events = append(events, ev)
				if id := fmt.Sprint(ev.Data["id"]); id == fmt.Sprint(wantID) {
					found = true
				}
			}
			if found && ev.Type == model.EventTypeCheckpoint {
				return events, ev
			}
		}
	}
}

func TestRecoveryRealProcessCrash(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("set RIVUS_RECOVERY_INTEGRATION=1 with tests/recovery/compose.yml running")
	}
	for _, stage := range []string{"before_catalog", "after_catalog", "after_mark", "after_checkpoint"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			key := "recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			table := key
			sourceDB, err := sql.Open("mysql", recoverySourceDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer sourceDB.Close()
			if _, err := sourceDB.ExecContext(ctx, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, status VARCHAR(32))"); err != nil {
				t.Fatal(err)
			}
			defer sourceDB.ExecContext(ctx, "DROP TABLE "+table)
			store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.Init(ctx); err != nil {
				t.Fatal(err)
			}
			var file string
			var pos uint32
			var ignored1, ignored2, ignored3 any
			if err := sourceDB.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&file, &pos, &ignored1, &ignored2, &ignored3); err != nil {
				t.Fatal(err)
			}
			initial := meta.Offset{BinlogFile: file, BinlogPos: pos}
			if err := store.SaveSnapshotStart(ctx, key, initial); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkSnapshotDone(ctx, key); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveOffset(ctx, key, initial); err != nil {
				t.Fatal(err)
			}
			if _, err := sourceDB.ExecContext(ctx, "INSERT INTO "+table+" VALUES (1,'pending')"); err != nil {
				t.Fatal(err)
			}
			events, checkpoint := sourceCDCEvents(t, store, key, table, 1)
			if len(events) != 1 {
				t.Fatalf("events=%d want 1", len(events))
			}
			cat := recoveryCatalog(t)
			tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
			if err != nil {
				t.Fatal(err)
			}
			// Catalog root is the parent of the namespace directory.
			warehouse := filepath.Dir(filepath.Dir(tbl.Location()))
			input := recoveryWorkerInput{StateKey: key, Warehouse: warehouse, Event: events[0], Checkpoint: checkpoint}
			encoded, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(t.TempDir(), "worker-input.json")
			if err := os.WriteFile(inputPath, encoded, 0600); err != nil {
				t.Fatal(err)
			}
			workerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(workerCtx, os.Args[0], "-test.run=^TestRecoveryCrashWorker$", "-test.v")
			cmd.Env = append(os.Environ(), "RIVUS_RECOVERY_CHILD_INPUT="+inputPath, "RIVUS_RECOVERY_CRASH_STAGE="+stage)
			output, err := cmd.CombinedOutput()
			if workerCtx.Err() != nil {
				t.Fatalf("worker timed out instead of reaching injected crash: %v\n%s", workerCtx.Err(), output)
			}
			exit, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatalf("worker did not crash: %v\n%s", err, output)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("worker did not receive SIGKILL: %v\n%s", err, output)
			}
			t.Logf("worker killed with SIGKILL at %s", stage)
			intents, err := store.ListIcebergCommits(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if len(intents) != 1 {
				t.Fatalf("durable intents=%d want 1", len(intents))
			}
			expectedStatus := meta.IcebergCommitPrepared
			if stage == "after_mark" || stage == "after_checkpoint" {
				expectedStatus = meta.IcebergCommitCommitted
			}
			if intents[0].Status != expectedStatus {
				t.Fatalf("status=%s want %s", intents[0].Status, expectedStatus)
			}
			if err := store.DeleteJobState(ctx, key); err == nil {
				t.Fatal("cleanup discarded unresolved recovery state")
			}
			reopened, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			freshCatalog, err := hadoop.NewCatalog("recovery", warehouse, nil)
			if err != nil {
				t.Fatal(err)
			}
			restarted := integrationSink(t, freshCatalog, reopened, input)
			recoveryErr := restarted.checkIcebergRecovery(ctx)
			if stage == "before_catalog" {
				if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), "recovery BLOCKED") {
					t.Fatalf("missing proof permitted replay: %v", recoveryErr)
				}
				saved, err := reopened.GetOffset(ctx, key)
				if err != nil || saved == nil || saved.BinlogFile != initial.BinlogFile || saved.BinlogPos != initial.BinlogPos {
					t.Fatalf("unproven checkpoint=%v error=%v", saved, err)
				}
				visible, err := freshCatalog.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
				if err != nil {
					t.Fatal(err)
				}
				if visible.CurrentSnapshot() != nil {
					t.Fatal("before-request crash changed Iceberg table")
				}
				t.Log("unproven outcome remained blocked; checkpoint and intent retained")
				return
			}
			if recoveryErr != nil {
				t.Fatal(recoveryErr)
			}
			saved, err := reopened.GetOffset(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if stage != "after_checkpoint" && (saved.BinlogFile != initial.BinlogFile || saved.BinlogPos != initial.BinlogPos) {
				t.Fatal("reconciliation advanced checkpoint before source replay")
			}
			if _, err := sourceDB.ExecContext(ctx, "INSERT INTO "+table+" VALUES (2,'next')"); err != nil {
				t.Fatal(err)
			}
			replay, finalCheckpoint := sourceCDCEvents(t, reopened, key, table, 2)
			for _, ev := range replay {
				if err := restarted.handleEvent(ctx, ev); err != nil {
					t.Fatal(err)
				}
			}
			if err := restarted.handleEvent(ctx, finalCheckpoint); err != nil {
				t.Fatal(err)
			}
			if err := restarted.flushAll(ctx); err != nil {
				t.Fatal(err)
			}
			visible, err := freshCatalog.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
			if err != nil {
				t.Fatal(err)
			}
			_, records, err := visible.Scan(icetable.WithSelectedFields("id", "status")).ToArrowRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ids := make(map[int64]string)
			for record, err := range records {
				if err != nil {
					t.Fatal(err)
				}
				idColumn := record.Column(0).(*array.Int64)
				statusColumn := record.Column(1).(*array.String)
				for row := 0; row < int(record.NumRows()); row++ {
					id := idColumn.Value(row)
					if _, exists := ids[id]; exists {
						record.Release()
						t.Fatalf("duplicate visible id=%d", id)
					}
					ids[id] = statusColumn.Value(row)
				}
				record.Release()
			}
			if len(ids) != 2 || ids[1] != "pending" || ids[2] != "next" {
				t.Fatalf("visible rows=%v want 1:pending,2:next", ids)
			}
			if len(visible.Metadata().Snapshots()) != 2 {
				t.Fatal("crashed batch was committed twice")
			}
			intents, err = reopened.ListIcebergCommits(ctx, key)
			if err != nil || len(intents) != 0 {
				t.Fatalf("intents=%v error=%v", intents, err)
			}
			saved, err = reopened.GetOffset(ctx, key)
			if err != nil || !binlogOffsetCovers(saved.BinlogFile, saved.BinlogPos, finalCheckpoint.SourceOffset.BinlogFile, finalCheckpoint.SourceOffset.BinlogPos) {
				t.Fatalf("checkpoint=%v error=%v", saved, err)
			}
			if err := reopened.DeleteJobState(ctx, key); err != nil {
				t.Fatal(err)
			}
			t.Logf("fresh process/store/catalog recovery: rows=2 snapshots=2 final checkpoint=%s:%d", saved.BinlogFile, saved.BinlogPos)
		})
	}
}

func TestRecoveryRealSQLCleanupRollback(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("integration opt-in")
	}
	ctx := context.Background()
	key := "recovery_cleanup_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := store.SaveSnapshotStart(ctx, key, meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 4}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshotProgress(ctx, key, "source.orders", 10, "cursor"); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOffset(ctx, key, meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}); err != nil {
		t.Fatal(err)
	}
	trigger := "fail_" + key
	query := "CREATE TRIGGER " + trigger + " BEFORE DELETE ON job_offsets FOR EACH ROW BEGIN IF OLD.job_id = '" + key + "' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected offset delete failure'; END IF; END"
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, "DROP TRIGGER IF EXISTS "+trigger)
	if err := store.DeleteJobState(ctx, key); err == nil || !strings.Contains(err.Error(), "injected offset delete failure") {
		t.Fatalf("cleanup=%v", err)
	}
	for _, table := range []string{"job_snapshots", "job_snapshot_progress", "job_offsets"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE job_id=?", key).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s rows=%d error=%v", table, count, err)
		}
	}
	if _, err := db.ExecContext(ctx, "DROP TRIGGER "+trigger); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteJobState(ctx, key); err != nil {
		t.Fatal(err)
	}
	t.Log("real MySQL SIGNAL propagated; all three checkpoint rows rolled back; retry cleanup succeeded")
}
