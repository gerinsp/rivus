//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	icecatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	mysqlsource "github.com/gerinsp/rivus/pkg/connectors/mysql"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/google/uuid"
)

// All endpoints are fixture constants. No repository environment/configuration
// or external service is used. Fault decorators compile only with integration.
type automaticWorkerInput struct {
	recoveryWorkerInput
	Config                   *config.JobConfig
	Owner, Stage, ControlDir string
}

type automaticCrashCatalog struct {
	icecatalog.Catalog
	stage    string
	mu       sync.Mutex
	accepted bool
}

func (c *automaticCrashCatalog) LoadTable(ctx context.Context, ident icetable.Identifier) (*icetable.Table, error) {
	c.mu.Lock()
	blocked := c.accepted && (c.stage == "during_recovery" || c.stage == "ownership_lost")
	c.mu.Unlock()
	if blocked {
		return nil, errors.New("injected catalog verification unavailable after accepted commit")
	}
	tbl, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return icetable.New(ident, tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *automaticCrashCatalog) CommitTable(ctx context.Context, ident icetable.Identifier, req []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	if c.stage == "unproven" {
		return nil, "", errors.New("injected ambiguous transport failure before catalog apply")
	}
	m, loc, err := c.Catalog.CommitTable(ctx, ident, req, updates)
	c.mu.Lock()
	lose := err == nil && !c.accepted && c.stage != "resume" && c.stage != "mark_failure"
	if err == nil {
		c.accepted = true
	}
	c.mu.Unlock()
	if lose {
		return nil, "", errors.New("injected lost response after durable catalog commit")
	}
	return m, loc, err
}

type automaticCrashStore struct {
	*meta.FencedOffsetStore
	onCheckpoint func(meta.Offset)
}

func (s *automaticCrashStore) SaveOffset(ctx context.Context, key string, off meta.Offset) error {
	err := s.FencedOffsetStore.SaveOffset(ctx, key, off)
	if err == nil && s.onCheckpoint != nil {
		s.onCheckpoint(off)
	}
	return err
}

func writeAutomaticMarker(t *testing.T, dir, name string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, name+".tmp")
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, name+".json")); err != nil {
		t.Fatal(err)
	}
}

func waitAutomaticRelease(ctx context.Context, dir string) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func TestAutomaticRecoveryManagerWorker(t *testing.T) {
	inputPath := os.Getenv("RIVUS_AUTOMATIC_CHILD_INPUT")
	if inputPath == "" {
		t.Skip("subprocess helper")
	}
	data, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	var input automaticWorkerInput
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	reg := connector.NewRegistry()
	mysqlsource.Register(reg)
	var readyOnce sync.Once
	reg.RegisterSink("iceberg_native", func(jctx connector.JobContext, _ any) (connector.Sink, error) {
		fenced, ok := jctx.MetaStore.(*meta.FencedOffsetStore)
		if !ok {
			return nil, errors.New("JobManager did not install real transactional ownership fence")
		}
		cat, err := hadoop.NewCatalog("automatic-recovery", input.Warehouse, nil)
		if err != nil {
			return nil, err
		}
		store := &automaticCrashStore{FencedOffsetStore: fenced}
		if input.Stage == "after_checkpoint" {
			store.onCheckpoint = func(off meta.Offset) {
				if !binlogOffsetCovers(off.BinlogFile, off.BinlogPos, input.Event.SourceOffset.BinlogFile, input.Event.SourceOffset.BinlogPos) {
					return
				}
				readyOnce.Do(func() {
					writeAutomaticMarker(t, input.ControlDir, "ready", map[string]string{"stage": "after_checkpoint"})
					waitAutomaticRelease(ctx, input.ControlDir)
				})
			}
		}
		sink := integrationSink(t, &automaticCrashCatalog{Catalog: cat, stage: input.Stage}, store, input.recoveryWorkerInput)
		sink.cfg.BatchSize = 1
		sink.retry = jctx.Retry
		sink.recoveryPolicy = automaticRecoveryPolicy(jctx.Retry)
		sink.progress = func(p connector.ProgressInfo) {
			jctx.ReportProgress(p)
			if p.RecoveryStatus == "" {
				return
			}
			writeAutomaticMarker(t, input.ControlDir, "progress", p)
			hold := (input.Stage == "after_recovered" && p.RecoveryStatus == "RECOVERED") || ((input.Stage == "during_recovery" || input.Stage == "ownership_lost") && p.RecoveryStatus == "RECOVERING" && p.RecoveryAttempts > 0 && p.RecoveryLastError != "")
			if hold {
				readyOnce.Do(func() {
					writeAutomaticMarker(t, input.ControlDir, "ready", p)
					waitAutomaticRelease(ctx, input.ControlDir)
					if input.Stage == "ownership_lost" && ctx.Err() == nil {
						// Deliberately exercise the old token in the stale, resumed
						// process as well as JobManager's cancellation path.
						writeErr := fenced.SaveOffset(context.Background(), input.StateKey, meta.Offset{BinlogFile: "stale-worker", BinlogPos: 999})
						markErr := fenced.MarkIcebergCommitCommitted(context.Background(), input.StateKey, p.RecoveryOperationID)
						if !errors.Is(writeErr, meta.ErrWorkerOwnershipLost) || !errors.Is(markErr, meta.ErrWorkerOwnershipLost) {
							t.Errorf("stale writes: offset=%v mark=%v", writeErr, markErr)
						}
						writeAutomaticMarker(t, input.ControlDir, "stale_rejected", map[string]string{"offset": fmt.Sprint(writeErr), "mark": fmt.Sprint(markErr)})
					}
				})
			}
		}
		if err := sink.reconcileAutomatically(ctx); err != nil {
			return nil, err
		}
		return sink, nil
	})
	manager := core.NewJobManager(reg, core.WithJobStore(registry), core.WithWorkerRole(core.WorkerRoleStreaming), core.WithWorkerID(input.Owner), core.WithWorkerTiming(100*time.Millisecond, 3*time.Second))
	if err := manager.RunWorker(ctx); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := manager.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}

type automaticIntegrationFixture struct {
	store            *meta.MySQLOffsetStore
	registry         *meta.MySQLJobStore
	sourceDB, metaDB *sql.DB
	input            automaticWorkerInput
	initial          meta.Offset
}

func newAutomaticIntegrationFixture(t *testing.T) *automaticIntegrationFixture {
	t.Helper()
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("requires tests/recovery/compose.yml and RIVUS_RECOVERY_INTEGRATION=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := "automatic_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	f := &automaticIntegrationFixture{}
	var err error
	f.sourceDB, err = sql.Open("mysql", recoverySourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.sourceDB.Close() })
	f.metaDB, err = sql.Open("mysql", recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.metaDB.Close() })
	f.store, err = meta.NewMySQLOffsetStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.store.Close() })
	f.registry, err = meta.NewMySQLJobStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.registry.Close() })
	if err := f.store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.registry.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.sourceDB.ExecContext(ctx, "CREATE TABLE "+key+" (id BIGINT PRIMARY KEY,status VARCHAR(32))"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.sourceDB.Exec("DROP TABLE IF EXISTS " + key) })
	var ignored1, ignored2, ignored3 any
	if err := f.sourceDB.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&f.initial.BinlogFile, &f.initial.BinlogPos, &ignored1, &ignored2, &ignored3); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SaveSnapshotStart(ctx, key, f.initial); err != nil {
		t.Fatal(err)
	}
	if err := f.store.MarkSnapshotDone(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SaveOffset(ctx, key, f.initial); err != nil {
		t.Fatal(err)
	}
	if _, err := f.sourceDB.ExecContext(ctx, "INSERT INTO "+key+" VALUES (1,'pending')"); err != nil {
		t.Fatal(err)
	}
	events, checkpoint := sourceCDCEvents(t, f.store, key, key, 1)
	if len(events) != 1 {
		t.Fatalf("initial source events=%d", len(events))
	}
	cat := recoveryCatalog(t)
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	warehouse := filepath.Dir(filepath.Dir(tbl.Location()))
	cfg := &config.JobConfig{ID: key, Name: key, Mode: config.JobModeResume,
		Source: &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"addr": "127.0.0.1:49316", "user": "root", "password": "recovery-test-only", "database": "source_test", "tables": []string{"source_test." + key}}},
		Sink:   &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{"default_namespace": "bronze", "snapshot_rolling_enabled": false}},
		Retry:  config.RetryPolicy{MaxAttempts: 5, BaseBackoff: 500 * time.Millisecond, MaxBackoff: time.Second}}
	cfg.Meta.MySQLDSN = recoveryMetaDSN
	f.input = automaticWorkerInput{recoveryWorkerInput: recoveryWorkerInput{StateKey: key, Warehouse: warehouse, Event: events[0], Checkpoint: checkpoint}, Config: cfg}
	if err := f.registry.SaveJob(ctx, meta.PersistedJob{ID: key, SubmissionID: uuid.NewString(), MetaKey: key, Config: cfg, DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "RUNNING"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Test-only teardown can remove the retained unproven fixture intent;
		// application cleanup must continue to refuse it.
		f.metaDB.Exec("DELETE FROM iceberg_commit_intents WHERE state_key=?", key)
		f.store.DeleteJobState(context.Background(), key)
		f.registry.DeleteJob(context.Background(), key)
	})
	return f
}

type automaticIntegrationProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	logPath string
	input   automaticWorkerInput
}

func startAutomaticIntegrationProcess(t *testing.T, f *automaticIntegrationFixture, stage, owner string) *automaticIntegrationProcess {
	t.Helper()
	input := f.input
	input.Stage, input.Owner, input.ControlDir = stage, owner, t.TempDir()
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(input.ControlDir, "input.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(input.ControlDir, "worker.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	p := &automaticIntegrationProcess{cmd: exec.Command(os.Args[0], "-test.run=^TestAutomaticRecoveryManagerWorker$", "-test.v"), done: make(chan struct{}), input: input, logPath: logPath}
	p.cmd.Env = append(os.Environ(), "RIVUS_AUTOMATIC_CHILD_INPUT="+path)
	p.cmd.Stdout, p.cmd.Stderr = logFile, logFile
	if err := p.cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); logFile.Close(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
			return
		default:
		}
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("subprocess failed to terminate")
		}
	})
	return p
}

func (p *automaticIntegrationProcess) logs() string {
	data, _ := os.ReadFile(p.logPath)
	return string(data)
}
func (p *automaticIntegrationProcess) wait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-p.done:
			t.Fatalf("worker exited early: %v\n%s", p.err, p.logs())
		case <-deadline.C:
			t.Fatalf("worker condition timed out\n%s", p.logs())
		case <-ticker.C:
		}
	}
}
func (p *automaticIntegrationProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("SIGKILL timeout")
	}
	exit, ok := p.err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected SIGKILL, got %v\n%s", p.err, p.logs())
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("expected SIGKILL, got %v\n%s", p.err, p.logs())
	}
}
func (p *automaticIntegrationProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		if p.err != nil {
			t.Fatalf("graceful stop=%v\n%s", p.err, p.logs())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("graceful stop timeout\n%s", p.logs())
	}
}

func assertAutomaticIntegrationRows(t *testing.T, f *automaticIntegrationFixture, want map[int64]string, snapshots int) {
	t.Helper()
	cat, err := hadoop.NewCatalog("oracle", f.input.Warehouse, nil)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.LoadTable(context.Background(), icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	_, records, err := tbl.Scan(icetable.WithSelectedFields("id", "status")).ToArrowRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]string{}
	for record, err := range records {
		if err != nil {
			t.Fatal(err)
		}
		ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
		for i := 0; i < int(record.NumRows()); i++ {
			id := ids.Value(i)
			if _, exists := got[id]; exists {
				record.Release()
				t.Fatalf("duplicate visible id=%d", id)
			}
			got[id] = values.Value(i)
		}
		record.Release()
	}
	if len(got) != len(want) || len(tbl.Metadata().Snapshots()) != snapshots {
		t.Fatalf("rows=%v snapshots=%d want rows=%v snapshots=%d", got, len(tbl.Metadata().Snapshots()), want, snapshots)
	}
	for id, value := range want {
		if got[id] != value {
			t.Fatalf("rows=%v want=%v", got, want)
		}
	}
}

func (f *automaticIntegrationFixture) record(t *testing.T) meta.PersistedJob {
	t.Helper()
	records, err := f.registry.LoadJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.ID == f.input.StateKey {
			return record
		}
	}
	t.Fatal("durable job missing")
	return meta.PersistedJob{}
}
func (f *automaticIntegrationFixture) offset(t *testing.T) meta.Offset {
	t.Helper()
	off, err := f.store.GetOffset(context.Background(), f.input.StateKey)
	if err != nil || off == nil {
		t.Fatalf("offset=%v error=%v", off, err)
	}
	return *off
}
func (f *automaticIntegrationFixture) waitCheckpoint(t *testing.T, p *automaticIntegrationProcess, checkpoint *meta.Offset) {
	t.Helper()
	p.wait(t, func() bool {
		off := f.offset(t)
		return binlogOffsetCovers(off.BinlogFile, off.BinlogPos, checkpoint.BinlogFile, checkpoint.BinlogPos)
	})
}
func (f *automaticIntegrationFixture) nextRow(t *testing.T, p *automaticIntegrationProcess) {
	t.Helper()
	if _, err := f.sourceDB.Exec("INSERT INTO " + f.input.StateKey + " VALUES (2,'next')"); err != nil {
		t.Fatal(err)
	}
	var checkpoint meta.Offset
	var ignored1, ignored2, ignored3 any
	if err := f.sourceDB.QueryRow("SHOW BINARY LOG STATUS").Scan(&checkpoint.BinlogFile, &checkpoint.BinlogPos, &ignored1, &ignored2, &ignored3); err != nil {
		t.Fatal(err)
	}
	f.waitCheckpoint(t, p, &checkpoint)
	assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending", 2: "next"}, 2)
	p.wait(t, func() bool {
		intents, err := f.store.ListIcebergCommits(context.Background(), f.input.StateKey)
		if err != nil {
			t.Fatal(err)
		}
		return len(intents) == 0
	})
}

func TestAutomaticRecoveryRealManagerCrashRestart(t *testing.T) {
	for _, stage := range []string{"during_recovery", "after_recovered", "after_checkpoint"} {
		t.Run(stage, func(t *testing.T) {
			f := newAutomaticIntegrationFixture(t)
			first := startAutomaticIntegrationProcess(t, f, stage, "crash-worker-a")
			first.wait(t, func() bool { _, err := os.Stat(filepath.Join(first.input.ControlDir, "ready.json")); return err == nil })
			intents, err := f.store.ListIcebergCommits(context.Background(), f.input.StateKey)
			wantStatus := meta.IcebergCommitCommitted
			if stage == "during_recovery" {
				wantStatus = meta.IcebergCommitPrepared
			}
			if err != nil || len(intents) != 1 || intents[0].Status != wantStatus {
				t.Fatalf("crash intents=%v error=%v", intents, err)
			}
			assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending"}, 1)
			checkpoint := f.offset(t)
			if stage != "after_checkpoint" {
				data, err := os.ReadFile(filepath.Join(first.input.ControlDir, "ready.json"))
				if err != nil {
					t.Fatal(err)
				}
				var progress connector.ProgressInfo
				if err := json.Unmarshal(data, &progress); err != nil {
					t.Fatal(err)
				}
				if progress.RecoveryCheckpoint != fmt.Sprintf("%s:%d", checkpoint.BinlogFile, checkpoint.BinlogPos) || binlogOffsetCovers(checkpoint.BinlogFile, checkpoint.BinlogPos, intents[0].SourceEndFile, intents[0].SourceEndPos) {
					t.Fatal("checkpoint changed during recovery or covered unfinished batch")
				}
			}
			if stage == "after_checkpoint" && !binlogOffsetCovers(checkpoint.BinlogFile, checkpoint.BinlogPos, f.input.Checkpoint.SourceOffset.BinlogFile, f.input.Checkpoint.SourceOffset.BinlogPos) {
				t.Fatal("checkpoint crash hook ran before durable save")
			}
			before := f.record(t)
			if before.LastStatus != "RUNNING" || before.DesiredState != meta.DesiredStateRunning || before.LeaseOwner != "crash-worker-a" {
				t.Fatalf("crash lifecycle=%#v", before)
			}
			first.kill(t)
			restarted := startAutomaticIntegrationProcess(t, f, "resume", "restart-worker-b")
			f.waitCheckpoint(t, restarted, &meta.Offset{BinlogFile: f.input.Checkpoint.SourceOffset.BinlogFile, BinlogPos: f.input.Checkpoint.SourceOffset.BinlogPos})
			restarted.wait(t, func() bool {
				record := f.record(t)
				return record.LeaseOwner == "restart-worker-b" && record.LastStatus == "RUNNING"
			})
			after := f.record(t)
			if after.SubmissionID != before.SubmissionID {
				t.Fatal("restart required a new submission")
			}
			assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending"}, 1)
			f.nextRow(t, restarted)
			restarted.stop(t)
			t.Logf("SIGKILL %s; real JobManager lease reclaim without resubmit; rows=2 snapshots=2, same submission, checkpoint covered", stage)
		})
	}
}

func TestAutomaticRecoveryRealSQLMarkFailure(t *testing.T) {
	f := newAutomaticIntegrationFixture(t)
	trigger := "mark_" + f.input.StateKey
	query := "CREATE TRIGGER " + trigger + " BEFORE UPDATE ON iceberg_commit_intents FOR EACH ROW BEGIN IF OLD.state_key='" + f.input.StateKey + "' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='integration mark temporarily unavailable'; END IF; END"
	if _, err := f.metaDB.Exec(query); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.metaDB.Exec("DROP TRIGGER IF EXISTS " + trigger) })
	p := startAutomaticIntegrationProcess(t, f, "mark_failure", "mark-worker")
	p.wait(t, func() bool {
		data, err := os.ReadFile(filepath.Join(p.input.ControlDir, "progress.json"))
		if err != nil {
			return false
		}
		var progress connector.ProgressInfo
		return json.Unmarshal(data, &progress) == nil && progress.RecoveryAttempts > 0 && strings.Contains(progress.RecoveryLastError, "integration mark temporarily unavailable")
	})
	assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending"}, 1)
	off := f.offset(t)
	if binlogOffsetCovers(off.BinlogFile, off.BinlogPos, f.input.Event.SourceOffset.BinlogFile, f.input.Event.SourceOffset.BinlogPos) {
		t.Fatal("mark failure advanced checkpoint over unfinished data")
	}
	if _, err := f.metaDB.Exec("DROP TRIGGER " + trigger); err != nil {
		t.Fatal(err)
	}
	f.waitCheckpoint(t, p, &meta.Offset{BinlogFile: f.input.Checkpoint.SourceOffset.BinlogFile, BinlogPos: f.input.Checkpoint.SourceOffset.BinlogPos})
	p.wait(t, func() bool {
		data, err := os.ReadFile(filepath.Join(p.input.ControlDir, "progress.json"))
		if err != nil {
			return false
		}
		var progress connector.ProgressInfo
		return json.Unmarshal(data, &progress) == nil && progress.RecoveryStatus == "RECOVERED" && progress.RecoveryAttempts >= 2
	})
	f.nextRow(t, p)
	p.stop(t)
	t.Log("real MySQL SIGNAL after catalog commit; automatic verification recovered after trigger removal, without resubmit/recommit")
}

func TestAutomaticRecoveryRealUnprovenOutcomeRemainsBlocked(t *testing.T) {
	f := newAutomaticIntegrationFixture(t)
	p := startAutomaticIntegrationProcess(t, f, "unproven", "blocked-worker")
	p.wait(t, func() bool { return f.record(t).LastStatus == "FAILED" })
	record := f.record(t)
	var progress core.JobProgress
	if err := json.Unmarshal(record.ProgressJSON, &progress); err != nil {
		t.Fatal(err)
	}
	if record.DesiredState != meta.DesiredStateStopped || progress.RecoveryStatus != "BLOCKED" || progress.RecoveryAttempts != 5 || progress.RecoveryOperationID == "" {
		t.Fatalf("blocked record=%#v progress=%#v", record, progress)
	}
	off := f.offset(t)
	if progress.RecoveryCheckpoint != fmt.Sprintf("%s:%d", off.BinlogFile, off.BinlogPos) || binlogOffsetCovers(off.BinlogFile, off.BinlogPos, f.input.Event.SourceOffset.BinlogFile, f.input.Event.SourceOffset.BinlogPos) {
		t.Fatal("unproven checkpoint changed during recovery or covered uncommitted data")
	}
	assertAutomaticIntegrationRows(t, f, map[int64]string{}, 0)
	intents, err := f.store.ListIcebergCommits(context.Background(), f.input.StateKey)
	if err != nil || len(intents) != 1 || intents[0].Status != meta.IcebergCommitPrepared {
		t.Fatalf("unproven intents=%v error=%v", intents, err)
	}
	if err := f.store.DeleteJobState(context.Background(), f.input.StateKey); err == nil {
		t.Fatal("unproven state cleanup was allowed")
	}
	p.stop(t)
	t.Log("missing proof exhausted 5 read-only attempts; FAILED/BLOCKED, no target write/replay/checkpoint advance")
}

func TestAutomaticRecoveryRealSuspendedWorkerLosesOwnership(t *testing.T) {
	f := newAutomaticIntegrationFixture(t)
	old := startAutomaticIntegrationProcess(t, f, "ownership_lost", "suspended-worker-a")
	old.wait(t, func() bool { _, err := os.Stat(filepath.Join(old.input.ControlDir, "ready.json")); return err == nil })
	before := f.record(t)
	if err := old.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	// SIGSTOP suspends pipeline AND lease renewal. The replacement must use
	// normal MySQL ClaimJobs after expiration, without an administrative owner edit.
	replacement := startAutomaticIntegrationProcess(t, f, "resume", "replacement-worker-b")
	replacement.wait(t, func() bool {
		record := f.record(t)
		return record.LeaseOwner == "replacement-worker-b" && record.LastStatus == "RUNNING"
	})
	f.waitCheckpoint(t, replacement, &meta.Offset{BinlogFile: f.input.Checkpoint.SourceOffset.BinlogFile, BinlogPos: f.input.Checkpoint.SourceOffset.BinlogPos})
	assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending"}, 1)
	saved := f.offset(t)
	if err := os.WriteFile(filepath.Join(old.input.ControlDir, "release"), []byte("release stale worker"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := old.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	old.wait(t, func() bool {
		_, err := os.Stat(filepath.Join(old.input.ControlDir, "stale_rejected.json"))
		return err == nil
	})
	data, err := os.ReadFile(filepath.Join(old.input.ControlDir, "stale_rejected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rejection map[string]string
	if err := json.Unmarshal(data, &rejection); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rejection["offset"], meta.ErrWorkerOwnershipLost.Error()) || !strings.Contains(rejection["mark"], meta.ErrWorkerOwnershipLost.Error()) {
		t.Fatalf("stale writes=%v", rejection)
	}
	old.wait(t, func() bool { return strings.Contains(old.logs(), "worker lease lost; stopping job=") })
	after, off := f.record(t), f.offset(t)
	if after.SubmissionID != before.SubmissionID || after.LeaseOwner != "replacement-worker-b" || after.LastStatus != "RUNNING" || off.BinlogFile != saved.BinlogFile || off.BinlogPos != saved.BinlogPos {
		t.Fatal("stale worker changed owner, lifecycle, submission, or checkpoint")
	}
	assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending"}, 1)
	f.nextRow(t, replacement)
	old.stop(t)
	replacement.stop(t)
	t.Log("SIGSTOP -> real lease expiry/claim -> SIGCONT: stale process checkpoint/mark rejected by MySQL fence; replacement continued same submission, rows=2 snapshots=2")
}

func recoveryFixtureDocker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", append([]string{"--context", "colima"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("fixture Docker %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestAutomaticRecoveryRealMetadataServerCrashRestart(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_CONTAINER_FAULTS") != "1" {
		t.Skip("requires explicit RIVUS_RECOVERY_CONTAINER_FAULTS=1; restarts only validated Colima metadata fixture")
	}
	f := newAutomaticIntegrationFixture(t)
	const container = "rivus-recovery-test-metadata-1"
	// Refuse to kill anything unless project, service and loopback binding
	// match the disposable fixture. Never infer an ambient Docker context.
	identity := recoveryFixtureDocker(t, "inspect", "--format", `{{index .Config.Labels "com.docker.compose.project"}}|{{index .Config.Labels "com.docker.compose.service"}}|{{(index (index .HostConfig.PortBindings "3306/tcp") 0).HostIp}}|{{(index (index .HostConfig.PortBindings "3306/tcp") 0).HostPort}}`, container)
	if identity != "rivus-recovery-test|metadata|127.0.0.1|49317" {
		t.Fatalf("refusing container fault: identity=%q", identity)
	}
	first := startAutomaticIntegrationProcess(t, f, "during_recovery", "db-crash-worker-a")
	first.wait(t, func() bool { _, err := os.Stat(filepath.Join(first.input.ControlDir, "ready.json")); return err == nil })
	before, saved := f.record(t), f.offset(t)
	intents, err := f.store.ListIcebergCommits(context.Background(), f.input.StateKey)
	if err != nil || len(intents) != 1 || intents[0].Status != meta.IcebergCommitPrepared {
		t.Fatalf("prepared before DB crash=%v error=%v", intents, err)
	}
	operation := intents[0].OperationID
	first.kill(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "docker", "--context", "colima", "start", container).Run()
	})
	recoveryFixtureDocker(t, "kill", "--signal", "KILL", container)
	if code := recoveryFixtureDocker(t, "inspect", "--format", "{{.State.ExitCode}}", container); code != "137" {
		t.Fatalf("metadata did not exit by SIGKILL: %s", code)
	}
	recoveryFixtureDocker(t, "start", container)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		pingCtx, stop := context.WithTimeout(ctx, time.Second)
		err := f.metaDB.PingContext(pingCtx)
		stop()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("metadata MySQL did not recover")
		case <-ticker.C:
		}
	}
	intents, err = f.store.ListIcebergCommits(ctx, f.input.StateKey)
	after, offset := f.record(t), f.offset(t)
	if err != nil || len(intents) != 1 || intents[0].OperationID != operation || intents[0].Status != meta.IcebergCommitPrepared || after.SubmissionID != before.SubmissionID || after.DesiredState != meta.DesiredStateRunning || offset.BinlogFile != saved.BinlogFile || offset.BinlogPos != saved.BinlogPos {
		t.Fatalf("metadata restart lost durable job/intent/checkpoint: intents=%v error=%v", intents, err)
	}
	assertAutomaticIntegrationRows(t, f, map[int64]string{1: "pending"}, 1)
	restarted := startAutomaticIntegrationProcess(t, f, "resume", "db-restart-worker-b")
	restarted.wait(t, func() bool {
		record := f.record(t)
		return record.LeaseOwner == "db-restart-worker-b" && record.LastStatus == "RUNNING"
	})
	f.waitCheckpoint(t, restarted, &meta.Offset{BinlogFile: f.input.Checkpoint.SourceOffset.BinlogFile, BinlogPos: f.input.Checkpoint.SourceOffset.BinlogPos})
	f.nextRow(t, restarted)
	restarted.stop(t)
	t.Log("worker SIGKILL + metadata mysqld SIGKILL (exit 137); InnoDB retained PREPARED/operation/checkpoint/submission; real JobManager restart recovered rows=2 snapshots=2")
}
