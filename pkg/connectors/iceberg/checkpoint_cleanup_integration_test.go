//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

func TestCheckpointCleanupRealMySQLTimeoutAndRetry(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated recovery fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
	key, submission, owner := "cleanup_"+strings.ReplaceAll(uuid.NewString(), "-", ""), uuid.NewString(), "cleanup-worker"
	defer func() {
		db.Exec("DELETE FROM iceberg_commit_intents WHERE state_key=?", key)
		base.DeleteJobState(context.Background(), key)
		registry.DeleteJob(context.Background(), key)
	}()
	if err := registry.SaveJob(ctx, meta.PersistedJob{ID: key, SubmissionID: submission, MetaKey: key,
		Config: &config.JobConfig{ID: key}, DesiredState: meta.DesiredStateRunning,
		ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "RUNNING"}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ClaimJobs(ctx, meta.JobExecutionRoleStreaming, owner, 100, time.Minute); err != nil {
		t.Fatal(err)
	}
	fenced, err := registry.FenceWorkerOffsets(base, meta.WorkerOwnership{JobID: key, StateKey: key, SubmissionID: submission, Owner: owner})
	if err != nil {
		t.Fatal(err)
	}
	intents := fenced.(meta.IcebergCommitIntentStore)
	operationIDs := make(map[string]string)
	for _, receipt := range []struct {
		id        string
		end       uint32
		committed bool
	}{{"covered", 42, true}, {"prepared", 40, false}, {"ahead", 80, true}} {
		id := uuid.NewString()
		operationIDs[receipt.id] = id
		if err := intents.BeginIcebergCommit(ctx, meta.IcebergCommitIntent{OperationID: id, JobID: key, StateKey: key,
			BatchID: id, Kind: "cdc", SourceEndFile: "mysql-bin.000001", SourceEndPos: receipt.end}); err != nil {
			t.Fatal(err)
		}
		if receipt.committed {
			if err := intents.MarkIcebergCommitCommitted(ctx, key, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Hold an advisory lock from a separate connection. The real fenced DELETE
	// waits inside MySQL until its own cleanup deadline cancels the transaction.
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	gate := key + "_gate"
	var acquired int
	if err := holder.QueryRowContext(ctx, "SELECT GET_LOCK(?,0)", gate).Scan(&acquired); err != nil || acquired != 1 {
		t.Fatalf("gate=%d error=%v", acquired, err)
	}
	defer holder.ExecContext(context.Background(), "DO RELEASE_LOCK(?)", gate)
	trigger := "gate_" + key
	query := fmt.Sprintf("CREATE TRIGGER %s BEFORE DELETE ON iceberg_commit_intents FOR EACH ROW BEGIN IF OLD.operation_id='%s' THEN DO GET_LOCK('%s',10); DO RELEASE_LOCK('%s'); END IF; END", trigger, operationIDs["covered"], gate, gate)
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TRIGGER IF EXISTS " + trigger)
	s := &Sink{jobID: key, stateKey: key, offsetSto: fenced,
		pendingOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}}
	if err := s.commitPendingOffset(ctx, true); err != nil {
		t.Fatalf("metadata cleanup timeout killed the checkpointed worker: %v", err)
	}
	off, err := base.GetOffset(ctx, key)
	if err != nil || off == nil || off.BinlogPos != 42 || s.pendingOffset != nil || s.pendingCheckpointCleanup == nil {
		t.Fatalf("durable checkpoint not acknowledged or retry lost: offset=%+v error=%v", off, err)
	}
	var retained int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM iceberg_commit_intents WHERE state_key=?", key).Scan(&retained); err != nil || retained != 3 {
		t.Fatalf("cleanup timeout discarded evidence: count=%d error=%v", retained, err)
	}
	if _, err := holder.ExecContext(ctx, "DO RELEASE_LOCK(?)", gate); err != nil {
		t.Fatal(err)
	}
	// A newer checkpoint is blocked by pending events. The deferred cleanup
	// must still use only the durable position 42, never the pending 100.
	s.pendingOffset = &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 100}
	s.states = map[string]*tableState{"app.orders": {pending: []model.Event{{Type: model.EventTypeInsert}}}}
	s.lastCheckpointCleanupAt = time.Now().Add(-checkpointCleanupInterval)
	if err := s.commitPendingOffset(ctx, true); err != nil {
		t.Fatal(err)
	}
	left, err := intents.ListIcebergCommits(ctx, key)
	if err != nil || len(left) != 2 || s.pendingCheckpointCleanup != nil {
		t.Fatalf("deferred cleanup did not finish: remaining=%d error=%v", len(left), err)
	}
	for _, intent := range left {
		if intent.OperationID == operationIDs["covered"] {
			t.Fatal("covered receipt was not cleaned on retry")
		}
	}
	off, err = base.GetOffset(ctx, key)
	if err != nil || off == nil || off.BinlogPos != 42 || s.pendingOffset == nil {
		t.Fatalf("backpressure advanced the durable checkpoint: offset=%+v error=%v", off, err)
	}
	t.Log("real MySQL cleanup deadline retained all receipts; checkpoint stayed durable; retry removed only covered COMMITTED evidence while backpressured")
}
