//go:build integration

package meta

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"testing"
)

func TestScopedTrinoQueryRealMySQLCASAndOwnership(t *testing.T) {
	db, base, fenced, key := realFenceFixture(t)
	ctx := context.Background()
	op := uuid.NewString()
	intent := IcebergCommitIntent{OperationID: op, JobID: key, StateKey: key, Kind: "rebuild", BatchID: "rebuild:" + fenced.ownership.SubmissionID + ":app.orders:reset", TargetUUID: "fixture-target-uuid"}
	if err := fenced.BeginIcebergCommit(ctx, intent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", key) })
	for i := 0; i < 2; i++ {
		if err := fenced.RecordIcebergTrinoQuery(ctx, key, op, "query-one"); err != nil {
			t.Fatal(err)
		}
	}
	if err := fenced.RecordIcebergTrinoQuery(ctx, key, op, "query-two"); err == nil {
		t.Fatal("query identity was overwritten")
	}
	if _, err := db.ExecContext(ctx, "UPDATE job_registry SET lease_owner='successor',lease_until=UTC_TIMESTAMP(6)+INTERVAL 30 SECOND WHERE job_id=?", key); err != nil {
		t.Fatal(err)
	}
	if err := fenced.RecordIcebergTrinoQuery(ctx, key, op, "query-one"); !errors.Is(err, ErrWorkerOwnershipLost) {
		t.Fatalf("stale worker recorded query: %v", err)
	}
	rows, err := base.ListIcebergCommits(ctx, key)
	if err != nil || len(rows) != 1 || rows[0].Status != IcebergCommitPrepared || rows[0].TrinoQueryID != "query-one" || rows[0].TargetUUID != intent.TargetUUID {
		t.Fatalf("durable identity changed: %+v %v", rows, err)
	}
	off, err := base.GetOffset(ctx, key)
	if err != nil || off == nil || off.BinlogPos != 4 {
		t.Fatalf("query journaling advanced checkpoint: %+v %v", off, err)
	}
}

func TestScopedTrinoQueryRealMySQLFailureRollsBack(t *testing.T) {
	db, base, fenced, key := realFenceFixture(t)
	ctx := context.Background()
	op := uuid.NewString()
	intent := IcebergCommitIntent{OperationID: op, JobID: key, StateKey: key, Kind: "rebuild", TargetUUID: "fixture-target-uuid"}
	if err := fenced.BeginIcebergCommit(ctx, intent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", key) })
	trigger := "fail_query_" + key
	if _, err := db.ExecContext(ctx, "CREATE TRIGGER "+trigger+" BEFORE UPDATE ON iceberg_commit_intents FOR EACH ROW BEGIN IF NEW.job_id='"+key+"' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected query persistence failure'; END IF; END"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TRIGGER IF EXISTS " + trigger) })
	if err := fenced.RecordIcebergTrinoQuery(ctx, key, op, "query-one"); err == nil {
		t.Fatal("SQL error swallowed")
	}
	rows, err := base.ListIcebergCommits(ctx, key)
	if err != nil || len(rows) != 1 || rows[0].TrinoQueryID != "" || rows[0].Status != IcebergCommitPrepared {
		t.Fatalf("failed transaction changed intent: %+v %v", rows, err)
	}
}
