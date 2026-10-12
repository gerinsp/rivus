//go:build integration

package meta

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

const fenceIntegrationDSN = "root:recovery-test-only@tcp(127.0.0.1:49317)/recovery_meta?parseTime=true&timeout=5s"

func realFenceFixture(t *testing.T) (*sql.DB, *MySQLOffsetStore, *FencedOffsetStore, string) {
	t.Helper()
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated recovery fixture opt-in")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", fenceIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := NewMySQLOffsetStore(fenceIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	registry, err := NewMySQLJobStore(fenceIntegrationDSN)
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
	key := "fence_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	submission := uuid.NewString()
	if err := registry.SaveJob(ctx, PersistedJob{ID: key, SubmissionID: submission, Config: &config.JobConfig{ID: key}, MetaKey: key, DesiredState: DesiredStateRunning, ExecutionRole: JobExecutionRoleStreaming, LastStatus: "RUNNING"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.DeleteJobState(context.Background(), key); registry.DeleteJob(context.Background(), key) })
	records, err := registry.ClaimJobs(ctx, JobExecutionRoleStreaming, "fence-owner", 100, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range records {
		if record.ID == key && record.LeaseOwner == "fence-owner" {
			found = true
		}
	}
	if !found {
		t.Fatal("real registry did not claim fixture")
	}
	if err := store.SaveOffset(ctx, key, Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 4}); err != nil {
		t.Fatal(err)
	}
	fenced, err := registry.FenceWorkerOffsets(store, WorkerOwnership{JobID: key, SubmissionID: submission, Owner: "fence-owner", StateKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return db, store, fenced.(*FencedOffsetStore), key
}

func TestWorkerFenceRealMySQLRowLockAndTakeover(t *testing.T) {
	db, store, fenced, key := realFenceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	gate := key + "_gate"
	owner, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	var acquired int
	if err := owner.QueryRowContext(ctx, "SELECT GET_LOCK(?,0)", gate).Scan(&acquired); err != nil || acquired != 1 {
		t.Fatalf("gate=%d error=%v", acquired, err)
	}
	defer owner.ExecContext(context.Background(), "DO RELEASE_LOCK(?)", gate)
	trigger := "gate_" + key
	query := "CREATE TRIGGER " + trigger + " BEFORE UPDATE ON job_offsets FOR EACH ROW BEGIN IF NEW.job_id='" + key + "' THEN DO GET_LOCK('" + gate + "',10); DO RELEASE_LOCK('" + gate + "'); END IF; END"
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TRIGGER IF EXISTS " + trigger)
	done := make(chan error, 1)
	go func() { done <- fenced.SaveOffset(ctx, key, Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}) }()
	// Observe the actual pending MySQL advisory lock; no timing assumption
	// is used to conclude that the fenced transaction has reached its write.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_TYPE='USER LEVEL LOCK' AND OBJECT_NAME=? AND LOCK_STATUS='PENDING'", gate).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("write passed gate: %v", err)
		case <-deadline.C:
			t.Fatal("write did not reach gate")
		case <-ticker.C:
		}
	}
	contender, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	if _, err := contender.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout=1"); err != nil {
		t.Fatal(err)
	}
	_, err = contender.ExecContext(ctx, "UPDATE job_registry SET lease_owner='takeover-owner' WHERE job_id=?", key)
	var mysqlErr *drivermysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1205 {
		t.Fatalf("takeover did not wait on registry row lock: %v", err)
	}
	if _, err := owner.ExecContext(ctx, "DO RELEASE_LOCK(?)", gate); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := contender.ExecContext(ctx, "UPDATE job_registry SET lease_owner='takeover-owner' WHERE job_id=?", key); err != nil {
		t.Fatal(err)
	}
	if err := fenced.SaveOffset(ctx, key, Offset{BinlogFile: "stale", BinlogPos: 999}); !errors.Is(err, ErrWorkerOwnershipLost) {
		t.Fatalf("stale write=%v", err)
	}
	off, err := store.GetOffset(ctx, key)
	if err != nil || off == nil || off.BinlogFile != "mysql-bin.000001" || off.BinlogPos != 42 {
		t.Fatalf("durable offset=%v error=%v", off, err)
	}
	t.Log("real MySQL transaction locked registry during offset mutation; competing session got 1205; stale token rejected after takeover")
}

func TestWorkerFenceRealMySQLLeaseExpiresDuringMutation(t *testing.T) {
	db, store, fenced, key := realFenceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	trigger := "slow_" + key
	query := "CREATE TRIGGER " + trigger + " BEFORE UPDATE ON job_offsets FOR EACH ROW BEGIN IF NEW.job_id='" + key + "' THEN DO SLEEP(2); END IF; END"
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TRIGGER IF EXISTS " + trigger)
	if _, err := db.ExecContext(ctx, "UPDATE job_registry SET lease_until=UTC_TIMESTAMP(6)+INTERVAL 1 SECOND WHERE job_id=?", key); err != nil {
		t.Fatal(err)
	}
	if err := fenced.SaveOffset(ctx, key, Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}); !errors.Is(err, ErrWorkerOwnershipLost) {
		t.Fatalf("expired mutation=%v", err)
	}
	off, err := store.GetOffset(ctx, key)
	if err != nil || off == nil || off.BinlogPos != 4 {
		t.Fatalf("expired transaction did not roll back: offset=%v error=%v", off, err)
	}
	t.Log("real MySQL lease expired inside slow SQL mutation; second ownership check rolled back offset 42 to 4")
}
