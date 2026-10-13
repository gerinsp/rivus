//go:build integration

package meta

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// Each migration test owns an entire database, so it can exercise old DDL
// without dropping tables used by the crash/fence fixtures.
func compatibilitySchemaFixture(t *testing.T) (context.Context, *sql.DB, string) {
	t.Helper()
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	cfg, err := drivermysql.ParseDSN(fenceIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	name := "compat_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanupCtx, "DROP DATABASE "+name); err != nil {
			t.Errorf("cleanup isolated compatibility database: %v", err)
		}
		admin.Close()
	})
	cfg.DBName = name
	dsn := cfg.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return ctx, db, dsn
}

func TestCompatibilitySchemaRealLegacyRegistryMigration(t *testing.T) {
	ctx, db, dsn := compatibilitySchemaFixture(t)
	_, err := db.ExecContext(ctx, `CREATE TABLE job_registry (
		job_id VARCHAR(255) PRIMARY KEY, job_name VARCHAR(255) NOT NULL,
		config_json LONGTEXT NOT NULL, desired_state VARCHAR(32) NOT NULL,
		last_status VARCHAR(32) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)`)
	if err != nil {
		t.Fatal(err)
	}
	const original = `{"id":"legacy","mode":"initial","metadata":{"operator":"preserve"},"retry":{"max_attempts":7}}`
	if _, err := db.ExecContext(ctx, `INSERT INTO job_registry
		(job_id,job_name,config_json,desired_state,last_status) VALUES ('legacy','Legacy',?,'STOPPED','STOPPING')`, original); err != nil {
		t.Fatal(err)
	}
	store, err := NewMySQLJobStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	var token string
	for run := 0; run < 2; run++ {
		if err := store.Init(ctx); err != nil {
			t.Fatal(err)
		}
		jobs, err := store.LoadJobs(ctx)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("load migrated registry: jobs=%d error=%v", len(jobs), err)
		}
		job := jobs[0]
		if job.ID != "legacy" || job.LastStatus != "STOPPING" || job.DesiredState != DesiredStateStopped || job.ExecutionRole != JobExecutionRoleAll || job.ResumeRequested || job.LeaseOwner != "" || !job.LeaseUntil.IsZero() {
			t.Fatal("migration changed lifecycle or synthesized ownership")
		}
		if job.SubmissionID == "" || (run > 0 && job.SubmissionID != token) {
			t.Fatal("migration did not provide a stable submission fence")
		}
		token = job.SubmissionID
		var payload string
		if err := db.QueryRowContext(ctx, "SELECT config_json FROM job_registry WHERE job_id='legacy'").Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if payload != original || job.Config.ID != "legacy" || job.Config.Metadata["operator"] != "preserve" {
			t.Fatal("migration rewrote legacy configuration")
		}
	}
}

func TestCompatibilitySchemaRealLegacySnapshotMigration(t *testing.T) {
	ctx, db, dsn := compatibilitySchemaFixture(t)
	for _, stmt := range []string{
		`CREATE TABLE job_offsets (job_id VARCHAR(255) PRIMARY KEY,
		binlog_file VARCHAR(255) NOT NULL, binlog_pos BIGINT NOT NULL,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)`,
		`CREATE TABLE job_snapshots (job_id VARCHAR(255) PRIMARY KEY,
		start_file VARCHAR(255) NOT NULL, start_pos BIGINT NOT NULL,
		done TINYINT(1) NOT NULL DEFAULT 0,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)`,
		`CREATE TABLE job_snapshot_progress (job_id VARCHAR(255) PRIMARY KEY,
		table_name VARCHAR(255) NOT NULL, next_offset BIGINT NOT NULL,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)`,
		`INSERT INTO job_snapshots (job_id,start_file,start_pos,done) VALUES
		('legacy_done','mysql-bin.000123',456,1),('unfinished','mysql-bin.000123',456,0)`,
		`INSERT INTO job_snapshot_progress (job_id,table_name,next_offset)
		VALUES ('legacy_done','app.orders',11),('unfinished','app.orders',11)`,
		`INSERT INTO job_offsets (job_id,binlog_file,binlog_pos) VALUES
		('legacy_done','mysql-bin.000123',789),('unfinished','mysql-bin.000123',456)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	store, err := NewMySQLOffsetStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	for run := 0; run < 2; run++ {
		if err := store.Init(ctx); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"legacy_done", "unfinished"} {
			checkpoint, err := store.GetOffset(ctx, key)
			position := uint32(456)
			if key == "legacy_done" {
				position = 789
			}
			if err != nil || checkpoint == nil || checkpoint.BinlogFile != "mysql-bin.000123" || checkpoint.BinlogPos != position {
				t.Fatalf("migration changed checkpoint: checkpoint=%+v error=%v", checkpoint, err)
			}
			state, err := store.GetSnapshotState(ctx, key)
			if err != nil || state == nil || state.StartFile != "mysql-bin.000123" || state.StartPos != 456 || state.Done != (key == "legacy_done") || state.CompletionVersion != 0 || state.CompletionConfirmed() {
				t.Fatalf("migration altered or attested old snapshot: state=%+v error=%v", state, err)
			}
			progress, err := store.GetSnapshotProgress(ctx, key)
			if err != nil || progress == nil || progress.TableName != "app.orders" || progress.NextOffset != 11 || progress.CursorJSON != "" {
				t.Fatalf("migration changed legacy progress: progress=%+v error=%v", progress, err)
			}
		}
	}
	if err := store.MarkSnapshotDone(ctx, "legacy_done"); err == nil {
		t.Fatal("empty restarted sink attested legacy done state")
	}
	state, err := store.GetSnapshotState(ctx, "legacy_done")
	if err != nil || state.CompletionConfirmed() || state.CompletionVersion != 0 {
		t.Fatal("rejected legacy completion changed its version")
	}
	// This calls the metadata barrier only; it does not simulate a data commit.
	// Existing sink tests establish when the caller may invoke this barrier.
	for run := 0; run < 2; run++ {
		if err := store.MarkSnapshotDone(ctx, "unfinished"); err != nil {
			t.Fatal(err)
		}
	}
	state, err = store.GetSnapshotState(ctx, "unfinished")
	if err != nil || !state.CompletionConfirmed() || state.StartPos != 456 {
		t.Fatal("migrated unfinished state cannot complete idempotently")
	}
	progress, err := store.GetSnapshotProgress(ctx, "unfinished")
	if err != nil || progress == nil || progress.NextOffset != 11 {
		t.Fatal("completion removed progress before source acknowledgment")
	}
}
