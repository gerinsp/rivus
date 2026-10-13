//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

func compatibilityBaseline(t *testing.T) string {
	t.Helper()
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima fixture opt-in")
	}
	binary := os.Getenv("RIVUS_COMPATIBILITY_BASELINE_BINARY")
	revision, err := os.ReadFile(binary + ".revision")
	if err != nil || strings.TrimSpace(string(revision)) != "ff300c01937e5bbd408aa268a4b0923b53e211c6" {
		t.Fatal("build the pinned compatibility baseline first; no substitute/skip permitted")
	}
	return binary
}

func compatibilityInputFile(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runCompatibilityBaseline(t *testing.T, binary, test string, crash bool, env ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run=^"+test+"$", "-test.v")
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("baseline timed out: %s", output)
	}
	if !crash {
		if err != nil {
			t.Fatalf("baseline failed: %v\n%s", err, output)
		}
		return
	}
	exit, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("baseline did not crash: %v\n%s", err, output)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("baseline failed without SIGKILL: %v\n%s", err, output)
	}
}

func TestCompatibilityRealDeployedWorkerAndNewWorker(t *testing.T) {
	binary := compatibilityBaseline(t)
	t.Run("old worker rejects rebuild protocol before catalog access", func(t *testing.T) {
		runCompatibilityBaseline(t, binary, "TestCompatibilityOldRebuildModeRejected", false)
	})
	for _, direction := range []string{"old receipt to new worker", "new receipt to old worker", "old owner after new takeover"} {
		t.Run(direction, func(t *testing.T) {
			ctx := context.Background()
			store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.Init(ctx); err != nil {
				t.Fatal(err)
			}
			key := "compatibility_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			defer func() {
				intents, _ := store.ListIcebergCommits(ctx, key)
				for _, intent := range intents {
					store.DeleteIcebergCommit(ctx, key, intent.OperationID)
				}
				store.DeleteJobState(ctx, key)
			}()
			if err := store.SaveOffset(ctx, key, meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 4}); err != nil {
				t.Fatal(err)
			}
			if direction == "old owner after new takeover" {
				registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer registry.Close()
				if err := registry.Init(ctx); err != nil {
					t.Fatal(err)
				}
				defer registry.DeleteJob(ctx, key)
				record := meta.PersistedJob{ID: key, MetaKey: key, SubmissionID: uuid.NewString(), Config: &config.JobConfig{ID: key}, DesiredState: meta.DesiredStateRunning, LastStatus: "QUEUED", ExecutionRole: meta.JobExecutionRoleStreaming}
				if err := registry.SaveJob(ctx, record); err != nil {
					t.Fatal(err)
				}
				claimed, err := registry.ClaimJobs(ctx, meta.JobExecutionRoleStreaming, "old-compatible-worker", 100, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, row := range claimed {
					if row.ID == key {
						found = true
					}
				}
				if !found {
					t.Fatal("old owner not claimed")
				}
				db, err := sql.Open("mysql", recoveryMetaDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.ExecContext(ctx, "UPDATE job_registry SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE job_id=?", key); err != nil {
					t.Fatal(err)
				}
				claimed, err = registry.ClaimJobs(ctx, meta.JobExecutionRoleStreaming, "new-compatible-worker", 100, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				found = false
				for _, row := range claimed {
					if row.ID == key && row.LeaseOwner == "new-compatible-worker" {
						found = true
					}
				}
				if !found {
					t.Fatal("new owner did not take over expired lease")
				}
				input := compatibilityInputFile(t, meta.WorkerOwnership{JobID: key, StateKey: key, SubmissionID: record.SubmissionID, Owner: "old-compatible-worker"})
				runCompatibilityBaseline(t, binary, "TestCompatibilityOldFenceRejectsNewOwner", false, "RIVUS_COMPATIBILITY_OWNERSHIP_INPUT="+input)
				if off, err := store.GetOffset(ctx, key); err != nil || off == nil || off.BinlogPos != 4 {
					t.Fatal("old binary changed checkpoint after takeover")
				}
				return
			}
			cat := recoveryCatalog(t)
			tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
			if err != nil {
				t.Fatal(err)
			}
			input := recoveryWorkerInput{StateKey: key, Warehouse: filepath.Dir(filepath.Dir(tbl.Location())), Event: eventAt(1, 42), Checkpoint: model.Event{Type: model.EventTypeCheckpoint, SourceOffset: checkpointAt(42)}}
			path := compatibilityInputFile(t, input)
			if direction == "old receipt to new worker" {
				runCompatibilityBaseline(t, binary, "TestRecoveryCrashWorker", true, "RIVUS_RECOVERY_CHILD_INPUT="+path, "RIVUS_RECOVERY_CRASH_STAGE=after_catalog")
				intents, err := store.ListIcebergCommits(ctx, key)
				if err != nil || len(intents) != 1 || !strings.HasPrefix(intents[0].ReplayKeys, "[") || intents[0].Status != meta.IcebergCommitPrepared {
					t.Fatal("baseline did not leave a legacy PREPARED receipt")
				}
				original := icebergIntentFingerprint(intents[0])
				sink := integrationSink(t, cat, store, input)
				if err := sink.checkIcebergRecovery(ctx); err != nil {
					t.Fatal(err)
				}
				intents, err = store.ListIcebergCommits(ctx, key)
				if err != nil || len(intents) != 1 || intents[0].Status != meta.IcebergCommitCommitted || icebergIntentFingerprint(intents[0]) != original {
					t.Fatal("upgrade altered legacy catalog fingerprint")
				}
				if err := sink.handleEvent(ctx, input.Event); err != nil {
					t.Fatal(err)
				}
				if err := sink.handleEvent(ctx, input.Checkpoint); err != nil {
					t.Fatal(err)
				}
				if err := sink.flushAll(ctx); err != nil {
					t.Fatal(err)
				}
				assertAutomaticData(t, cat, 1, 1)
				if off, err := store.GetOffset(ctx, key); err != nil || off == nil || off.BinlogPos != 42 {
					t.Fatal("upgraded worker did not checkpoint the proven replay")
				}
				if intents, err := store.ListIcebergCommits(ctx, key); err != nil || len(intents) != 0 {
					t.Fatal("legacy replay receipt not cleared after covered checkpoint")
				}
			} else {
				sink := integrationSink(t, cat, store, input)
				if err := sink.handleEvent(ctx, input.Event); err != nil {
					t.Fatal(err)
				}
				if err := sink.flushAll(ctx); err != nil {
					t.Fatal(err)
				}
				intents, err := store.ListIcebergCommits(ctx, key)
				if err != nil || len(intents) != 1 || !strings.HasPrefix(intents[0].ReplayKeys, `{"version":2,`) {
					t.Fatal("new worker did not persist a version 2 receipt")
				}
				runCompatibilityBaseline(t, binary, "TestCompatibilityOldVersion2ReceiptBlocked", false, "RIVUS_RECOVERY_CHILD_INPUT="+path)
				if off, err := store.GetOffset(ctx, key); err != nil || off == nil || off.BinlogPos != 4 {
					t.Fatal("rollback advanced checkpoint across unsupported manifest")
				}
				if intents, err := store.ListIcebergCommits(ctx, key); err != nil || len(intents) != 1 || intents[0].Status != meta.IcebergCommitCommitted {
					t.Fatal("rollback removed receipt")
				}
				assertAutomaticData(t, cat, 1, 1)
			}
		})
	}
}
