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

	"github.com/apache/iceberg-go/catalog/hadoop"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

func TestRecoveryRealProcessCrashWithCheckpointInsideCommittedBatch(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	key := "midbatch_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	source, err := sql.Open("mysql", recoverySourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.ExecContext(ctx, "CREATE TABLE "+key+" (id BIGINT PRIMARY KEY,status VARCHAR(32))"); err != nil {
		t.Fatal(err)
	}
	defer source.Exec("DROP TABLE " + key)
	store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		intents, _ := store.ListIcebergCommits(context.Background(), key)
		for _, intent := range intents {
			store.DeleteIcebergCommit(context.Background(), key, intent.OperationID)
		}
		store.DeleteJobState(context.Background(), key)
	}()
	position := func() meta.Offset {
		var file string
		var pos uint32
		var a, b, c any
		if err := source.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&file, &pos, &a, &b, &c); err != nil {
			t.Fatal(err)
		}
		return meta.Offset{BinlogFile: file, BinlogPos: pos}
	}
	initial := position()
	if err := store.SaveSnapshotStart(ctx, key, initial); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSnapshotDone(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOffset(ctx, key, initial); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, "INSERT INTO "+key+" VALUES (1,'pending')"); err != nil {
		t.Fatal(err)
	}
	middle := position()
	if _, err := source.ExecContext(ctx, "INSERT INTO "+key+" VALUES (2,'pending')"); err != nil {
		t.Fatal(err)
	}
	events, checkpoint := sourceCDCEvents(t, store, key, key, 2)
	if len(events) != 2 || !binlogOffsetCovers(middle.BinlogFile, middle.BinlogPos, events[0].SourceOffset.BinlogFile, events[0].SourceOffset.BinlogPos) || binlogOffsetCovers(middle.BinlogFile, middle.BinlogPos, events[1].SourceOffset.BinlogFile, events[1].SourceOffset.BinlogPos) {
		t.Fatal("real checkpoint did not separate two source transactions")
	}
	cat := recoveryCatalog(t)
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	warehouse := filepath.Dir(filepath.Dir(tbl.Location()))
	input := recoveryWorkerInput{StateKey: key, Warehouse: warehouse, Event: events[0], ExtraEvent: &events[1], Checkpoint: checkpoint, LaggingCheckpoint: &model.SourceOffset{BinlogFile: middle.BinlogFile, BinlogPos: middle.BinlogPos}}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(inputPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryCrashWorker$", "-test.v")
	cmd.Env = append(os.Environ(), "RIVUS_RECOVERY_CHILD_INPUT="+inputPath, "RIVUS_RECOVERY_CRASH_STAGE=after_checkpoint")
	output, err := cmd.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("worker was not killed: %v\n%s", err, output)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("worker did not receive SIGKILL: %v\n%s", err, output)
	}
	saved, err := store.GetOffset(ctx, key)
	if err != nil || saved == nil || saved.BinlogFile != middle.BinlogFile || saved.BinlogPos != middle.BinlogPos {
		t.Fatalf("mid-batch checkpoint=%v error=%v", saved, err)
	}
	intents, err := store.ListIcebergCommits(ctx, key)
	if err != nil || len(intents) != 1 || intents[0].Status != meta.IcebergCommitCommitted {
		t.Fatalf("durable receipt=%v error=%v", intents, err)
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
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	replay, finalCheckpoint := sourceCDCEvents(t, reopened, key, key, 2)
	if len(replay) != 1 || replay[0].Data["id"] != int64(2) {
		t.Fatal("source restart did not replay only suffix")
	}
	if err := restarted.handleEvent(ctx, replay[0]); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(ctx, finalCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := restarted.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	assertAutomaticData(t, freshCatalog, 2, 1)
	if intents, err := reopened.ListIcebergCommits(ctx, key); err != nil || len(intents) != 0 {
		t.Fatal("completed suffix replay retained receipt")
	}
	if _, err := source.ExecContext(ctx, "INSERT INTO "+key+" VALUES (3,'pending')"); err != nil {
		t.Fatal(err)
	}
	next, nextCheckpoint := sourceCDCEvents(t, reopened, key, key, 3)
	if err := restarted.handleEvent(ctx, next[0]); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(ctx, nextCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := restarted.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	assertAutomaticData(t, freshCatalog, 3, 2)
	t.Log("SIGKILL after interior checkpoint; actual MySQL suffix replay skipped accepted row and CDC continued without duplicate")
}
