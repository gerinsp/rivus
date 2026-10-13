//go:build integration

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/google/uuid"
)

func TestDeleteRecoveryRealMySQLReturnsConflictWithoutRemovingJob(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_INTEGRATION") != "1" {
		t.Skip("isolated Colima fixture opt-in")
	}
	ctx := context.Background()
	const dsn = "root:recovery-test-only@tcp(127.0.0.1:49317)/recovery_meta?parseTime=true&timeout=5s"
	registry, err := meta.NewMySQLJobStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	store, err := meta.NewMySQLOffsetStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := registry.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	id := "delete_receipt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	key, op := id+"_state", uuid.NewString()
	cfg := &config.JobConfig{ID: id, Mode: config.JobModeInitial, Meta: config.MetaConfig{MySQLDSN: dsn}, Source: &config.ConnectorSpec{Type: "mysql"}, Sink: &config.ConnectorSpec{Type: "iceberg_native"}}
	if err := registry.SaveJob(ctx, meta.PersistedJob{ID: id, SubmissionID: uuid.NewString(), Config: cfg, MetaKey: key, DesiredState: meta.DesiredStateStopped, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "FAILED"}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.DeleteIcebergCommit(ctx, key, op)
		store.DeleteJobState(ctx, key)
		registry.DeleteJob(ctx, id)
	}()
	initial := meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 4}
	if err := store.SaveOffset(ctx, key, initial); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginIcebergCommit(ctx, meta.IcebergCommitIntent{JobID: id, StateKey: key, OperationID: op, Kind: "cdc", Operation: "append", SourceEndFile: initial.BinlogFile, SourceEndPos: 42}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkIcebergCommitCommitted(ctx, key, op); err != nil {
		t.Fatal(err)
	}
	manager := core.NewJobManager(connector.NewRegistry(), core.WithJobStore(registry), core.WithControlPlaneRole())
	if err := manager.RestorePersistedJobs(ctx); err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(ctx)
	server := &Server{jobManager: manager}
	response := httptest.NewRecorder()
	server.handleJobByID(response, httptest.NewRequest(http.MethodDelete, "/api/jobs/"+id, nil))
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), op) {
		t.Fatalf("delete status=%d response=%s", response.Code, response.Body.String())
	}
	if job, err := manager.Get(id); err != nil || job.GetStatus() != core.JobStatusFailed {
		t.Fatal("conflict removed job from UI projection")
	}
	if off, err := store.GetOffset(ctx, key); err != nil || off == nil || off.BinlogPos != 4 {
		t.Fatal("rejected delete changed checkpoint")
	}
	if intents, err := store.ListIcebergCommits(ctx, key); err != nil || len(intents) != 1 || intents[0].OperationID != op {
		t.Fatal("rejected delete removed commit receipt")
	}
}
