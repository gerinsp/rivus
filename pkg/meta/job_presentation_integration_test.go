//go:build integration

package meta

import (
	"context"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestJobPresentationRealRenameSurvivesWorkerUpdates(t *testing.T) {
	_, offsets, _, id := realFenceFixture(t)
	ctx := context.Background()
	s, err := NewMySQLJobStore(fenceIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	records, err := s.LoadJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var old PersistedJob
	for _, r := range records {
		if r.ID == id {
			old = r
		}
	}
	sid := old.SubmissionID
	if ok, err := s.RenameJob(ctx, id, "  Sync (btm)  "); err != nil || !ok {
		t.Fatalf("rename %v %v", ok, err)
	}
	old.Name = "obsolete"
	old.Config.Name = "obsolete"
	if ok, err := s.SaveClaimedJob(ctx, old, "fence-owner"); err != nil || !ok {
		t.Fatalf("save %v %v", ok, err)
	}
	// A control-plane UPSERT must also preserve the independently edited name.
	if err := s.SaveJob(ctx, old); err != nil {
		t.Fatal(err)
	}
	records, err = s.LoadJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.ID == id {
			if r.Name != "Sync (btm)" || r.Config.Name != r.Name || r.SubmissionID != sid || r.LeaseOwner != "fence-owner" || r.MetaKey != id || r.LastStatus != "RUNNING" {
				t.Fatalf("name update changed lifecycle or was overwritten: %+v", r)
			}
		}
	}
	off, err := offsets.GetOffset(ctx, id)
	if err != nil || off == nil || off.BinlogPos != 4 {
		t.Fatalf("checkpoint changed: %+v %v", off, err)
	}
	if ok, err := s.RenameJob(ctx, id, " "); err == nil || ok {
		t.Fatal("empty name accepted")
	}
	if ok, err := s.RenameJob(ctx, id+"missing", "name"); err != nil || ok {
		t.Fatalf("missing: %v %v", ok, err)
	}
}
func TestJobPresentationRealExplicitStopKeepsRecoveryEvidence(t *testing.T) {
	db, offsets, _, prefix := realFenceFixture(t)
	s, err := NewMySQLJobStore(fenceIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()
	for _, kind := range []string{"released", "expired", "live", "running-desired"} {
		id := prefix + "_" + kind
		cfg := &config.JobConfig{ID: id}
		desired := DesiredStateStopped
		if kind == "running-desired" {
			desired = DesiredStateRunning
		}
		if err := s.SaveJob(ctx, PersistedJob{ID: id, SubmissionID: "stable", Config: cfg, MetaKey: id, DesiredState: desired, ExecutionRole: JobExecutionRoleStreaming, LastStatus: "STOPPING"}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", id)
			offsets.DeleteJobState(context.Background(), id)
			s.DeleteJob(context.Background(), id)
		})
		if kind != "released" {
			until := time.Now().UTC().Add(time.Minute)
			if kind == "expired" {
				until = time.Now().UTC().Add(-time.Minute)
			}
			if _, err := db.Exec("UPDATE job_registry SET lease_owner='owner',lease_until=? WHERE job_id=?", until, id); err != nil {
				t.Fatal(err)
			}
		}
		if err := offsets.SaveOffset(ctx, id, Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}); err != nil {
			t.Fatal(err)
		}
		if err := offsets.BeginIcebergCommit(ctx, IcebergCommitIntent{OperationID: uuid.NewString(), JobID: id, StateKey: id, BatchID: "unknown", Kind: "cdc", Status: IcebergCommitPrepared}); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"released", "expired"} {
		if ok, err := s.RequestJobStop(ctx, prefix+"_"+kind); err != nil || !ok {
			t.Fatalf("stop %v %v", ok, err)
		}
	}
	for _, kind := range []string{"released", "expired"} {
		if ok, err := s.RequestJobStop(ctx, prefix+"_"+kind); err != nil || !ok {
			t.Fatalf("stop %v %v", ok, err)
		}
	}
	for _, kind := range []string{"released", "expired", "live", "running-desired"} {
		id := prefix + "_" + kind
		control, err := s.LoadJobControl(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		want := "STOPPED"
		if kind == "live" || kind == "running-desired" {
			want = "STOPPING"
		}
		if control.LastStatus != want {
			t.Fatalf("%s status %s want %s", kind, control.LastStatus, want)
		}
		off, err := offsets.GetOffset(ctx, id)
		if err != nil || off == nil || off.BinlogPos != 42 {
			t.Fatalf("offset changed %s", kind)
		}
		intents, err := offsets.ListIcebergCommits(ctx, id)
		if err != nil || len(intents) != 1 || intents[0].Status != IcebergCommitPrepared {
			t.Fatalf("evidence changed %s", kind)
		}
	}
}
