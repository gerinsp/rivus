package core

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

type recoveryWorkerStore struct {
	*memoryJobStore
	token  meta.WorkerOwnership
	fenced meta.OffsetStore
}

func (s *recoveryWorkerStore) FenceWorkerOffsets(base meta.OffsetStore, token meta.WorkerOwnership) (meta.OffsetStore, error) {
	s.token = token
	s.fenced = &coreOwnedOffsetStore{OffsetStore: base}
	return s.fenced, nil
}

type coreOwnedOffsetStore struct{ meta.OffsetStore }

func (*coreOwnedOffsetStore) CheckOwnership(context.Context) error { return nil }

func TestAutomaticRecoveryClaimFencesBothConnectorsBeforeConstruction(t *testing.T) {
	for _, capable := range []bool{true, false} {
		t.Run(map[bool]string{true: "fenced", false: "unsupported"}[capable], func(t *testing.T) {
			base := newMemoryJobStore()
			fencer := &recoveryWorkerStore{memoryJobStore: base}
			var store meta.JobStore = base
			if capable {
				store = fencer
			}
			reg := connector.NewRegistry()
			constructed := 0
			check := func(jctx connector.JobContext) error {
				constructed++
				if jctx.MetaStore != fencer.fenced || fencer.fenced == nil {
					return errors.New("connector received unfenced checkpoint store")
				}
				return nil
			}
			reg.RegisterSource("test_source", func(jctx connector.JobContext, _ any) (connector.Source, error) {
				return &closingSource{}, check(jctx)
			})
			reg.RegisterSink("iceberg_native", func(jctx connector.JobContext, _ any) (connector.Sink, error) {
				return &closingSink{}, check(jctx)
			})
			manager := NewJobManager(reg, WithJobStore(store), WithWorkerRole(WorkerRoleStreaming), WithWorkerID("owner"))
			cfg := newTestJobConfig("fenced-job")
			cfg.Sink.Type = "iceberg_native"
			cfg.Mode = config.JobModeLatest
			job := manager.newManagedJob(cfg)
			job.metaStore = &closingOffsetStore{}
			manager.jobs[cfg.ID] = job
			record := meta.PersistedJob{ID: cfg.ID, Config: cfg, SubmissionID: job.SubmissionID(), MetaKey: "state", LeaseOwner: "owner", LeaseUntil: time.Now().Add(time.Minute), DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: string(JobStatusCreated)}
			base.jobs[cfg.ID] = record
			err := manager.startClaimedWorkerJob(record)
			if !capable {
				if err == nil || constructed != 0 {
					t.Fatalf("unsupported ownership store: err=%v constructed=%d", err, constructed)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !job.waitRunDone(time.Second) {
				t.Fatal("job did not finish")
			}
			if constructed != 2 || fencer.token.JobID != cfg.ID || fencer.token.SubmissionID != record.SubmissionID || fencer.token.Owner != "owner" || fencer.token.StateKey != "state" {
				t.Fatalf("fence token=%#v constructed=%d", fencer.token, constructed)
			}
		})
	}
}

func TestAutomaticRecoveryProgressSurvivesSourceAndSinkHeartbeats(t *testing.T) {
	j := NewJob(&config.JobConfig{ID: "job"}, nil)
	j.setStatus(JobStatusRunning)
	j.updateProgress(connector.ProgressInfo{Phase: "streaming", Summary: "CDC streaming"})
	started := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	j.updateProgress(connector.ProgressInfo{Phase: "sink_recovery", RecoveryStatus: "RECOVERING", RecoveryOperationID: "operation", RecoveryAttempts: 2, RecoveryLastError: "catalog unavailable", RecoveryCheckpoint: "mysql-bin.000001:4", RecoveryStartedAt: started})
	for _, info := range []connector.ProgressInfo{{Phase: "cdc_health"}, {Phase: "sink_commit"}, {Phase: "streaming"}} {
		j.updateProgress(info)
		raw, err := json.Marshal(j.Progress())
		if err != nil {
			t.Fatal(err)
		}
		var p map[string]interface{}
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		if p["recovery_status"] != "RECOVERING" || p["recovery_operation_id"] != "operation" || p["recovery_attempts"] != float64(2) || p["recovery_last_error"] != "catalog unavailable" || p["recovery_checkpoint"] != "mysql-bin.000001:4" {
			t.Fatalf("heartbeat discarded recovery: %s", raw)
		}
	}
	if j.GetStatus() != JobStatusRunning {
		t.Fatal("recovery stopped desired running lifecycle")
	}
	if desiredStateForStatus(JobStatusRunning) != meta.DesiredStateRunning {
		t.Fatal("recovery not restartable")
	}
}

func TestSplitWorkerShutdownRetainsLeaseUntilDrainedStatePersisted(t *testing.T) {
	store := newMemoryJobStore()
	drainStarted, allowDrain := make(chan struct{}, 1), make(chan struct{})
	reg, _ := newGracefulPauseTestRegistry(drainStarted, allowDrain)
	manager := NewJobManager(reg, WithJobStore(store), WithWorkerRole(WorkerRoleStreaming), WithWorkerID("shutdown-worker"))
	cfg := newTestJobConfig("split-shutdown-lease")
	cfg.Mode = config.JobModeLatest
	job := manager.newManagedJob(cfg)
	manager.jobs[cfg.ID] = job
	record := meta.PersistedJob{ID: cfg.ID, SubmissionID: job.SubmissionID(), Config: cfg, DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: string(JobStatusCreated), LeaseOwner: "shutdown-worker", LeaseUntil: time.Now().Add(time.Minute)}
	store.jobs[cfg.ID] = record
	if err := manager.startClaimedWorkerJob(record); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, "running worker", func() bool { return job.GetStatus() == JobStatusRunning })
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- manager.Shutdown(ctx)
	}()
	select {
	case <-drainStarted:
	case <-time.After(time.Second):
		t.Fatal("drain did not start")
	}
	close(allowDrain)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown lost its own lease: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	after, ok := store.Get(cfg.ID)
	if !ok || after.DesiredState != meta.DesiredStateRunning || after.LastStatus != string(JobStatusPaused) || after.SubmissionID != record.SubmissionID || after.LeaseOwner != "" {
		t.Fatalf("drained registry=%#v", after)
	}
}
