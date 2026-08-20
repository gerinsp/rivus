package core

import (
	"context"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

// Extend the existing in-memory test store with the durable lifecycle surface
// used by the master process. Production uses MySQLJobStore.
func (s *memoryJobStore) LoadJobControl(_ context.Context, jobID string) (*meta.JobControlState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.jobs[jobID]
	if !ok {
		return nil, nil
	}
	return &meta.JobControlState{
		DesiredState:  record.DesiredState,
		ExecutionRole: record.ExecutionRole,
		LeaseOwner:    record.LeaseOwner,
		LeaseUntil:    record.LeaseUntil,
		LastStatus:    record.LastStatus,
	}, nil
}

func (s *memoryJobStore) LoadJobPauseRequests(_ context.Context, owner string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := make([]string, 0)
	for id, record := range s.jobs {
		if record.LeaseOwner == owner && record.LeaseUntil.After(now) && record.DesiredState == meta.DesiredStateRunning && record.LastStatus == string(JobStatusPausing) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *memoryJobStore) RequestJobStop(_ context.Context, jobID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.jobs[jobID]
	if !ok {
		return false, nil
	}
	record.DesiredState = meta.DesiredStateStopped
	record.LastStatus = string(JobStatusStopped)
	record.LeaseOwner = ""
	record.LeaseUntil = time.Time{}
	record.UpdatedAt = time.Now()
	s.jobs[jobID] = record
	return true, nil
}

func (s *memoryJobStore) RequestJobPause(_ context.Context, jobID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.jobs[jobID]
	if !ok || record.DesiredState != meta.DesiredStateRunning || record.LastStatus != string(JobStatusRunning) {
		return false, nil
	}
	record.LastStatus = string(JobStatusPausing)
	record.UpdatedAt = time.Now()
	s.jobs[jobID] = record
	return true, nil
}

func (s *memoryJobStore) RequestJobResume(_ context.Context, jobID string, role meta.JobExecutionRole) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.jobs[jobID]
	if !ok {
		return false, nil
	}
	switch record.LastStatus {
	case string(JobStatusPaused), string(JobStatusFailed), string(JobStatusStopped):
	default:
		return false, nil
	}
	record.DesiredState = meta.DesiredStateRunning
	record.ExecutionRole = role
	record.LastStatus = string(JobStatusQueued)
	record.LeaseOwner = ""
	record.LeaseUntil = time.Time{}
	record.UpdatedAt = time.Now()
	s.jobs[jobID] = record
	return true, nil
}

func TestMasterSubmitQueuesWithoutExecuting(t *testing.T) {
	store := newMemoryJobStore()
	reg, modes := newTestRegistry()
	manager := NewJobManager(reg,
		WithJobStore(store),
		WithControlPlaneRole(),
		WithWorkerID("master-1"),
	)

	job, err := manager.Submit(newTestJobConfig("master-submit"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if got := job.GetStatus(); got != JobStatusQueued {
		t.Fatalf("status = %s, want %s", got, JobStatusQueued)
	}
	record, ok := store.Get(job.Config.ID)
	if !ok {
		t.Fatal("submitted job was not persisted")
	}
	if record.ExecutionRole != meta.JobExecutionRoleSnapshot || record.DesiredState != meta.DesiredStateRunning {
		t.Fatalf("record role=%s desired=%s, want SNAPSHOT/RUNNING", record.ExecutionRole, record.DesiredState)
	}
	select {
	case mode := <-modes:
		t.Fatalf("master executed job unexpectedly with mode=%s", mode)
	default:
	}
}

func TestMasterCancelFencesRemoteWorker(t *testing.T) {
	store := newMemoryJobStore()
	manager := NewJobManager(nil, WithJobStore(store), WithControlPlaneRole())
	job, err := manager.Submit(newTestJobConfig("master-cancel"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	store.mu.Lock()
	record := store.jobs[job.Config.ID]
	record.LastStatus = string(JobStatusRunning)
	record.LeaseOwner = "snapshot-1"
	record.LeaseUntil = time.Now().Add(time.Minute)
	store.jobs[job.Config.ID] = record
	store.mu.Unlock()
	setObservedJobStatus(job, JobStatusRunning)

	if err := manager.RequestCancel(job.Config.ID); err != nil {
		t.Fatalf("RequestCancel returned error: %v", err)
	}
	record, _ = store.Get(job.Config.ID)
	if record.DesiredState != meta.DesiredStateStopped || record.LastStatus != string(JobStatusStopped) || record.LeaseOwner != "" {
		t.Fatalf("cancelled record desired=%s status=%s owner=%q", record.DesiredState, record.LastStatus, record.LeaseOwner)
	}
	if got := job.GetStatus(); got != JobStatusStopped {
		t.Fatalf("observed status=%s, want STOPPED", got)
	}
}

func TestMasterPauseKeepsLeaseForGracefulDrain(t *testing.T) {
	store := newMemoryJobStore()
	manager := NewJobManager(nil, WithJobStore(store), WithControlPlaneRole())
	job, err := manager.Submit(newTestJobConfig("master-pause"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	store.mu.Lock()
	record := store.jobs[job.Config.ID]
	record.LastStatus = string(JobStatusRunning)
	record.LeaseOwner = "snapshot-1"
	record.LeaseUntil = time.Now().Add(time.Minute)
	store.jobs[job.Config.ID] = record
	store.mu.Unlock()
	setObservedJobStatus(job, JobStatusRunning)

	if err := manager.RequestPause(job.Config.ID); err != nil {
		t.Fatalf("RequestPause returned error: %v", err)
	}
	record, _ = store.Get(job.Config.ID)
	if record.LastStatus != string(JobStatusPausing) || record.LeaseOwner != "snapshot-1" || record.DesiredState != meta.DesiredStateRunning {
		t.Fatalf("pause record desired=%s status=%s owner=%q", record.DesiredState, record.LastStatus, record.LeaseOwner)
	}
	if got := job.GetStatus(); got != JobStatusPausing {
		t.Fatalf("observed status=%s, want PAUSING", got)
	}
}

func TestMasterResubmitPreservesStreamingHandoffRole(t *testing.T) {
	store := newMemoryJobStore()
	manager := NewJobManager(nil, WithJobStore(store), WithControlPlaneRole())
	job, err := manager.Submit(newTestJobConfig("master-resubmit"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	store.mu.Lock()
	record := store.jobs[job.Config.ID]
	record.DesiredState = meta.DesiredStateStopped
	record.ExecutionRole = meta.JobExecutionRoleStreaming
	record.LastStatus = string(JobStatusStopped)
	record.LeaseOwner = "old-streaming-worker"
	record.LeaseUntil = time.Now().Add(time.Minute)
	store.jobs[job.Config.ID] = record
	store.mu.Unlock()
	setObservedJobStatus(job, JobStatusStopped)

	resubmitted, err := manager.RequestResubmit(job.Config.ID)
	if err != nil {
		t.Fatalf("RequestResubmit returned error: %v", err)
	}
	if resubmitted != job || job.GetStatus() != JobStatusQueued {
		t.Fatalf("resubmitted job status=%s, want QUEUED", job.GetStatus())
	}
	record, _ = store.Get(job.Config.ID)
	if record.ExecutionRole != meta.JobExecutionRoleStreaming || record.DesiredState != meta.DesiredStateRunning || record.LeaseOwner != "" {
		t.Fatalf("resubmit record role=%s desired=%s owner=%q", record.ExecutionRole, record.DesiredState, record.LeaseOwner)
	}
}

func TestResubmitConditionalUpdateDoesNotFenceRunningJob(t *testing.T) {
	store := newMemoryJobStore()
	manager := NewJobManager(nil, WithJobStore(store), WithControlPlaneRole())
	job, err := manager.Submit(newTestJobConfig("master-resubmit-race"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	store.mu.Lock()
	record := store.jobs[job.Config.ID]
	record.ExecutionRole = meta.JobExecutionRoleStreaming
	record.LastStatus = string(JobStatusRunning)
	record.LeaseOwner = "streaming-new-owner"
	record.LeaseUntil = time.Now().Add(time.Minute)
	store.jobs[job.Config.ID] = record
	store.mu.Unlock()
	setObservedJobStatus(job, JobStatusStopped) // stale master view

	if _, err := manager.RequestResubmit(job.Config.ID); err != ErrJobResubmitNotAllowed {
		t.Fatalf("RequestResubmit error=%v, want ErrJobResubmitNotAllowed", err)
	}
	record, _ = store.Get(job.Config.ID)
	if record.LeaseOwner != "streaming-new-owner" || record.LastStatus != string(JobStatusRunning) {
		t.Fatalf("running lease was fenced: status=%s owner=%q", record.LastStatus, record.LeaseOwner)
	}
}

func TestStaleWorkerPersistenceCannotOverwriteTerminalStatus(t *testing.T) {
	store := newMemoryJobStore()
	manager := NewJobManager(nil,
		WithJobStore(store),
		WithWorkerRole(WorkerRoleSnapshot),
		WithWorkerID("snapshot-race"),
	)
	job := NewJob(newTestJobConfig("stale-status-race"), nil)

	if err := store.SaveJob(context.Background(), meta.PersistedJob{
		ID:            job.Config.ID,
		SubmissionID:  job.SubmissionID(),
		Name:          job.Config.Name,
		Config:        job.Config,
		DesiredState:  meta.DesiredStateRunning,
		ExecutionRole: meta.JobExecutionRoleSnapshot,
		LastStatus:    string(JobStatusRunning),
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := store.ClaimJobs(context.Background(), meta.JobExecutionRoleSnapshot, "snapshot-race", 1, time.Minute); err != nil {
		t.Fatalf("claim job: %v", err)
	}

	manager.mu.Lock()
	manager.jobs[job.Config.ID] = job
	manager.executionRoles[job.Config.ID] = meta.JobExecutionRoleSnapshot
	manager.workerLeases[job.Config.ID] = job.SubmissionID()
	manager.mu.Unlock()

	setObservedJobStatus(job, JobStatusDone)
	store.mu.Lock()
	record := store.jobs[job.Config.ID]
	record.DesiredState = meta.DesiredStateStopped
	record.LastStatus = string(JobStatusDone)
	store.jobs[job.Config.ID] = record
	store.mu.Unlock()

	if err := manager.saveManagedJobRecordIfCurrent(context.Background(), job, JobStatusRunning, meta.DesiredStateRunning, JobStatusRunning); err != nil {
		t.Fatalf("stale save returned error: %v", err)
	}
	record, _ = store.Get(job.Config.ID)
	if record.LastStatus != string(JobStatusDone) || record.DesiredState != meta.DesiredStateStopped {
		t.Fatalf("stale save overwrote terminal row: status=%s desired=%s", record.LastStatus, record.DesiredState)
	}

	manager.mu.Lock()
	manager.executionRoles[job.Config.ID] = meta.JobExecutionRoleStreaming
	manager.mu.Unlock()
	if err := manager.saveManagedJobRecordIfCurrent(context.Background(), job, JobStatusDone, meta.DesiredStateRunning, JobStatusStopped); err != nil {
		t.Fatalf("handoff save returned error: %v", err)
	}
	record, _ = store.Get(job.Config.ID)
	if record.LastStatus != string(JobStatusStopped) || record.DesiredState != meta.DesiredStateRunning || record.ExecutionRole != meta.JobExecutionRoleStreaming {
		t.Fatalf("handoff save status=%s desired=%s role=%s", record.LastStatus, record.DesiredState, record.ExecutionRole)
	}
}

func TestWorkerAppliesDurablePauseGracefully(t *testing.T) {
	store := newMemoryJobStore()
	drainStarted := make(chan struct{}, 1)
	allowDrain := make(chan struct{})
	reg, _ := newGracefulPauseTestRegistry(drainStarted, allowDrain)
	cfg := newTestJobConfig("worker-remote-pause")
	cfg.Mode = config.JobModeLatest

	if err := store.SaveJob(context.Background(), meta.PersistedJob{
		ID:            cfg.ID,
		Name:          cfg.Name,
		Config:        cfg,
		DesiredState:  meta.DesiredStateRunning,
		ExecutionRole: meta.JobExecutionRoleStreaming,
		LastStatus:    string(JobStatusQueued),
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	manager := NewJobManager(reg,
		WithJobStore(store),
		WithWorkerRole(WorkerRoleStreaming),
		WithWorkerID("streaming-pause-1"),
	)
	if err := manager.RestorePersistedJobs(context.Background()); err != nil {
		t.Fatalf("RestorePersistedJobs returned error: %v", err)
	}
	if err := manager.reconcileWorkerJobs(context.Background(), store); err != nil {
		t.Fatalf("reconcileWorkerJobs returned error: %v", err)
	}
	job, err := manager.Get(cfg.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	waitForCondition(t, "streaming job to run", func() bool { return job.GetStatus() == JobStatusRunning })
	// Job status becomes visible in memory before its synchronous persistence
	// listener finishes. A real control-plane pause reads the durable row, so
	// wait for that same externally observable precondition instead of racing
	// the listener on fast CI machines.
	waitForCondition(t, "streaming job running state to persist", func() bool {
		record, ok := store.Get(cfg.ID)
		return ok &&
			record.LastStatus == string(JobStatusRunning) &&
			record.DesiredState == meta.DesiredStateRunning &&
			record.LeaseOwner == "streaming-pause-1"
	})

	if updated, err := store.RequestJobPause(context.Background(), cfg.ID); err != nil || !updated {
		t.Fatalf("RequestJobPause updated=%t err=%v", updated, err)
	}
	if err := manager.applyDurablePauseRequests(context.Background(), store); err != nil {
		t.Fatalf("applyDurablePauseRequests returned error: %v", err)
	}
	if got := job.GetStatus(); got != JobStatusPausing {
		t.Fatalf("worker status=%s, want PAUSING", got)
	}

	select {
	case <-drainStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("sink did not begin graceful drain")
	}
	close(allowDrain)
	waitForCondition(t, "streaming job to pause", func() bool { return job.GetStatus() == JobStatusPaused })
	var record meta.PersistedJob
	waitForCondition(t, "streaming paused state to persist", func() bool {
		var ok bool
		record, ok = store.Get(cfg.ID)
		return ok && record.LastStatus == string(JobStatusPaused) && record.DesiredState == meta.DesiredStateStopped
	})
	if record.LastStatus != string(JobStatusPaused) || record.DesiredState != meta.DesiredStateStopped {
		t.Fatalf("paused record desired=%s status=%s", record.DesiredState, record.LastStatus)
	}
}
