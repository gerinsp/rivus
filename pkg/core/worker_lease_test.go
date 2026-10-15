package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

type blockingClaimStore struct {
	claimStarted chan struct{}
	claimOnce    sync.Once

	mu         sync.Mutex
	renewCount int
}

func (s *blockingClaimStore) Init(context.Context) error { return nil }

func (s *blockingClaimStore) SaveJob(context.Context, meta.PersistedJob) error { return nil }

func (s *blockingClaimStore) LoadJobs(context.Context) ([]meta.PersistedJob, error) { return nil, nil }

func (s *blockingClaimStore) DeleteJob(context.Context, string) error { return nil }

func (s *blockingClaimStore) ClaimJobs(ctx context.Context, _ meta.JobExecutionRole, _ string, limit int, _ time.Duration) ([]meta.PersistedJob, error) {
	if limit != 1 {
		return nil, &unexpectedClaimLimitError{got: limit}
	}
	s.claimOnce.Do(func() { close(s.claimStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *blockingClaimStore) RenewJobLease(context.Context, string, string, string, time.Duration) (bool, error) {
	s.mu.Lock()
	s.renewCount++
	s.mu.Unlock()
	return true, nil
}

func (s *blockingClaimStore) ReleaseJobLease(context.Context, string, string, string) error {
	return nil
}

func (s *blockingClaimStore) renewals() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renewCount
}

type unexpectedClaimLimitError struct{ got int }

func (e *unexpectedClaimLimitError) Error() string { return "unexpected worker claim limit" }

func TestRunWorkerRenewsLeasesWhileClaimOrStartupIsBlocked(t *testing.T) {
	store := &blockingClaimStore{claimStarted: make(chan struct{})}
	manager := NewJobManager(nil,
		WithJobStore(store),
		WithWorkerRole(WorkerRoleStreaming),
		WithWorkerID("lease-test-worker"),
		WithWorkerTiming(5*time.Millisecond, 60*time.Millisecond),
	)
	job := NewJob(newTestJobConfig("lease-test-job"), nil)
	job.status = JobStatusRunning

	manager.jobs[job.Config.ID] = job
	manager.executionRoles[job.Config.ID] = meta.JobExecutionRoleStreaming
	manager.workerLeases[job.Config.ID] = job.SubmissionID()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.RunWorker(ctx) }()

	select {
	case <-store.claimStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("worker never entered the blocking claim")
	}

	deadline := time.Now().Add(time.Second)
	for store.renewals() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := store.renewals(); got < 3 {
		cancel()
		t.Fatalf("lease renewals while claim blocked = %d, want at least 3", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunWorker returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunWorker did not stop after cancellation")
	}
}

func TestWorkerRenewsTerminalJobUntilPipelineDrains(t *testing.T) {
	for _, status := range []JobStatus{JobStatusFailed, JobStatusStopped, JobStatusDone} {
		t.Run(string(status), func(t *testing.T) {
			store := &blockingClaimStore{}
			manager := NewJobManager(nil, WithJobStore(store), WithWorkerRole(WorkerRoleStreaming), WithWorkerID("drain-owner"))
			job := NewJob(newTestJobConfig("draining-job"), nil)
			job.status = status
			done := make(chan struct{})
			job.setRunDone(done)
			manager.jobs[job.Config.ID] = job
			manager.executionRoles[job.Config.ID] = meta.JobExecutionRoleStreaming
			manager.workerLeases[job.Config.ID] = job.SubmissionID()
			if err := manager.renewWorkerJobLeases(context.Background(), store); err != nil {
				t.Fatal(err)
			}
			if store.renewals() != 1 {
				t.Fatal("terminal job lost its lease before final drained status could be saved")
			}
			close(done)
			if err := manager.renewWorkerJobLeases(context.Background(), store); err != nil {
				t.Fatal(err)
			}
			if store.renewals() != 1 {
				t.Fatal("fully drained terminal job kept renewing its lease")
			}
		})
	}
}
