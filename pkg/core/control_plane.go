package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

// WorkerRoleMaster is a pure control-plane role. It may load and mutate the
// durable job registry, but it never claims or executes snapshot/streaming jobs.
const WorkerRoleMaster WorkerRole = "master"

// WithControlPlaneRole configures a JobManager for the master/API process.
// Keep this separate from WithWorkerRole so legacy RIVUS_WORKER_ROLE parsing
// remains backward compatible until deployments move to the explicit command.
func WithControlPlaneRole() JobManagerOption {
	return func(m *JobManager) {
		m.workerRole = WorkerRoleMaster
	}
}

func (m *JobManager) ownsWorkerLease(jobID string) bool {
	m.mu.RLock()
	_, owned := m.workerLeases[jobID]
	m.mu.RUnlock()
	return owned
}

// usesDurableControl is true when this manager is only observing a job that
// executes in another process. That includes the pure master role and the
// remote half of the older split API/worker deployment.
func (m *JobManager) usesDurableControl(jobID string) bool {
	return m.workerRole != WorkerRoleAll && !m.ownsWorkerLease(jobID)
}

func (m *JobManager) durableControlStore(ctx context.Context) (meta.JobControlStore, error) {
	if err := m.ensureJobStoreReady(ctx); err != nil {
		return nil, err
	}
	store, ok := m.jobStore.(meta.JobControlStore)
	if !ok {
		return nil, errors.New("remote job lifecycle control requires a JobControlStore")
	}
	return store, nil
}

// beginDurableLifecycle mirrors the existing JobManager lifecycle fence for
// operations that are executed through the durable control store. Holding the
// read lock prevents Shutdown from crossing a control mutation mid-request.
func (m *JobManager) beginDurableLifecycle(job *Job) (func(), error) {
	m.lifecycleMu.RLock()
	m.mu.RLock()
	shuttingDown := m.shuttingDown
	managed := job != nil && job.Config != nil && m.jobs[job.Config.ID] == job
	m.mu.RUnlock()
	if shuttingDown {
		m.lifecycleMu.RUnlock()
		return nil, ErrJobManagerShuttingDown
	}
	if !managed {
		m.lifecycleMu.RUnlock()
		return nil, ErrJobNotFound
	}
	return m.lifecycleMu.RUnlock, nil
}

// RequestCancel is the lifecycle entry point for API/control-plane callers.
// Local monolith/worker jobs preserve the existing direct cancellation path;
// remote jobs are fenced through the durable registry instead.
func (m *JobManager) RequestCancel(id string) error {
	m.mu.RLock()
	job := m.jobs[id]
	m.mu.RUnlock()
	if job == nil {
		return ErrJobNotFound
	}
	if !m.usesDurableControl(id) {
		return m.Cancel(id)
	}
	return m.cancelDurableJob(job)
}

// RequestPause preserves graceful source-stop/sink-drain semantics even when
// the API and the executing worker are different processes.
func (m *JobManager) RequestPause(id string) error {
	m.mu.RLock()
	job := m.jobs[id]
	m.mu.RUnlock()
	if job == nil {
		return ErrJobNotFound
	}
	if !m.usesDurableControl(id) {
		return m.Pause(id)
	}
	return m.pauseDurableJob(job)
}

// RequestResubmit makes a remote job claimable again while preserving the
// durable execution role chosen by snapshot handoff.
func (m *JobManager) RequestResubmit(id string) (*Job, error) {
	m.mu.RLock()
	job := m.jobs[id]
	m.mu.RUnlock()
	if job == nil {
		return nil, ErrJobNotFound
	}
	if !m.usesDurableControl(id) {
		return m.Resubmit(id)
	}
	return m.resubmitDurableJob(job)
}

// setObservedJobStatus updates the control-plane's local projection without
// invoking the worker status listener. The durable row was already changed by
// JobControlStore and must remain the source of truth.
func setObservedJobStatus(job *Job, status JobStatus) {
	if job == nil {
		return
	}
	job.mu.Lock()
	_, _, _, _ = job.setStatusLocked(status)
	job.mu.Unlock()
}

func setObservedQueuedProgress(job *Job, role meta.JobExecutionRole) {
	if job == nil {
		return
	}
	job.mu.Lock()
	job.progress = &JobProgress{
		Phase:   "queued",
		Summary: "Waiting for " + strings.ToLower(string(role)) + " worker",
		Detail:  "The job will resume from its durable checkpoint",
	}
	job.Updated = time.Now()
	job.mu.Unlock()
}

func (m *JobManager) cancelDurableJob(job *Job) error {
	release, err := m.beginDurableLifecycle(job)
	if err != nil {
		return err
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := m.durableControlStore(ctx)
	if err != nil {
		return err
	}
	updated, err := store.RequestJobStop(ctx, job.Config.ID)
	if err != nil {
		return err
	}
	if !updated {
		return ErrJobNotFound
	}
	setObservedJobStatus(job, JobStatusStopped)
	return nil
}

func (m *JobManager) pauseDurableJob(job *Job) error {
	release, err := m.beginDurableLifecycle(job)
	if err != nil {
		return err
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := m.durableControlStore(ctx)
	if err != nil {
		return err
	}
	updated, err := store.RequestJobPause(ctx, job.Config.ID)
	if err != nil {
		return err
	}
	if !updated {
		state, loadErr := store.LoadJobControl(ctx, job.Config.ID)
		if loadErr != nil {
			return loadErr
		}
		if state == nil {
			return ErrJobNotFound
		}
		return ErrJobPauseNotAllowed
	}
	setObservedJobStatus(job, JobStatusPausing)
	return nil
}

func (m *JobManager) resubmitDurableJob(job *Job) (*Job, error) {
	release, err := m.beginDurableLifecycle(job)
	if err != nil {
		return nil, err
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := m.durableControlStore(ctx)
	if err != nil {
		return nil, err
	}
	state, err := store.LoadJobControl(ctx, job.Config.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, ErrJobNotFound
	}
	status := parsePersistedStatus(state.LastStatus)
	if status != JobStatusFailed && status != JobStatusStopped && status != JobStatusPaused {
		return nil, ErrJobResubmitNotAllowed
	}
	role := state.ExecutionRole
	if role != meta.JobExecutionRoleSnapshot && role != meta.JobExecutionRoleStreaming {
		role = executionRoleForConfig(job.Config)
	}
	updated, err := store.RequestJobResume(ctx, job.Config.ID, role)
	if err != nil {
		return nil, err
	}
	if !updated {
		// The row existed when we read it, so a failed conditional update means
		// its lifecycle changed concurrently. Do not clear a new worker lease.
		return nil, ErrJobResubmitNotAllowed
	}
	m.mu.Lock()
	m.executionRoles[job.Config.ID] = role
	m.mu.Unlock()
	setObservedJobStatus(job, JobStatusQueued)
	setObservedQueuedProgress(job, role)
	return job, nil
}

// RunControlObserver applies durable lifecycle requests to jobs currently
// leased by this worker. One query finds all PAUSING jobs owned by this worker;
// transient metadata errors are logged and retried instead of killing the data
// worker process.
func (m *JobManager) RunControlObserver(ctx context.Context) error {
	if m.workerRole != WorkerRoleSnapshot && m.workerRole != WorkerRoleStreaming {
		return errors.New("control observer requires snapshot or streaming worker role")
	}
	store, ok := m.jobStore.(meta.JobControlStore)
	if !ok {
		return errors.New("control observer requires a JobControlStore")
	}
	if err := m.ensureJobStoreReady(ctx); err != nil {
		return err
	}

	poll := m.workerPollInterval
	if poll <= 0 {
		poll = 2 * time.Second
	}
	check := func() {
		if err := m.applyDurablePauseRequests(ctx, store); err != nil && ctx.Err() == nil {
			log.Printf("[job-manager] control observer failed role=%s owner=%s: %v", m.workerRole, m.workerID, err)
		}
	}

	check()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			check()
		}
	}
}

func (m *JobManager) applyDurablePauseRequests(ctx context.Context, store meta.JobControlStore) error {
	ids, err := store.LoadJobPauseRequests(ctx, m.workerID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		m.mu.RLock()
		job := m.jobs[id]
		_, leased := m.workerLeases[id]
		m.mu.RUnlock()
		if job == nil || !leased || job.GetStatus() != JobStatusRunning {
			continue
		}
		if !job.RequestPause() {
			return fmt.Errorf("job %s could not enter graceful pause", id)
		}
	}
	return nil
}
