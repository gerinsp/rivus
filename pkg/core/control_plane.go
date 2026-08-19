package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

const WorkerRoleMaster WorkerRole = "master"

func isExecutionWorkerRole(role WorkerRole) bool {
	return role == WorkerRoleSnapshot || role == WorkerRoleStreaming
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

func (m *JobManager) cancelDurableJob(job *Job) error {
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
		return nil, ErrJobNotFound
	}
	m.mu.Lock()
	m.executionRoles[job.Config.ID] = role
	m.mu.Unlock()
	setObservedJobStatus(job, JobStatusQueued)
	job.updateProgress(connectorQueuedProgress(role))
	return job, nil
}

func connectorQueuedProgress(role meta.JobExecutionRole) connectorProgressInfo {
	return connectorProgressInfo{
		phase:   "queued",
		summary: "Waiting for " + strings.ToLower(string(role)) + " worker",
		detail:  "The job will resume from its durable checkpoint",
	}
}

// Small adapter keeps control_plane.go independent from connector internals in
// tests while still reusing Job.updateProgress through applyQueuedProgress.
type connectorProgressInfo struct {
	phase   string
	summary string
	detail  string
}

func (j *Job) applyQueuedProgress(info connectorProgressInfo) {
	j.mu.Lock()
	j.progress = &JobProgress{Phase: info.phase, Summary: info.summary, Detail: info.detail}
	j.Updated = time.Now()
	j.mu.Unlock()
}

func (m *JobManager) observeDurablePauseRequest(ctx context.Context, store meta.JobWorkerStore, job *Job) error {
	controlStore, ok := store.(meta.JobControlStore)
	if !ok || job == nil || job.Config == nil {
		return nil
	}
	state, err := controlStore.LoadJobControl(ctx, job.Config.ID)
	if err != nil {
		return err
	}
	if state == nil || !strings.EqualFold(state.LastStatus, string(JobStatusPausing)) {
		return nil
	}
	if job.GetStatus() == JobStatusRunning {
		if !job.RequestPause() {
			return fmt.Errorf("job %s could not enter graceful pause", job.Config.ID)
		}
	}
	return nil
}
