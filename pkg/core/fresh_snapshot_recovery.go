package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

// A proven Iceberg commit still needs its source replay receipt until the
// checkpoint covers it. Reconciliation alone does not authorize a reset.
func (j *Job) checkFreshSnapshotRecovery(ctx context.Context, store meta.OffsetStore, key string) error {
	sinkType, _ := j.pickSink()
	if sinkType != "iceberg_native" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	blocked := func(err error) error {
		return fmt.Errorf("fresh snapshot BLOCKED job_id=%s: %w; preserve commit intents and checkpoint; resume requires the saved source binlog history, otherwise a controlled target rebuild is required; see docs/iceberg-checkpoint-recovery.md", j.Config.ID, err)
	}
	checker, ok := store.(meta.JobStateCleanupChecker)
	if !ok {
		return blocked(fmt.Errorf("metadata store cannot verify retained Iceberg intents"))
	}
	if err := meta.CheckOwnership(ctx, store); err != nil {
		return blocked(err)
	}
	// The public ID also covers intents from earlier configurations/state keys,
	// matching the protection used by legacy checkpoint cleanup.
	if err := checker.CheckJobStateCleanup(ctx, j.Config.ID); err != nil {
		return blocked(err)
	}
	if strings.TrimSpace(key) != "" && key != j.Config.ID {
		if err := checker.CheckJobStateCleanup(ctx, key); err != nil {
			return blocked(err)
		}
	}
	return meta.CheckOwnership(ctx, store)
}

func (m *JobManager) rebuildPurgedIcebergJob(job *Job, admissionErr error) error {
	if !errors.Is(admissionErr, meta.ErrJobStateCleanupBlocked) {
		return admissionErr
	}
	blocked := func(err error) error {
		return fmt.Errorf("%w; configured snapshot rebuild unavailable: %w", admissionErr, err)
	}
	if job.runActive() {
		return blocked(ErrJobStillStopping)
	}
	if m.workerRole == WorkerRoleAll {
		return blocked(fmt.Errorf("archived rebuild requires durable split-worker ownership"))
	}
	if err := config.ValidateIcebergSnapshotRebuild(job.Config); err != nil {
		return blocked(err)
	}
	rebuilder, ok := m.jobStore.(meta.JobRebuildStore)
	if !ok {
		return blocked(fmt.Errorf("metadata registry does not support durable recovery archives"))
	}
	store, key, err := job.checkpointReader()
	if err != nil {
		return blocked(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.ensureJobStoreReady(ctx); err != nil {
		return blocked(err)
	}
	result, err := rebuilder.RequestJobRebuild(ctx, store, job.Config.ID, job.SubmissionID(), key)
	if err != nil {
		return blocked(err)
	}
	job.mu.Lock()
	job.submissionID = result.SubmissionID
	job.mu.Unlock()
	m.mu.Lock()
	m.executionRoles[job.Config.ID] = meta.JobExecutionRoleSnapshot
	m.mu.Unlock()
	setObservedJobStatus(job, JobStatusQueued)
	setObservedFreshSnapshotProgress(job)
	log.Printf("[job %s] purged binlog rebuild queued archive_id=%s submission_id=%s; previous checkpoint and confirmed intents archived; snapshot uses configured full target reset", job.Config.ID, result.ArchiveID, result.SubmissionID)
	return nil
}

func jobFreshSnapshotOnResubmit(job *Job) (bool, error) {
	if !jobCheckpointBinlogPurged(job) {
		return false, nil
	}
	sinkType, _ := job.pickSink()
	if sinkType == "iceberg_native" {
		store, key, err := job.checkpointReader()
		if err != nil {
			return false, err
		}
		if err := job.checkFreshSnapshotRecovery(context.Background(), store, key); err != nil {
			return false, fmt.Errorf("cannot resume job_id=%s: saved MySQL binlog has been purged; %w", job.Config.ID, err)
		}
	}
	return true, nil
}
