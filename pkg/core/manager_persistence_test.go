package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

func TestRestorePersistedJobsLoadsStoppedAndResumesRunning(t *testing.T) {
	store := newMemoryJobStore()
	store.jobs["job-running"] = meta.PersistedJob{
		ID:           "job-running",
		Name:         "running",
		Config:       newTestJobConfig("job-running"),
		DesiredState: meta.DesiredStateRunning,
		LastStatus:   string(JobStatusRunning),
		CreatedAt:    time.Now().Add(-2 * time.Minute),
		UpdatedAt:    time.Now().Add(-1 * time.Minute),
	}
	store.jobs["job-stopped"] = meta.PersistedJob{
		ID:           "job-stopped",
		Name:         "stopped",
		Config:       newTestJobConfig("job-stopped"),
		DesiredState: meta.DesiredStateStopped,
		LastStatus:   string(JobStatusStopped),
		CreatedAt:    time.Now().Add(-4 * time.Minute),
		UpdatedAt:    time.Now().Add(-3 * time.Minute),
	}

	reg, modes := newTestRegistry()
	manager := NewJobManager(
		reg,
		WithJobStore(store),
	)

	if err := manager.RestorePersistedJobs(context.Background()); err != nil {
		t.Fatalf("RestorePersistedJobs returned error: %v", err)
	}

	waitForCondition(t, "running job to resume", func() bool {
		job, err := manager.Get("job-running")
		return err == nil && job.GetStatus() == JobStatusRunning
	})

	if _, err := manager.Get("job-running"); err != nil {
		t.Fatalf("expected restored running job: %v", err)
	}
	jobStopped, err := manager.Get("job-stopped")
	if err != nil {
		t.Fatalf("expected restored stopped job: %v", err)
	}
	if got := jobStopped.GetStatus(); got != JobStatusStopped {
		t.Fatalf("restored stopped job status = %s, want %s", got, JobStatusStopped)
	}

	select {
	case mode := <-modes:
		if mode != config.JobModeResume {
			t.Fatalf("restored active job mode = %s, want %s", mode, config.JobModeResume)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resumed job mode")
	}

	select {
	case extra := <-modes:
		t.Fatalf("stopped job should not auto-start, got mode %s", extra)
	default:
	}

	if err := manager.Cancel("job-running"); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
}

func TestSubmitCancelResubmitAndDeletePersistLifecycle(t *testing.T) {
	store := newMemoryJobStore()
	reg, modes := newTestRegistry()
	manager := NewJobManager(
		reg,
		WithJobStore(store),
	)

	job, err := manager.Submit(newTestJobConfig("job-1"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	waitForCondition(t, "job to reach running", func() bool {
		return job.GetStatus() == JobStatusRunning
	})
	waitForCondition(t, "job to be persisted as running", func() bool {
		record, ok := store.Get("job-1")
		return ok && record.DesiredState == meta.DesiredStateRunning && record.LastStatus == string(JobStatusRunning)
	})

	if _, ok := store.Get("job-1"); !ok {
		t.Fatal("expected persisted job record")
	}
	select {
	case mode := <-modes:
		if mode != config.JobModeInitial {
			t.Fatalf("submit mode = %s, want %s", mode, config.JobModeInitial)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial submit mode")
	}

	if err := manager.Cancel("job-1"); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	waitForCondition(t, "job to be persisted as stopped", func() bool {
		record, ok := store.Get("job-1")
		return ok && record.DesiredState == meta.DesiredStateStopped && record.LastStatus == string(JobStatusStopped)
	})

	if _, err := manager.Resubmit("job-1"); err != nil {
		t.Fatalf("Resubmit returned error: %v", err)
	}
	waitForCondition(t, "job to be persisted as running after resubmit", func() bool {
		record, ok := store.Get("job-1")
		return ok && record.DesiredState == meta.DesiredStateRunning && record.LastStatus == string(JobStatusRunning)
	})

	select {
	case mode := <-modes:
		if mode != config.JobModeResume {
			t.Fatalf("resubmit mode = %s, want %s", mode, config.JobModeResume)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resume mode")
	}

	if err := manager.Delete("job-1"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	waitForCondition(t, "job record deletion", func() bool {
		_, ok := store.Get("job-1")
		return !ok
	})
}

func TestPauseDrainsSinkBeforePausedAndCanResume(t *testing.T) {
	store := newMemoryJobStore()
	drainStarted := make(chan struct{}, 1)
	allowDrain := make(chan struct{})
	reg, modes := newGracefulPauseTestRegistry(drainStarted, allowDrain)
	manager := NewJobManager(
		reg,
		WithJobStore(store),
	)

	job, err := manager.Submit(newTestJobConfig("job-graceful-pause"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	waitForCondition(t, "job to reach running", func() bool {
		return job.GetStatus() == JobStatusRunning
	})
	select {
	case mode := <-modes:
		if mode != config.JobModeInitial {
			t.Fatalf("submit mode = %s, want %s", mode, config.JobModeInitial)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial mode")
	}

	if err := manager.Pause(job.Config.ID); err != nil {
		t.Fatalf("Pause returned error: %v", err)
	}
	if got := job.GetStatus(); got != JobStatusPausing {
		t.Fatalf("job status immediately after pause = %s, want %s", got, JobStatusPausing)
	}

	select {
	case <-drainStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("sink did not start draining after source stopped")
	}
	if got := job.GetStatus(); got != JobStatusPausing {
		t.Fatalf("job status while sink is draining = %s, want %s", got, JobStatusPausing)
	}

	close(allowDrain)
	waitForCondition(t, "job to reach paused", func() bool {
		return job.GetStatus() == JobStatusPaused
	})
	waitForCondition(t, "paused job to persist stopped desired state", func() bool {
		record, ok := store.Get(job.Config.ID)
		return ok &&
			record.DesiredState == meta.DesiredStateStopped &&
			record.LastStatus == string(JobStatusPaused)
	})

	if _, err := manager.Resubmit(job.Config.ID); err != nil {
		t.Fatalf("Resubmit paused job returned error: %v", err)
	}
	waitForCondition(t, "paused job to resume", func() bool {
		return job.GetStatus() == JobStatusRunning
	})
	select {
	case mode := <-modes:
		if mode != config.JobModeResume {
			t.Fatalf("resume mode = %s, want %s", mode, config.JobModeResume)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resume mode")
	}

	_ = manager.Cancel(job.Config.ID)
}

func TestPauseRejectsNonRunningJob(t *testing.T) {
	manager := NewJobManager(connector.NewRegistry())
	job := manager.newManagedJob(newTestJobConfig("job-not-running"))
	job.setStatus(JobStatusStopped)
	manager.mu.Lock()
	manager.jobs[job.Config.ID] = job
	manager.mu.Unlock()

	if err := manager.Pause(job.Config.ID); !errors.Is(err, ErrJobPauseNotAllowed) {
		t.Fatalf("Pause error = %v, want %v", err, ErrJobPauseNotAllowed)
	}
}

func TestCancelReturnsBeforePipelineFinishesDraining(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePipeline := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releasePipeline()

	manager := NewJobManager(newStalledRegistry(release))
	job, err := manager.Submit(newTestJobConfig("job-slow-cancel"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	waitForCondition(t, "job to reach running", func() bool {
		return job.GetStatus() == JobStatusRunning
	})

	done := make(chan error, 1)
	go func() {
		done <- manager.Cancel("job-slow-cancel")
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Cancel returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Cancel waited for a draining pipeline")
	}
	if got := job.GetStatus(); got != JobStatusStopped {
		t.Fatalf("job status = %s, want %s", got, JobStatusStopped)
	}

	releasePipeline()
	if !job.waitRunDone(2 * time.Second) {
		t.Fatal("pipeline did not finish after release")
	}
}

func TestDeleteReturnsBeforePipelineFinishesDraining(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePipeline := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releasePipeline()

	store := newMemoryJobStore()
	manager := NewJobManager(newStalledRegistry(release), WithJobStore(store))
	job, err := manager.Submit(newTestJobConfig("job-slow-delete"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	waitForCondition(t, "job to reach running", func() bool {
		return job.GetStatus() == JobStatusRunning
	})

	done := make(chan error, 1)
	go func() {
		done <- manager.Delete("job-slow-delete")
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Delete returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Delete waited for a draining pipeline")
	}
	if _, err := manager.Get("job-slow-delete"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("deleted job remains visible: %v", err)
	}
	waitForCondition(t, "job record deletion", func() bool {
		_, ok := store.Get("job-slow-delete")
		return !ok
	})

	releasePipeline()
	if !job.waitRunDone(2 * time.Second) {
		t.Fatal("pipeline did not finish after release")
	}
}

func TestRestorePersistedFailedJobRetainsErrorHistory(t *testing.T) {
	failedAt := time.Now().Add(-time.Minute).UTC()
	store := newMemoryJobStore()
	store.jobs["job-failed"] = meta.PersistedJob{
		ID:           "job-failed",
		Name:         "failed",
		Config:       newTestJobConfig("job-failed"),
		DesiredState: meta.DesiredStateStopped,
		LastStatus:   string(JobStatusFailed),
		Errors: []meta.PersistedJobError{{
			Component: "sink",
			Message:   "iceberg commit failed",
			Time:      failedAt,
		}},
	}

	manager := NewJobManager(connector.NewRegistry(), WithJobStore(store))
	if err := manager.RestorePersistedJobs(context.Background()); err != nil {
		t.Fatalf("RestorePersistedJobs returned error: %v", err)
	}

	job, err := manager.Get("job-failed")
	if err != nil {
		t.Fatalf("expected restored failed job: %v", err)
	}
	lastErr := job.GetLastError()
	if lastErr == nil || lastErr.Component != "sink" || lastErr.Message != "iceberg commit failed" || !lastErr.Time.Equal(failedAt) {
		t.Fatalf("restored last error = %#v, want persisted sink error", lastErr)
	}

	info := manager.List()[0]
	if info.ErrorCount != 1 || info.LastError == nil || info.LastError.Message != "iceberg commit failed" {
		t.Fatalf("list error summary = count %d last %#v, want persisted failure", info.ErrorCount, info.LastError)
	}
}

func TestFailedStatusPersistsErrorHistory(t *testing.T) {
	store := newMemoryJobStore()
	manager := NewJobManager(connector.NewRegistry(), WithJobStore(store))
	job := manager.newManagedJob(newTestJobConfig("job-failed"))

	job.addError("sink", errors.New("catalog write rejected"))
	job.setStatus(JobStatusFailed)

	record, ok := store.Get("job-failed")
	if !ok {
		t.Fatal("expected failed job record to be saved")
	}
	if len(record.Errors) != 1 || record.Errors[0].Message != "catalog write rejected" {
		t.Fatalf("persisted errors = %#v, want sink failure", record.Errors)
	}
}

func TestNormalizeModeSupportsSnapshotOnly(t *testing.T) {
	if got := normalizeMode(config.JobModeSnapshotOnly); got != config.JobModeSnapshotOnly {
		t.Fatalf("normalizeMode(snapshot-only) = %s, want %s", got, config.JobModeSnapshotOnly)
	}
	if got := normalizeMode(config.JobMode("unknown")); got != config.JobModeInitial {
		t.Fatalf("normalizeMode(unknown) = %s, want %s", got, config.JobModeInitial)
	}
}

func TestNormalizeConfigAppliesDefaultMetaMySQLDSN(t *testing.T) {
	manager := NewJobManager(
		connector.NewRegistry(),
		WithDefaultMetaMySQLDSN("meta-dsn"),
	)

	cfg := manager.normalizeConfig(newTestJobConfig("job-default-meta"))
	if cfg == nil {
		t.Fatal("normalizeConfig returned nil")
	}
	if cfg.Meta.MySQLDSN != "meta-dsn" {
		t.Fatalf("expected default meta dsn, got %q", cfg.Meta.MySQLDSN)
	}
}

func TestSubmitQueuesSnapshotJobsBeyondConcurrencyLimit(t *testing.T) {
	reg, reporters := newSnapshotQueueTestRegistry()
	manager := NewJobManager(
		reg,
		WithMaxConcurrentSnapshotJobs(2),
	)

	job1, err := manager.Submit(newTestJobConfig("job-queue-1"))
	if err != nil {
		t.Fatalf("Submit job1 returned error: %v", err)
	}
	job2, err := manager.Submit(newTestJobConfig("job-queue-2"))
	if err != nil {
		t.Fatalf("Submit job2 returned error: %v", err)
	}
	job3, err := manager.Submit(newTestJobConfig("job-queue-3"))
	if err != nil {
		t.Fatalf("Submit job3 returned error: %v", err)
	}

	waitForCondition(t, "first two jobs to run and third to queue", func() bool {
		return job1.GetStatus() == JobStatusRunning &&
			job2.GetStatus() == JobStatusRunning &&
			job3.GetStatus() == JobStatusQueued
	})

	var firstReporter connector.ProgressReporter
	select {
	case firstReporter = <-reporters:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first reporter")
	}
	select {
	case <-reporters:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second reporter")
	}

	firstReporter(connector.ProgressInfo{
		Phase:   "snapshot_complete",
		Summary: "Snapshot complete",
	})

	waitForCondition(t, "queued job to start after snapshot slot release", func() bool {
		return job3.GetStatus() == JobStatusRunning
	})

	_ = manager.Cancel("job-queue-1")
	_ = manager.Cancel("job-queue-2")
	_ = manager.Cancel("job-queue-3")
}

func TestResumeBypassesSnapshotConcurrencyQueue(t *testing.T) {
	reg, modes := newTestRegistry()
	manager := NewJobManager(
		reg,
		WithMaxConcurrentSnapshotJobs(1),
	)

	blockingJob, err := manager.Submit(newTestJobConfig("job-blocking-snapshot"))
	if err != nil {
		t.Fatalf("Submit blocking job returned error: %v", err)
	}
	waitForCondition(t, "blocking snapshot job to run", func() bool {
		return blockingJob.GetStatus() == JobStatusRunning
	})
	select {
	case mode := <-modes:
		if mode != config.JobModeInitial {
			t.Fatalf("blocking job mode = %s, want %s", mode, config.JobModeInitial)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for blocking job mode")
	}

	resumeJob := manager.newManagedJob(newTestJobConfig("job-resume-now"))
	resumeJob.setStatus(JobStatusStopped)
	resumeJob.updateProgress(connector.ProgressInfo{
		Phase:           "streaming",
		Summary:         "CDC streaming",
		Detail:          "Listening from mysql-bin.000001:4 across 1 table(s)",
		CompletedTables: 1,
		TotalTables:     1,
	})
	manager.mu.Lock()
	manager.jobs[resumeJob.Config.ID] = resumeJob
	manager.mu.Unlock()

	if _, err := manager.Resubmit(resumeJob.Config.ID); err != nil {
		t.Fatalf("Resubmit returned error: %v", err)
	}
	waitForCondition(t, "resume job to start without queueing", func() bool {
		return resumeJob.GetStatus() == JobStatusRunning
	})
	if got := resumeJob.GetStatus(); got == JobStatusQueued {
		t.Fatalf("resume job status = %s, want direct start", got)
	}

	select {
	case mode := <-modes:
		if mode != config.JobModeResume {
			t.Fatalf("resume job mode = %s, want %s", mode, config.JobModeResume)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resume mode")
	}

	_ = manager.Cancel("job-blocking-snapshot")
	_ = manager.Cancel("job-resume-now")
}

func TestResumeWithoutCDCStateUsesSnapshotConcurrencyQueue(t *testing.T) {
	reg, modes := newTestRegistry()
	manager := NewJobManager(
		reg,
		WithMaxConcurrentSnapshotJobs(1),
	)

	blockingJob, err := manager.Submit(newTestJobConfig("job-blocking-snapshot-2"))
	if err != nil {
		t.Fatalf("Submit blocking job returned error: %v", err)
	}
	waitForCondition(t, "blocking snapshot job to run", func() bool {
		return blockingJob.GetStatus() == JobStatusRunning
	})
	select {
	case mode := <-modes:
		if mode != config.JobModeInitial {
			t.Fatalf("blocking job mode = %s, want %s", mode, config.JobModeInitial)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for blocking job mode")
	}

	resumeJob := manager.newManagedJob(newTestJobConfig("job-resume-queued"))
	resumeJob.setStatus(JobStatusStopped)
	manager.mu.Lock()
	manager.jobs[resumeJob.Config.ID] = resumeJob
	manager.mu.Unlock()

	if _, err := manager.Resubmit(resumeJob.Config.ID); err != nil {
		t.Fatalf("Resubmit returned error: %v", err)
	}
	waitForCondition(t, "resume job without CDC state to queue", func() bool {
		return resumeJob.GetStatus() == JobStatusQueued
	})

	select {
	case mode := <-modes:
		t.Fatalf("queued resume job should not start yet, got mode %s", mode)
	default:
	}

	_ = manager.Cancel("job-blocking-snapshot-2")
	_ = manager.Cancel("job-resume-queued")
}

func TestPreflightSnapshotOnlyCountResumeSkipsMatchedAndResetsMismatched(t *testing.T) {
	cfg := newTestJobConfig("job-count-resume")
	cfg.Mode = config.JobModeSnapshotOnly
	cfg.Metadata = map[string]string{"snapshot_only_count_resume": "true"}
	job := NewJob(cfg, connector.NewRegistry())

	src := &countResumeSource{
		tables: []connector.TableRef{
			{Schema: "app", Table: "matched"},
			{Schema: "app", Table: "partial"},
		},
		sourceCounts: map[string]int64{
			"app.matched": 10,
			"app.partial": 20,
		},
	}
	sink := &countResumeSink{
		targetCounts: map[string]int64{
			"target.matched": 10,
			"target.partial": 7,
		},
	}

	if err := job.preflight(context.Background(), src, sink, config.JobModeSnapshotOnly); err != nil {
		t.Fatalf("preflight returned error: %v", err)
	}

	if got := len(src.skipped); got != 1 {
		t.Fatalf("skipped tables = %d, want 1", got)
	}
	if got := tableRefKey(src.skipped[0]); got != "app.matched" {
		t.Fatalf("skipped table = %q, want app.matched", got)
	}
	if got := len(sink.resetTargets); got != 1 {
		t.Fatalf("reset targets = %d, want 1", got)
	}
	if got := sink.resetTargets[0]; got != "target.partial" {
		t.Fatalf("reset target = %q, want target.partial", got)
	}
}

func TestPreflightInitialCountResumeSkipsMatchedAndResetsMismatched(t *testing.T) {
	cfg := newTestJobConfig("job-initial-count-resume")
	cfg.Mode = config.JobModeInitial
	cfg.Metadata = map[string]string{"snapshot_only_count_resume": "true"}
	job := NewJob(cfg, connector.NewRegistry())

	src := &countResumeSource{
		tables: []connector.TableRef{
			{Schema: "app", Table: "matched"},
			{Schema: "app", Table: "partial"},
		},
		sourceCounts: map[string]int64{
			"app.matched": 10,
			"app.partial": 20,
		},
	}
	sink := &countResumeSink{
		targetCounts: map[string]int64{
			"target.matched": 10,
			"target.partial": 7,
		},
	}

	if err := job.preflight(context.Background(), src, sink, config.JobModeInitial); err != nil {
		t.Fatalf("preflight returned error: %v", err)
	}

	if got := len(src.skipped); got != 1 {
		t.Fatalf("skipped tables = %d, want 1", got)
	}
	if got := tableRefKey(src.skipped[0]); got != "app.matched" {
		t.Fatalf("skipped table = %q, want app.matched", got)
	}
	if got := len(sink.resetTargets); got != 1 {
		t.Fatalf("reset targets = %d, want 1", got)
	}
	if got := sink.resetTargets[0]; got != "target.partial" {
		t.Fatalf("reset target = %q, want target.partial", got)
	}
}

func TestPreflightSnapshotOnlyCountResumeAggregatesFanInTargets(t *testing.T) {
	cfg := newTestJobConfig("job-count-resume-fan-in")
	cfg.Mode = config.JobModeSnapshotOnly
	cfg.Metadata = map[string]string{"snapshot_only_count_resume": "true"}
	job := NewJob(cfg, connector.NewRegistry())

	src := &countResumeSource{
		tables: []connector.TableRef{
			{Schema: "app", Table: "tbl_reservasi_backup_202501"},
			{Schema: "app", Table: "tbl_reservasi_backup_202502"},
			{Schema: "app", Table: "tbl_member_backup_202501"},
			{Schema: "app", Table: "tbl_member_backup_202502"},
		},
		sourceCounts: map[string]int64{
			"app.tbl_reservasi_backup_202501": 10,
			"app.tbl_reservasi_backup_202502": 15,
			"app.tbl_member_backup_202501":    20,
			"app.tbl_member_backup_202502":    30,
		},
	}
	sink := &countResumeSink{
		targetCounts: map[string]int64{
			"target.tbl_reservasi": 25,
			"target.tbl_member":    7,
		},
		targetFor: map[string]string{
			"app.tbl_reservasi_backup_202501": "tbl_reservasi",
			"app.tbl_reservasi_backup_202502": "tbl_reservasi",
			"app.tbl_member_backup_202501":    "tbl_member",
			"app.tbl_member_backup_202502":    "tbl_member",
		},
	}

	if err := job.preflight(context.Background(), src, sink, config.JobModeSnapshotOnly); err != nil {
		t.Fatalf("preflight returned error: %v", err)
	}

	if got := len(src.skipped); got != 2 {
		t.Fatalf("skipped tables = %d, want 2 (%#v)", got, src.skipped)
	}
	for _, table := range src.skipped {
		if !strings.HasPrefix(table.Table, "tbl_reservasi_backup_") {
			t.Fatalf("skipped table = %s, want only tbl_reservasi backups", tableRefKey(table))
		}
	}
	if got := len(sink.resetTargets); got != 1 {
		t.Fatalf("reset targets = %d, want 1 (%#v)", got, sink.resetTargets)
	}
	if got := sink.resetTargets[0]; got != "target.tbl_member" {
		t.Fatalf("reset target = %q, want target.tbl_member", got)
	}
}

func TestPreflightSnapshotOnlySkipsTablesWithoutPrimaryKey(t *testing.T) {
	cfg := newTestJobConfig("job-skip-no-pk")
	cfg.Mode = config.JobModeSnapshotOnly
	job := NewJob(cfg, connector.NewRegistry())

	src := &countResumeSource{
		tables: []connector.TableRef{
			{Schema: "app", Table: "with_pk"},
			{Schema: "app", Table: "without_pk"},
		},
		schemas: map[string]*model.TableSchema{
			"app.with_pk": {
				SchemaName: "app",
				TableName:  "with_pk",
				Columns: []model.TableColumn{
					{Name: "id", DataType: "bigint", IsPK: true},
				},
			},
			"app.without_pk": {
				SchemaName: "app",
				TableName:  "without_pk",
				Columns: []model.TableColumn{
					{Name: "code", DataType: "varchar"},
				},
			},
		},
	}
	sink := &countResumeSink{skipWithoutPK: true}

	if err := job.preflight(context.Background(), src, sink, config.JobModeSnapshotOnly); err != nil {
		t.Fatalf("preflight returned error: %v", err)
	}

	if got := len(src.skipped); got != 1 {
		t.Fatalf("skipped tables = %d, want 1", got)
	}
	if got := tableRefKey(src.skipped[0]); got != "app.without_pk" {
		t.Fatalf("skipped table = %q, want app.without_pk", got)
	}
	if got := sink.ensured; len(got) != 1 || got[0] != "target.with_pk" {
		t.Fatalf("ensured targets = %#v, want [target.with_pk]", got)
	}
}

func TestPreflightDoesNotSkipTablesWithoutPrimaryKeyOutsideSnapshotOnly(t *testing.T) {
	cfg := newTestJobConfig("job-no-skip-initial")
	cfg.Mode = config.JobModeInitial
	job := NewJob(cfg, connector.NewRegistry())

	src := &countResumeSource{
		tables: []connector.TableRef{
			{Schema: "app", Table: "without_pk"},
		},
		schemas: map[string]*model.TableSchema{
			"app.without_pk": {
				SchemaName: "app",
				TableName:  "without_pk",
				Columns: []model.TableColumn{
					{Name: "code", DataType: "varchar"},
				},
			},
		},
	}
	sink := &countResumeSink{skipWithoutPK: true}

	if err := job.preflight(context.Background(), src, sink, config.JobModeInitial); err != nil {
		t.Fatalf("preflight returned error: %v", err)
	}

	if len(src.skipped) != 0 {
		t.Fatalf("skipped tables = %#v, want none", src.skipped)
	}
	if got := sink.ensured; len(got) != 1 || got[0] != "target.without_pk" {
		t.Fatalf("ensured targets = %#v, want [target.without_pk]", got)
	}
}

type memoryJobStore struct {
	mu   sync.Mutex
	jobs map[string]meta.PersistedJob
}

func newMemoryJobStore() *memoryJobStore {
	return &memoryJobStore{
		jobs: make(map[string]meta.PersistedJob),
	}
}

func (s *memoryJobStore) Init(context.Context) error {
	return nil
}

func (s *memoryJobStore) SaveJob(_ context.Context, job meta.PersistedJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := job
	record.Config = cloneTestJobConfig(job.Config)
	if record.CreatedAt.IsZero() {
		if existing, ok := s.jobs[job.ID]; ok && !existing.CreatedAt.IsZero() {
			record.CreatedAt = existing.CreatedAt
		} else {
			record.CreatedAt = time.Now()
		}
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = time.Now()
	}
	s.jobs[job.ID] = record
	return nil
}

func (s *memoryJobStore) LoadJobs(_ context.Context) ([]meta.PersistedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]meta.PersistedJob, 0, len(s.jobs))
	for _, job := range s.jobs {
		record := job
		record.Config = cloneTestJobConfig(job.Config)
		out = append(out, record)
	}
	return out, nil
}

func (s *memoryJobStore) DeleteJob(_ context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, jobID)
	return nil
}

func (s *memoryJobStore) Get(jobID string) (meta.PersistedJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.jobs[jobID]
	if !ok {
		return meta.PersistedJob{}, false
	}
	record.Config = cloneTestJobConfig(record.Config)
	return record, true
}

func newTestRegistry() (*connector.Registry, <-chan config.JobMode) {
	reg := connector.NewRegistry()
	modes := make(chan config.JobMode, 16)

	reg.RegisterSource("test_source", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		modes <- jctx.Mode
		return sourceFunc(func(ctx context.Context, out chan<- model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})
	reg.RegisterSink("test_sink", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		return sinkFunc(func(ctx context.Context, in <-chan model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})

	return reg, modes
}

func newGracefulPauseTestRegistry(drainStarted chan<- struct{}, allowDrain <-chan struct{}) (*connector.Registry, <-chan config.JobMode) {
	reg := connector.NewRegistry()
	modes := make(chan config.JobMode, 4)

	reg.RegisterSource("test_source", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		modes <- jctx.Mode
		return sourceFunc(func(ctx context.Context, out chan<- model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})
	reg.RegisterSink("test_sink", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		return sinkFunc(func(ctx context.Context, in <-chan model.Event) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case _, ok := <-in:
				if ok {
					return errors.New("unexpected test event")
				}
			}

			select {
			case drainStarted <- struct{}{}:
			default:
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-allowDrain:
				return nil
			}
		}), nil
	})

	return reg, modes
}

func newStalledRegistry(release <-chan struct{}) *connector.Registry {
	reg := connector.NewRegistry()
	reg.RegisterSource("test_source", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		return sourceFunc(func(ctx context.Context, out chan<- model.Event) error {
			<-release
			return nil
		}), nil
	})
	reg.RegisterSink("test_sink", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		return sinkFunc(func(ctx context.Context, in <-chan model.Event) error {
			<-release
			return nil
		}), nil
	})
	return reg
}

func newSnapshotQueueTestRegistry() (*connector.Registry, <-chan connector.ProgressReporter) {
	reg := connector.NewRegistry()
	reporters := make(chan connector.ProgressReporter, 8)
	reg.RegisterSource("test_source", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		reporters <- jctx.ReportProgress
		return sourceFunc(func(ctx context.Context, out chan<- model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})
	reg.RegisterSink("test_sink", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		return sinkFunc(func(ctx context.Context, in <-chan model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})
	return reg, reporters
}

func newTestJobConfig(id string) *config.JobConfig {
	cfg := &config.JobConfig{
		ID:   id,
		Name: id,
		Mode: config.JobModeInitial,
		Source: &config.ConnectorSpec{
			Type:   "test_source",
			Config: map[string]any{},
		},
		Sink: &config.ConnectorSpec{
			Type:   "test_sink",
			Config: map[string]any{},
		},
	}
	config.ApplyDefaults(cfg)
	return cfg
}

func cloneTestJobConfig(cfg *config.JobConfig) *config.JobConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	return &cloned
}

type sourceFunc func(ctx context.Context, out chan<- model.Event) error

func (f sourceFunc) Run(ctx context.Context, out chan<- model.Event) error {
	return f(ctx, out)
}

type sinkFunc func(ctx context.Context, in <-chan model.Event) error

func (f sinkFunc) Run(ctx context.Context, in <-chan model.Event) error {
	return f(ctx, in)
}

type countResumeSource struct {
	tables       []connector.TableRef
	schemas      map[string]*model.TableSchema
	sourceCounts map[string]int64
	skipped      []connector.TableRef
}

func (s *countResumeSource) Run(context.Context, chan<- model.Event) error {
	return nil
}

func (s *countResumeSource) Tables() []connector.TableRef {
	out := make([]connector.TableRef, len(s.tables))
	copy(out, s.tables)
	return out
}

func (s *countResumeSource) FetchSchema(_ context.Context, schema, table string) (*model.TableSchema, error) {
	if s.schemas != nil {
		if sourceSchema := s.schemas[strings.ToLower(schema+"."+table)]; sourceSchema != nil {
			return sourceSchema, nil
		}
	}
	return &model.TableSchema{
		SchemaName: schema,
		TableName:  table,
		Columns: []model.TableColumn{
			{Name: "id", DataType: "bigint", IsPK: true},
		},
	}, nil
}

func (s *countResumeSource) CountRows(_ context.Context, schema, table string) (int64, error) {
	return s.sourceCounts[strings.ToLower(schema+"."+table)], nil
}

func (s *countResumeSource) SkipSnapshotTables(tables []connector.TableRef) {
	s.skipped = append([]connector.TableRef(nil), tables...)
}

type countResumeSink struct {
	targetCounts  map[string]int64
	targetFor     map[string]string
	skipWithoutPK bool
	ensured       []string
	resetTargets  []string
}

func (s *countResumeSink) Run(context.Context, <-chan model.Event) error {
	return nil
}

func (s *countResumeSink) EnsureTable(_ context.Context, targetSchema, targetTable string, _ *model.TableSchema) error {
	s.ensured = append(s.ensured, strings.ToLower(targetSchema+"."+targetTable))
	return nil
}

func (s *countResumeSink) ResolveTarget(srcSchema string, srcTable string) (string, string) {
	if s.targetFor != nil {
		if targetTable := s.targetFor[strings.ToLower(srcSchema+"."+srcTable)]; targetTable != "" {
			return "target", targetTable
		}
	}
	return "target", srcTable
}

func (s *countResumeSink) CountTargetRows(_ context.Context, targetSchema, targetTable string) (int64, error) {
	return s.targetCounts[strings.ToLower(targetSchema+"."+targetTable)], nil
}

func (s *countResumeSink) ResetTargetTable(_ context.Context, targetSchema, targetTable string) error {
	s.resetTargets = append(s.resetTargets, strings.ToLower(targetSchema+"."+targetTable))
	return nil
}

func (s *countResumeSink) SkipSnapshotTableWithoutPrimaryKey(_, _ string, schema *model.TableSchema) bool {
	if !s.skipWithoutPK || schema == nil {
		return false
	}
	for _, col := range schema.Columns {
		if col.IsPK {
			return false
		}
	}
	return true
}

func tableRefKey(table connector.TableRef) string {
	return strings.ToLower(table.Schema + "." + table.Table)
}

func waitForCondition(t *testing.T, desc string, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}
