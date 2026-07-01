package core

import (
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

var (
	ErrJobNotFound           = errors.New("job not found")
	ErrJobResubmitNotAllowed = errors.New("job resubmit not allowed")
	ErrJobStillStopping      = errors.New("job pipeline is still stopping")
	ErrJobPauseNotAllowed    = errors.New("job pause not allowed")
)

const defaultMaxConcurrentSnapshotJobs = 2

type JobInfo struct {
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	Status     JobStatus    `json:"status"`
	Created    string       `json:"created"`
	Updated    string       `json:"updated"`
	MetaKey    string       `json:"meta_key"`
	SinkType   string       `json:"sink_type"`
	ErrorCount int          `json:"error_count"`
	LastError  *JobError    `json:"last_error,omitempty"`
	Progress   *JobProgress `json:"progress,omitempty"`
}

type JobManager struct {
	mu              sync.RWMutex
	jobs            map[string]*Job
	reg             *connector.Registry
	failureNotifier jobFailureNotifier

	jobStore          meta.JobStore
	defaultMetaMySQL  string
	jobStoreReady     bool
	jobStoreReadyLock sync.Mutex

	maxConcurrentSnapshotJobs int
	snapshotQueue             []string
	snapshotQueueModes        map[string]config.JobMode
	startingSnapshotJobs      map[string]struct{}
}

type JobManagerOption func(*JobManager)

func WithJobStore(store meta.JobStore) JobManagerOption {
	return func(m *JobManager) {
		m.jobStore = store
	}
}

func WithDefaultMetaMySQLDSN(dsn string) JobManagerOption {
	return func(m *JobManager) {
		m.defaultMetaMySQL = strings.TrimSpace(dsn)
	}
}

func WithMaxConcurrentSnapshotJobs(limit int) JobManagerOption {
	return func(m *JobManager) {
		m.maxConcurrentSnapshotJobs = limit
	}
}

func withJobFailureNotifier(notifier jobFailureNotifier) JobManagerOption {
	return func(m *JobManager) {
		m.failureNotifier = notifier
	}
}

func NewJobManager(reg *connector.Registry, opts ...JobManagerOption) *JobManager {
	m := &JobManager{
		jobs:                      make(map[string]*Job),
		reg:                       reg,
		failureNotifier:           newTelegramJobFailureNotifier(nil),
		maxConcurrentSnapshotJobs: snapshotJobLimitFromEnv(),
		snapshotQueueModes:        make(map[string]config.JobMode),
		startingSnapshotJobs:      make(map[string]struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

func (m *JobManager) Submit(cfg *config.JobConfig) (*Job, error) {
	cfg = m.normalizeConfig(cfg)
	if cfg == nil {
		return nil, errors.New("job config is nil")
	}

	job := NewJob(cfg, m.reg)
	m.mu.Lock()
	if _, exists := m.jobs[cfg.ID]; exists {
		m.mu.Unlock()
		return nil, errors.New("job id already exists")
	}
	m.jobs[cfg.ID] = job
	shouldQueue := m.shouldQueueSnapshotStartLocked(job, cfg.Mode)
	if shouldQueue {
		m.enqueueSnapshotJobLocked(cfg.ID, cfg.Mode)
	}
	m.mu.Unlock()
	m.attachStatusListener(job, false)

	if shouldQueue {
		m.attachStatusListener(job, true)
		job.setStatus(JobStatusQueued)
		return job, nil
	}

	if err := m.startJob(job, cfg.Mode, true); err != nil {
		log.Printf("[job-manager] job start failed job=%s: %v", cfg.ID, err)
		return nil, err
	}
	m.attachStatusListener(job, true)
	return job, nil
}

type SubmitResult struct {
	ID     string    `json:"id"`
	Name   string    `json:"name,omitempty"`
	Status JobStatus `json:"status,omitempty"`
	Action string    `json:"action"`
	Error  string    `json:"error,omitempty"`
}

func (m *JobManager) SubmitMany(configs []*config.JobConfig) []SubmitResult {
	results := make([]SubmitResult, 0, len(configs))
	seen := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		if cfg == nil {
			results = append(results, SubmitResult{Action: "failed", Error: "job config is nil"})
			continue
		}
		id := strings.TrimSpace(cfg.ID)
		name := strings.TrimSpace(cfg.Name)
		if id == "" {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "failed", Error: "job id is empty"})
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "skipped", Error: "duplicate job id in submitted file"})
			continue
		}
		seen[id] = struct{}{}
		if m.HasJob(id) {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "skipped", Error: "job id already exists"})
			continue
		}
		job, err := m.Submit(cfg)
		if err != nil {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "failed", Error: err.Error()})
			continue
		}
		results = append(results, SubmitResult{
			ID:     job.Config.ID,
			Name:   job.Config.Name,
			Status: job.GetStatus(),
			Action: "submitted",
		})
	}
	return results
}

func (m *JobManager) Cancel(id string) error {
	m.mu.RLock()
	job, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return ErrJobNotFound
	}
	if job.GetStatus() == JobStatusQueued {
		m.mu.Lock()
		m.removeSnapshotQueueLocked(id)
		m.mu.Unlock()
		job.setStatus(JobStatusStopped)
		return nil
	}
	job.StopAsync()
	return nil
}

func (m *JobManager) Pause(id string) error {
	m.mu.RLock()
	job, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return ErrJobNotFound
	}
	if !job.RequestPause() {
		return ErrJobPauseNotAllowed
	}
	return nil
}

func (m *JobManager) Resubmit(id string) (*Job, error) {
	m.mu.RLock()
	job, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrJobNotFound
	}

	switch job.GetStatus() {
	case JobStatusFailed, JobStatusStopped, JobStatusPaused:
		if job.runActive() {
			if !job.waitRunDone(500 * time.Millisecond) {
				return nil, ErrJobStillStopping
			}
		}
		log.Printf("[job %s] resubmit requested mode=resume previous_status=%s", id, job.GetStatus())
		if m.queueOrStart(job, config.JobModeResume, false) {
			return job, nil
		}
		if err := m.startJob(job, config.JobModeResume, false); err != nil {
			return nil, err
		}
		return job, nil
	default:
		return nil, ErrJobResubmitNotAllowed
	}
}

func (m *JobManager) Get(id string) (*Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

func (m *JobManager) HasJob(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.jobs[id]
	return ok
}

func (m *JobManager) List() []JobInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]JobInfo, 0, len(m.jobs))
	for _, j := range m.jobs {
		errors := j.GetErrors()
		var lastError *JobError
		if len(errors) > 0 {
			last := errors[len(errors)-1]
			lastError = &last
		}
		out = append(out, JobInfo{
			ID:         j.Config.ID,
			Name:       j.Config.Name,
			Status:     j.GetStatus(),
			Created:    j.Created.Format("2006-01-02 15:04:05"),
			Updated:    j.Updated.Format("2006-01-02 15:04:05"),
			MetaKey:    j.MetaKey(),
			SinkType:   sinkTypeFromConfig(j.Config),
			ErrorCount: len(errors),
			LastError:  lastError,
			Progress:   j.Progress(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Updated != out[j].Updated {
			return out[i].Updated > out[j].Updated
		}
		if out[i].Created != out[j].Created {
			return out[i].Created > out[j].Created
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func sinkTypeFromConfig(cfg *config.JobConfig) string {
	if cfg == nil {
		return ""
	}
	if cfg.Sink == nil {
		return ""
	}
	if strings.TrimSpace(cfg.Sink.Type) == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(cfg.Sink.Type))
}

func (m *JobManager) RestorePersistedJobs(ctx context.Context) error {
	if m.jobStore == nil {
		return nil
	}
	if err := m.ensureJobStoreReady(ctx); err != nil {
		return err
	}

	records, err := m.jobStore.LoadJobs(ctx)
	if err != nil {
		return err
	}

	for _, record := range records {
		cfg := m.normalizeConfig(record.Config)
		if cfg == nil {
			log.Printf("[job-manager] skip persisted job %s: empty config", record.ID)
			continue
		}
		if strings.TrimSpace(cfg.ID) == "" {
			cfg.ID = record.ID
		}
		if strings.TrimSpace(cfg.Name) == "" {
			cfg.Name = record.Name
		}
		if strings.TrimSpace(cfg.ID) == "" {
			log.Printf("[job-manager] skip persisted job with empty id")
			continue
		}

		job := m.newManagedJob(cfg)
		shouldResume := strings.EqualFold(string(record.DesiredState), string(meta.DesiredStateRunning))
		m.restoreJobSnapshot(job, record, shouldResume)

		m.mu.Lock()
		if _, exists := m.jobs[cfg.ID]; exists {
			m.mu.Unlock()
			log.Printf("[job-manager] skip duplicate persisted job id=%s", cfg.ID)
			continue
		}
		m.jobs[cfg.ID] = job
		m.mu.Unlock()

		if !shouldResume {
			continue
		}
		if m.queueOrStart(job, config.JobModeResume, false) {
			continue
		}
		if err := m.startJob(job, config.JobModeResume, false); err != nil {
			log.Printf("[job-manager] auto-resume failed job=%s: %v", cfg.ID, err)
		}
	}

	return nil
}

func (m *JobManager) Delete(id string) error {
	m.mu.Lock()
	job, ok := m.jobs[id]
	if !ok {
		m.mu.Unlock()
		return ErrJobNotFound
	}
	delete(m.jobs, id)
	m.removeSnapshotQueueLocked(id)
	delete(m.startingSnapshotJobs, id)
	m.mu.Unlock()

	// The API should not wait for a connector to drain or a metadata DB round trip.
	// Detach first so late shutdown transitions cannot restore a deleted job record.
	job.setStatusListener(nil)
	job.setProgressListener(nil)
	job.requestStop()
	go func() {
		if err := m.deletePersistedJob(context.Background(), id); err != nil {
			log.Printf("[job-manager] delete persisted job failed job=%s: %v", id, err)
		}
		if !job.waitRunDone(30 * time.Second) {
			log.Printf("[job %s] delete timeout waiting for pipeline shutdown; cleaning metadata anyway", id)
		}
		job.CleanupMeta()
	}()
	return nil
}

func (m *JobManager) normalizeConfig(cfg *config.JobConfig) *config.JobConfig {
	if cfg == nil {
		return nil
	}

	cloned := *cfg
	if strings.TrimSpace(cloned.Meta.MySQLDSN) == "" && m.defaultMetaMySQL != "" {
		cloned.Meta.MySQLDSN = m.defaultMetaMySQL
	}
	config.ApplyDefaults(&cloned)
	return &cloned
}

func (m *JobManager) newManagedJob(cfg *config.JobConfig) *Job {
	job := NewJob(cfg, m.reg)
	m.attachStatusListener(job, true)
	return job
}

func (m *JobManager) attachStatusListener(job *Job, notifyFailures bool) {
	job.setStatusListener(func(status JobStatus) {
		if err := m.saveJobRecord(context.Background(), job, desiredStateForStatus(status), status); err != nil {
			log.Printf("[job-manager] persist job state failed job=%s status=%s: %v", job.Config.ID, status, err)
		}
		if snapshotStatusReleasesSlot(status) {
			m.startQueuedSnapshotJobsAsync()
		}
		if !notifyFailures || status != JobStatusFailed || m.failureNotifier == nil {
			return
		}

		payload, ok := buildJobFailureNotification(job)
		if !ok {
			log.Printf("[job-manager] skipped failed notification job=%s channel=telegram reason=disabled_or_missing_configuration", job.Config.ID)
			return
		}

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := m.failureNotifier.NotifyJobFailed(ctx, payload); err != nil {
				log.Printf("[job-manager] failed notification delivery job=%s channel=telegram: %v", job.Config.ID, err)
				return
			}
			log.Printf("[job-manager] sent failed notification job=%s channel=telegram", job.Config.ID)
		}()
	})
	job.setProgressListener(func(progress *JobProgress) {
		if snapshotProgressReleasesSlot(progress) {
			m.startQueuedSnapshotJobsAsync()
		}
	})
}

func (m *JobManager) restoreJobSnapshot(job *Job, record meta.PersistedJob, resumeOnBoot bool) {
	now := time.Now()
	created := record.CreatedAt
	if created.IsZero() {
		created = now
	}
	updated := record.UpdatedAt
	if updated.IsZero() {
		updated = created
	}
	status := parsePersistedStatus(record.LastStatus)
	if resumeOnBoot {
		switch status {
		case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing:
			status = JobStatusStopped
		}
	}

	job.mu.Lock()
	job.Created = created
	job.Updated = updated
	job.status = status
	job.errors = make([]JobError, len(record.Errors))
	for i, persistedErr := range record.Errors {
		job.errors[i] = JobError{
			Component: persistedErr.Component,
			Message:   persistedErr.Message,
			Time:      persistedErr.Time,
		}
	}
	job.mu.Unlock()
}

func (m *JobManager) ensureJobStoreReady(ctx context.Context) error {
	if m.jobStore == nil {
		return nil
	}

	m.jobStoreReadyLock.Lock()
	defer m.jobStoreReadyLock.Unlock()

	if m.jobStoreReady {
		return nil
	}
	if err := m.jobStore.Init(ctx); err != nil {
		return err
	}
	m.jobStoreReady = true
	return nil
}

func (m *JobManager) saveJobRecord(ctx context.Context, job *Job, desired meta.DesiredState, status JobStatus) error {
	if m.jobStore == nil || job == nil || job.Config == nil {
		return nil
	}

	job.mu.RLock()
	cfg := job.Config
	name := ""
	if cfg != nil {
		name = cfg.Name
	}
	created := job.Created
	updated := job.Updated
	errorHistory := make([]meta.PersistedJobError, len(job.errors))
	for i, jobErr := range job.errors {
		errorHistory[i] = meta.PersistedJobError{
			Component: jobErr.Component,
			Message:   jobErr.Message,
			Time:      jobErr.Time,
		}
	}
	job.mu.RUnlock()

	if cfg == nil {
		return nil
	}

	saveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.ensureJobStoreReady(saveCtx); err != nil {
		return err
	}

	return m.jobStore.SaveJob(saveCtx, meta.PersistedJob{
		ID:           cfg.ID,
		Name:         name,
		Config:       m.normalizeConfig(cfg),
		DesiredState: desired,
		LastStatus:   string(status),
		Errors:       errorHistory,
		CreatedAt:    created,
		UpdatedAt:    updated,
	})
}

func (m *JobManager) deletePersistedJob(ctx context.Context, id string) error {
	if m.jobStore == nil {
		return nil
	}

	deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.ensureJobStoreReady(deleteCtx); err != nil {
		return err
	}
	return m.jobStore.DeleteJob(deleteCtx, id)
}

func desiredStateForStatus(status JobStatus) meta.DesiredState {
	switch status {
	case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing:
		return meta.DesiredStateRunning
	default:
		return meta.DesiredStateStopped
	}
}

func parsePersistedStatus(raw string) JobStatus {
	switch JobStatus(strings.ToUpper(strings.TrimSpace(raw))) {
	case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing, JobStatusPaused, JobStatusFailed, JobStatusStopped, JobStatusDone:
		return JobStatus(strings.ToUpper(strings.TrimSpace(raw)))
	default:
		return JobStatusCreated
	}
}

func snapshotJobLimitFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("RIVUS_MAX_CONCURRENT_SNAPSHOT_JOBS"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("RIVUS_MAX_CONCURRENT_SNAPSHOTS"))
	}
	if raw == "" {
		return defaultMaxConcurrentSnapshotJobs
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("[job-manager] invalid snapshot concurrency limit %q, using default %d", raw, defaultMaxConcurrentSnapshotJobs)
		return defaultMaxConcurrentSnapshotJobs
	}
	return limit
}

func (m *JobManager) startJob(job *Job, mode config.JobMode, removeOnStartFailure bool) error {
	if job == nil || job.Config == nil {
		return errors.New("job is nil")
	}
	if m.snapshotGateApplies(job.Config, mode) {
		m.mu.Lock()
		m.startingSnapshotJobs[job.Config.ID] = struct{}{}
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.startingSnapshotJobs, job.Config.ID)
			m.mu.Unlock()
			m.startQueuedSnapshotJobsAsync()
		}()
	}

	err := job.startWithMode(mode)
	if err == nil {
		status := job.GetStatus()
		if saveErr := m.saveJobRecord(context.Background(), job, desiredStateForStatus(status), status); saveErr != nil {
			job.requestStop()
			err = saveErr
		}
	}
	if err != nil && removeOnStartFailure {
		m.mu.Lock()
		delete(m.jobs, job.Config.ID)
		m.removeSnapshotQueueLocked(job.Config.ID)
		delete(m.startingSnapshotJobs, job.Config.ID)
		m.mu.Unlock()
		job.setStatusListener(nil)
		job.setProgressListener(nil)
		job.requestStop()
		if deleteErr := m.deletePersistedJob(context.Background(), job.Config.ID); deleteErr != nil {
			log.Printf("[job-manager] delete failed submit record job=%s: %v", job.Config.ID, deleteErr)
		}
	}
	return err
}

func (m *JobManager) queueOrStart(job *Job, mode config.JobMode, removeOnStartFailure bool) bool {
	bypassSnapshotGate := normalizeMode(mode) == config.JobModeResume && m.resumeCanBypassSnapshotGate(job)
	m.mu.Lock()
	shouldQueue := !bypassSnapshotGate && m.shouldQueueSnapshotStartLocked(job, mode)
	if shouldQueue {
		m.enqueueSnapshotJobLocked(job.Config.ID, mode)
	}
	m.mu.Unlock()
	if shouldQueue {
		job.setStatus(JobStatusQueued)
		return true
	}
	return false
}

func (m *JobManager) startQueuedSnapshotJobsAsync() {
	go m.startQueuedSnapshotJobs()
}

func (m *JobManager) startQueuedSnapshotJobs() {
	for {
		m.mu.Lock()
		if !m.hasSnapshotSlotLocked() || len(m.snapshotQueue) == 0 {
			m.mu.Unlock()
			return
		}
		id := m.snapshotQueue[0]
		m.snapshotQueue = m.snapshotQueue[1:]
		mode := m.snapshotQueueModes[id]
		delete(m.snapshotQueueModes, id)
		job := m.jobs[id]
		if job == nil || job.GetStatus() != JobStatusQueued {
			m.mu.Unlock()
			continue
		}
		m.startingSnapshotJobs[id] = struct{}{}
		m.mu.Unlock()

		log.Printf("[job-manager] starting queued snapshot job=%s mode=%s", id, mode)
		if err := job.startWithMode(mode); err != nil {
			log.Printf("[job-manager] queued job start failed job=%s: %v", id, err)
			job.setStatus(JobStatusFailed)
		}
		status := job.GetStatus()
		if err := m.saveJobRecord(context.Background(), job, desiredStateForStatus(status), status); err != nil {
			log.Printf("[job-manager] persist queued job start failed job=%s: %v", id, err)
		}
		m.mu.Lock()
		delete(m.startingSnapshotJobs, id)
		m.mu.Unlock()
	}
}

func (m *JobManager) shouldQueueSnapshotStartLocked(job *Job, mode config.JobMode) bool {
	if job == nil || job.Config == nil || !m.snapshotGateApplies(job.Config, mode) {
		return false
	}
	return !m.hasSnapshotSlotLocked()
}

func (m *JobManager) snapshotGateApplies(cfg *config.JobConfig, mode config.JobMode) bool {
	if cfg == nil || m.maxConcurrentSnapshotJobs == 0 {
		return false
	}
	switch normalizeMode(mode) {
	case config.JobModeInitial, config.JobModeSnapshotOnly:
		return true
	case config.JobModeResume:
		stored := normalizeMode(cfg.Mode)
		return stored == config.JobModeInitial || stored == config.JobModeSnapshotOnly
	default:
		return false
	}
}

func (m *JobManager) resumeCanBypassSnapshotGate(job *Job) bool {
	if job == nil || job.Config == nil {
		return false
	}
	storedMode := normalizeMode(job.Config.Mode)
	if storedMode != config.JobModeInitial && storedMode != config.JobModeSnapshotOnly {
		return true
	}
	if progressIndicatesCDC(job.Progress()) {
		return true
	}

	dsn := strings.TrimSpace(job.Config.Meta.MySQLDSN)
	if dsn == "" {
		return false
	}

	srcType, srcCfg := job.pickSource()
	sinkType, sinkCfg := job.pickSink()
	metaKey := buildMetaKey(job.Config.ID, string(storedMode), srcType, srcCfg, sinkType, sinkCfg)

	store, err := meta.NewMySQLOffsetStore(dsn)
	if err != nil {
		log.Printf("[job-manager] resume checkpoint inspection skipped job=%s: %v", job.Config.ID, err)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Init(ctx); err != nil {
		log.Printf("[job-manager] resume checkpoint inspection init skipped job=%s: %v", job.Config.ID, err)
		return false
	}

	st, err := store.GetSnapshotState(ctx, metaKey)
	if err != nil {
		log.Printf("[job-manager] resume snapshot state inspection skipped job=%s: %v", job.Config.ID, err)
		return false
	}
	if st != nil {
		return st.Done
	}

	off, err := store.GetOffset(ctx, metaKey)
	if err != nil {
		log.Printf("[job-manager] resume offset inspection skipped job=%s: %v", job.Config.ID, err)
		return false
	}
	return off != nil && strings.TrimSpace(off.BinlogFile) != "" && off.BinlogPos > 0
}

func (m *JobManager) hasSnapshotSlotLocked() bool {
	if m.maxConcurrentSnapshotJobs <= 0 {
		return true
	}
	return m.activeSnapshotJobsLocked() < m.maxConcurrentSnapshotJobs
}

func (m *JobManager) activeSnapshotJobsLocked() int {
	active := len(m.startingSnapshotJobs)
	for _, job := range m.jobs {
		if job == nil || job.Config == nil || !m.snapshotGateApplies(job.Config, job.Config.Mode) {
			continue
		}
		status := job.GetStatus()
		if status != JobStatusPending && status != JobStatusRunning && status != JobStatusPausing {
			continue
		}
		if snapshotProgressReleasesSlot(job.Progress()) {
			continue
		}
		active++
	}
	return active
}

func (m *JobManager) enqueueSnapshotJobLocked(id string, mode config.JobMode) {
	if _, exists := m.snapshotQueueModes[id]; exists {
		return
	}
	m.snapshotQueue = append(m.snapshotQueue, id)
	m.snapshotQueueModes[id] = mode
	log.Printf("[job-manager] queued snapshot job=%s mode=%s queue_len=%d active=%d limit=%d",
		id, mode, len(m.snapshotQueue), m.activeSnapshotJobsLocked(), m.maxConcurrentSnapshotJobs)
}

func (m *JobManager) removeSnapshotQueueLocked(id string) {
	delete(m.snapshotQueueModes, id)
	if len(m.snapshotQueue) == 0 {
		return
	}
	out := m.snapshotQueue[:0]
	for _, queuedID := range m.snapshotQueue {
		if queuedID != id {
			out = append(out, queuedID)
		}
	}
	m.snapshotQueue = out
}

func snapshotStatusReleasesSlot(status JobStatus) bool {
	switch status {
	case JobStatusPaused, JobStatusFailed, JobStatusStopped, JobStatusDone:
		return true
	default:
		return false
	}
}

func snapshotProgressReleasesSlot(progress *JobProgress) bool {
	if progress == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(progress.Phase)) {
	case "snapshot_complete", "streaming", "done", "failed", "stopped":
		return true
	default:
		return false
	}
}

func progressIndicatesCDC(progress *JobProgress) bool {
	if progress == nil {
		return false
	}
	phase := strings.ToLower(strings.TrimSpace(progress.Phase))
	if phase == "streaming" || phase == "snapshot_complete" {
		return true
	}
	detail := strings.ToLower(strings.TrimSpace(progress.Detail))
	return strings.Contains(detail, "cdc streaming") || strings.Contains(detail, "listening from")
}
