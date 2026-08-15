package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	defaultMaintenanceWorkerPoll    = 30 * time.Second
	defaultMaintenanceLeaseDuration = 15 * time.Minute
	defaultMaintenanceRetryLimit    = 5
	defaultMaintenanceRetryBackoff  = time.Minute
	defaultMaintenanceTaskPageSize  = 1
	defaultMaintenanceDuePageSize   = 100
	defaultCompactionCheckInterval  = 7 * 24 * time.Hour
)

type MaintenanceWorkerOptions struct {
	Queue         bool
	PollInterval  time.Duration
	LeaseDuration time.Duration
	TaskPageSize  int
	DuePageSize   int
	WorkerID      string
}

type maintenanceWorkerJob struct {
	Job      meta.PersistedJob
	Settings nativeMaintenanceSettings
}

func RunMaintenanceWorker(ctx context.Context, dsn string, opts MaintenanceWorkerOptions) error {
	if strings.TrimSpace(dsn) == "" {
		return fmt.Errorf("RIVUS_META_MYSQL_DSN is required for maintenance-worker")
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = durationEnv("RIVUS_MAINTENANCE_POLL_INTERVAL_SECONDS", defaultMaintenanceWorkerPoll)
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = durationEnv("RIVUS_MAINTENANCE_LEASE_SECONDS", defaultMaintenanceLeaseDuration)
	}
	if opts.TaskPageSize != 1 {
		opts.TaskPageSize = intEnv("RIVUS_MAINTENANCE_TASK_PAGE_SIZE", defaultMaintenanceTaskPageSize)
		if opts.TaskPageSize != 1 {
			opts.TaskPageSize = 1
		}
	}
	if opts.DuePageSize <= 0 || opts.DuePageSize > 500 {
		opts.DuePageSize = intEnv("RIVUS_MAINTENANCE_DUE_PAGE_SIZE", defaultMaintenanceDuePageSize)
	}
	if strings.TrimSpace(opts.WorkerID) == "" {
		opts.WorkerID = maintenanceWorkerID()
	}

	if raw := strings.TrimSpace(os.Getenv("RIVUS_MAINTENANCE_GOMAXPROCS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			runtime.GOMAXPROCS(n)
		}
	} else {
		runtime.GOMAXPROCS(1)
	}
	if strings.TrimSpace(os.Getenv("GOMEMLIMIT")) == "" {
		debug.SetMemoryLimit(256 * 1024 * 1024)
	}

	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Init(ctx); err != nil {
		return fmt.Errorf("initialize maintenance store: %w", err)
	}

	jobStore, err := meta.NewMySQLJobStore(dsn)
	if err != nil {
		return err
	}
	if err := jobStore.Init(ctx); err != nil {
		return fmt.Errorf("initialize job store: %w", err)
	}

	log.Printf("[maintenance-worker %s] started queue=%t poll=%s lease=%s task_page=%d due_page=%d",
		opts.WorkerID, opts.Queue, opts.PollInterval, opts.LeaseDuration, opts.TaskPageSize, opts.DuePageSize)

	var jobs map[string]maintenanceWorkerJob
	var lastStateSync time.Time
	for {
		now := time.Now().UTC()
		if jobs == nil || now.Sub(lastStateSync) >= 10*time.Minute {
			jobs, err = syncMaintenanceStates(ctx, store, jobStore, now)
			if err != nil {
				return err
			}
			lastStateSync = now
		}
		if err := enqueueDueMaintenance(ctx, store, jobs, now, opts.DuePageSize); err != nil {
			return err
		}
		processed, err := processMaintenancePage(ctx, store, jobs, opts, now)
		if err != nil {
			return err
		}
		if !opts.Queue {
			log.Printf("[maintenance-worker %s] one-shot complete processed=%d", opts.WorkerID, processed)
			return nil
		}
		select {
		case <-ctx.Done():
			log.Printf("[maintenance-worker %s] shutdown requested", opts.WorkerID)
			return nil
		case <-time.After(opts.PollInterval):
		}
	}
}

func syncMaintenanceStates(ctx context.Context, store *meta.IcebergMaintenanceStore, jobStore meta.JobStore, now time.Time) (map[string]maintenanceWorkerJob, error) {
	persisted, err := jobStore.LoadJobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("load persisted jobs for maintenance: %w", err)
	}
	jobs := make(map[string]maintenanceWorkerJob)
	for _, job := range persisted {
		if job.Config == nil {
			continue
		}
		sinkType, sinkCfg := jobSinkSpec(job.Config)
		if !strings.EqualFold(sinkType, "iceberg_native") || !nativeMaintenanceEnabledFromRaw(sinkCfg) {
			continue
		}
		settings, err := nativeMaintenanceSettingsFromRaw(sinkCfg)
		if err != nil {
			log.Printf("[maintenance-worker] skip job=%s invalid native config: %v", job.ID, err)
			continue
		}
		iceCfg, err := decodeIcebergConfig(sinkCfg)
		if err != nil {
			log.Printf("[maintenance-worker] skip job=%s decode Iceberg config: %v", job.ID, err)
			continue
		}
		lightSink := &Sink{cfg: iceCfg}
		targets, err := orphanCleanupTargets(job.Config, lightSink, nil)
		if err != nil {
			log.Printf("[maintenance-worker] skip job=%s resolve targets: %v", job.ID, err)
			continue
		}
		jobID := firstNonEmpty(strings.TrimSpace(job.ID), strings.TrimSpace(job.Config.ID))
		if jobID == "" {
			continue
		}
		jobs[jobID] = maintenanceWorkerJob{Job: job, Settings: settings}

		snapshotComplete := maintenanceSnapshotComplete(ctx, store, job)
		catalogName := maintenanceCatalogName(iceCfg)
		for _, target := range targets {
			tableIdentity := canonicalMaintenanceTableKey(catalogName, target.Namespace, target.Table)
			compactionDue := now.Add(deterministicJitter(tableIdentity+"|compact", settings.IdleCompactionInterval))
			expireDue := now.Add(deterministicJitter(tableIdentity+"|expire", settings.ExpireInterval))
			// A table without a Rivus write signal is inactive. Do not spend an
			// object-storage scan on it every month; seed its first orphan check
			// in the longer inactive interval instead.
			orphanDue := now.Add(deterministicJitter(tableIdentity+"|orphan", settings.OrphanInactiveInterval))
			if err := store.UpsertState(ctx, meta.IcebergMaintenanceState{
				TableKey:         tableIdentity,
				Catalog:          catalogName,
				Namespace:        target.Namespace,
				Table:            target.Table,
				OwnerType:        "job",
				OwnerJobID:       jobID,
				SnapshotComplete: snapshotComplete,
			}, compactionDue, expireDue, orphanDue); err != nil {
				return nil, fmt.Errorf("upsert maintenance state %s: %w", tableIdentity, err)
			}
		}
	}
	return jobs, nil
}

func maintenanceSnapshotComplete(ctx context.Context, store *meta.IcebergMaintenanceStore, job meta.PersistedJob) bool {
	if job.Config == nil {
		return false
	}
	mode := normalizeMaintenanceMode(job.Config.Mode)
	if mode == config.JobModeLatest || mode == config.JobModeLatestOffset {
		return true
	}
	sourceType, sourceCfg := jobSourceSpec(job.Config)
	sinkType, sinkCfg := jobSinkSpec(job.Config)
	metaKey := maintenanceMetaKey(job.Config.ID, string(mode), sourceType, sourceCfg, sinkType, sinkCfg)
	done, found, err := store.SnapshotDone(ctx, metaKey)
	if err != nil {
		log.Printf("[maintenance-worker] snapshot barrier lookup job=%s error=%v", job.ID, err)
		return false
	}
	if found {
		return done
	}
	return mode == config.JobModeSnapshotOnly && strings.EqualFold(strings.TrimSpace(job.LastStatus), "DONE")
}

func enqueueDueMaintenance(ctx context.Context, store *meta.IcebergMaintenanceStore, jobs map[string]maintenanceWorkerJob, now time.Time, pageSize int) error {
	operations := []struct {
		name     string
		priority int
	}{
		{name: "compact", priority: 10},
		{name: "expire_snapshots", priority: 30},
		{name: "remove_orphan_files", priority: 40},
	}
	for _, operation := range operations {
		states, err := store.DueStates(ctx, operation.name, now, pageSize)
		if err != nil {
			return fmt.Errorf("query due %s states: %w", operation.name, err)
		}
		for _, state := range states {
			job, ok := jobs[state.OwnerJobID]
			if !ok {
				continue
			}
			due := stateDueTime(state, operation.name)
			if due == nil {
				continue
			}
			if operation.name == "remove_orphan_files" && !orphanCleanupActive(state, now, job.Settings.OrphanInterval) {
				next := nextMaintenanceSchedule(state, operation.name, now, job.Settings)
				if err := store.AdvanceSchedule(ctx, state.TableKey, operation.name, next); err != nil {
					return fmt.Errorf("defer inactive orphan cleanup for %s: %w", state.TableKey, err)
				}
				continue
			}
			window := due.UTC().Format(time.RFC3339Nano)
			_, err := store.EnqueueTask(ctx, state, operation.name, operation.priority, window, now, map[string]any{
				"scheduled_at": due.UTC().Format(time.RFC3339Nano),
				"native":       true,
			})
			if err != nil {
				return fmt.Errorf("enqueue %s for %s: %w", operation.name, state.TableKey, err)
			}
			next := nextMaintenanceSchedule(state, operation.name, now, job.Settings)
			if err := store.AdvanceSchedule(ctx, state.TableKey, operation.name, next); err != nil {
				return fmt.Errorf("advance %s schedule for %s: %w", operation.name, state.TableKey, err)
			}
		}
	}
	return nil
}

func processMaintenancePage(ctx context.Context, store *meta.IcebergMaintenanceStore, jobs map[string]maintenanceWorkerJob, opts MaintenanceWorkerOptions, now time.Time) (int, error) {
	tasks, err := store.ClaimTasks(ctx, opts.WorkerID, now, opts.LeaseDuration, opts.TaskPageSize)
	if err != nil {
		return 0, fmt.Errorf("claim maintenance tasks: %w", err)
	}
	if len(tasks) == 0 {
		return 0, nil
	}
	runID, err := store.CreateRun(ctx, opts.WorkerID, len(tasks), now)
	if err != nil {
		return 0, fmt.Errorf("create maintenance run: %w", err)
	}

	successes, skipped, failures := 0, 0, 0
	for _, task := range tasks {
		if ctx.Err() != nil {
			break
		}
		state, err := store.GetState(ctx, task.TableKey)
		if err != nil {
			return len(tasks), err
		}
		if state == nil || !state.SnapshotComplete {
			failures++
			_ = store.FinishTask(ctx, task.ID, opts.WorkerID, meta.MaintenanceTaskRetry, "snapshot barrier is not complete", timePtr(time.Now().Add(time.Minute)))
			continue
		}
		job, ok := jobs[task.OwnerJobID]
		if !ok || job.Job.Config == nil {
			failures++
			_ = store.FinishTask(ctx, task.ID, opts.WorkerID, meta.MaintenanceTaskFailed, "owner job configuration is unavailable", nil)
			continue
		}

		leaseCtx, leaseCancel := context.WithCancel(ctx)
		var leaseWG sync.WaitGroup
		leaseWG.Add(1)
		go func(taskID int64) {
			defer leaseWG.Done()
			renewEvery := opts.LeaseDuration / 3
			if renewEvery < 10*time.Second {
				renewEvery = 10 * time.Second
			}
			ticker := time.NewTicker(renewEvery)
			defer ticker.Stop()
			for {
				select {
				case <-leaseCtx.Done():
					return
				case <-ticker.C:
					if err := store.RenewLease(leaseCtx, taskID, opts.WorkerID, time.Now().Add(opts.LeaseDuration)); err != nil {
						log.Printf("[maintenance-worker %s] lease renewal task=%d error=%v", opts.WorkerID, taskID, err)
						return
					}
				}
			}
		}(task.ID)

		outcome := executeNativeMaintenanceTask(ctx, task.OwnerJobID, job.Job.Config, *state, task, job.Settings)
		leaseCancel()
		leaseWG.Wait()
		outcome.Result.RunID = runID
		if err := store.InsertResult(ctx, outcome.Result); err != nil {
			return len(tasks), fmt.Errorf("store maintenance result task=%d: %w", task.ID, err)
		}

		switch outcome.Result.Status {
		case "succeeded":
			successes++
			_ = store.RecordStateSuccess(ctx, state.TableKey, task.Operation, time.Now().UTC(), task.Operation == "compact")
			if err := store.FinishTask(ctx, task.ID, opts.WorkerID, meta.MaintenanceTaskSucceeded, "", nil); err != nil {
				return len(tasks), err
			}
		case "skipped":
			skipped++
			_ = store.RecordStateSuccess(ctx, state.TableKey, task.Operation, time.Now().UTC(), false)
			if err := store.FinishTask(ctx, task.ID, opts.WorkerID, meta.MaintenanceTaskSkipped, "", nil); err != nil {
				return len(tasks), err
			}
		default:
			failures++
			_ = store.RecordStateError(ctx, state.TableKey, outcome.Result.Error)
			retryLimit := retryLimitFromRaw(job.Job.Config)
			if retryLimit <= 0 {
				retryLimit = defaultMaintenanceRetryLimit
			}
			if outcome.Retryable && task.AttemptCount < retryLimit {
				retryAt := time.Now().Add(maintenanceRetryBackoff(task.AttemptCount, retryBackoffFromRaw(job.Job.Config)))
				if err := store.FinishTask(ctx, task.ID, opts.WorkerID, meta.MaintenanceTaskRetry, outcome.Result.Error, &retryAt); err != nil {
					return len(tasks), err
				}
			} else if err := store.FinishTask(ctx, task.ID, opts.WorkerID, meta.MaintenanceTaskFailed, outcome.Result.Error, nil); err != nil {
				return len(tasks), err
			}
		}
	}
	if err := store.FinishRun(ctx, runID, successes, skipped, failures, time.Now().UTC()); err != nil {
		return len(tasks), err
	}
	return len(tasks), nil
}

func nativeMaintenanceEnabledFromRaw(sinkCfg any) bool {
	maintenance := rawMaintenanceMap(sinkCfg)
	return rawBool(maintenance, "native_enabled", false)
}

func nativeMaintenanceSettingsFromRaw(sinkCfg any) (nativeMaintenanceSettings, error) {
	settings := defaultNativeMaintenanceSettings()
	maintenance := rawMaintenanceMap(sinkCfg)
	settings.Enabled = rawBool(maintenance, "native_enabled", false)
	settings.MaxSelectedInputBytes = rawInt64(maintenance, "native_max_selected_input_bytes", settings.MaxSelectedInputBytes)
	settings.MaxSelectedFiles = rawInt(maintenance, "native_max_selected_files", settings.MaxSelectedFiles)
	settings.TargetFileSizeBytes = rawInt64(maintenance, "native_target_file_size_bytes", settings.TargetFileSizeBytes)
	settings.SmallFileSizeBytes = rawInt64(maintenance, "small_file_size_bytes", settings.SmallFileSizeBytes)
	settings.MinSmallFiles = rawInt(maintenance, "small_files_min_count", settings.MinSmallFiles)
	settings.MinSmallBytes = rawInt64(maintenance, "small_files_min_total_bytes", settings.MinSmallBytes)
	settings.ScanConcurrency = rawInt(maintenance, "native_scan_concurrency", settings.ScanConcurrency)
	settings.Timeout = time.Duration(rawInt(maintenance, "native_timeout_seconds", int(settings.Timeout/time.Second))) * time.Second
	settings.TempDirectory = rawString(maintenance, "worker_temp_directory", settings.TempDirectory)
	settings.ExpireInterval = time.Duration(rawInt(maintenance, "native_expire_interval_seconds", int(settings.ExpireInterval/time.Second))) * time.Second
	settings.SnapshotMaxAge = time.Duration(rawFloat64(maintenance, "native_snapshot_max_age_hours", settings.SnapshotMaxAge.Hours()) * float64(time.Hour))
	settings.SnapshotRetainLast = rawInt(maintenance, "native_snapshot_retain_last", settings.SnapshotRetainLast)
	settings.OrphanInterval = time.Duration(rawInt(maintenance, "native_orphan_interval_seconds", int(settings.OrphanInterval/time.Second))) * time.Second
	settings.OrphanInactiveInterval = time.Duration(rawInt(maintenance, "native_orphan_inactive_interval_seconds", int(settings.OrphanInactiveInterval/time.Second))) * time.Second
	settings.OrphanMinAge = time.Duration(rawFloat64(maintenance, "native_orphan_min_age_hours", settings.OrphanMinAge.Hours()) * float64(time.Hour))
	settings.OrphanDryRun = rawBool(maintenance, "native_orphan_dry_run", settings.OrphanDryRun)
	settings.SparkPollInterval = time.Duration(rawInt(maintenance, "spark_poll_interval_seconds", int(settings.SparkPollInterval/time.Second))) * time.Second
	settings.SparkTimeout = time.Duration(rawInt(maintenance, "spark_timeout_seconds", int(settings.SparkTimeout/time.Second))) * time.Second
	settings.IdleCompactionInterval = time.Duration(rawInt(maintenance, "native_idle_check_interval_seconds", int(settings.IdleCompactionInterval/time.Second))) * time.Second

	switch {
	case settings.MaxSelectedInputBytes <= 0:
		return settings, fmt.Errorf("native_max_selected_input_bytes must be > 0")
	case settings.MaxSelectedFiles <= 0:
		return settings, fmt.Errorf("native_max_selected_files must be > 0")
	case settings.TargetFileSizeBytes <= 0:
		return settings, fmt.Errorf("native_target_file_size_bytes must be > 0")
	case settings.SmallFileSizeBytes <= 0:
		return settings, fmt.Errorf("small_file_size_bytes must be > 0")
	case settings.MinSmallFiles <= 0:
		return settings, fmt.Errorf("small_files_min_count must be > 0")
	case settings.MinSmallBytes <= 0:
		return settings, fmt.Errorf("small_files_min_total_bytes must be > 0")
	case settings.ScanConcurrency <= 0:
		return settings, fmt.Errorf("native_scan_concurrency must be > 0")
	case settings.Timeout <= 0:
		return settings, fmt.Errorf("native_timeout_seconds must be > 0")
	case settings.ExpireInterval <= 0:
		return settings, fmt.Errorf("native_expire_interval_seconds must be > 0")
	case settings.SnapshotMaxAge <= 0:
		return settings, fmt.Errorf("native_snapshot_max_age_hours must be > 0")
	case settings.SnapshotRetainLast < 1:
		return settings, fmt.Errorf("native_snapshot_retain_last must be >= 1")
	case settings.OrphanInterval <= 0:
		return settings, fmt.Errorf("native_orphan_interval_seconds must be > 0")
	case settings.OrphanInactiveInterval < settings.OrphanInterval:
		return settings, fmt.Errorf("native_orphan_inactive_interval_seconds must be >= native_orphan_interval_seconds")
	case settings.OrphanMinAge < defaultNativeOrphanAge:
		return settings, fmt.Errorf("native_orphan_min_age_hours must be at least %.0f", defaultNativeOrphanAge.Hours())
	case settings.IdleCompactionInterval <= 0:
		return settings, fmt.Errorf("native_idle_check_interval_seconds must be > 0")
	}
	return settings, nil
}

func rawMaintenanceMap(sinkCfg any) map[string]any {
	root, ok := sinkCfg.(map[string]any)
	if !ok {
		return nil
	}
	value, ok := root["table_maintenance"]
	if !ok {
		return nil
	}
	out, _ := value.(map[string]any)
	return out
}

func rawBool(m map[string]any, key string, fallback bool) bool {
	if m == nil {
		return fallback
	}
	value, ok := m[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func rawString(m map[string]any, key, fallback string) string {
	if m == nil {
		return fallback
	}
	if value, ok := m[key].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func rawInt(m map[string]any, key string, fallback int) int {
	return int(rawInt64(m, key, int64(fallback)))
}

func rawInt64(m map[string]any, key string, fallback int64) int64 {
	if m == nil {
		return fallback
	}
	value, ok := m[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func rawFloat64(m map[string]any, key string, fallback float64) float64 {
	if m == nil {
		return fallback
	}
	value, ok := m[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		if n, err := v.Float64(); err == nil {
			return n
		}
	case string:
		if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return n
		}
	}
	return fallback
}

func canonicalMaintenanceTableKey(catalog, namespace, table string) string {
	return strings.TrimSpace(catalog) + "." + strings.TrimSpace(namespace) + "." + strings.TrimSpace(table)
}

func deterministicJitter(key string, window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}
	sum := sha256.Sum256([]byte(key))
	value := binary.BigEndian.Uint64(sum[:8])
	return time.Duration(value % uint64(window))
}

func nextMaintenanceSchedule(state meta.IcebergMaintenanceState, operation string, now time.Time, settings nativeMaintenanceSettings) time.Time {
	tableKey := state.TableKey
	interval := settings.IdleCompactionInterval
	if interval <= 0 {
		interval = defaultCompactionCheckInterval
	}
	switch operation {
	case "expire_snapshots":
		interval = settings.ExpireInterval
	case "remove_orphan_files":
		interval = settings.OrphanInterval
		if !orphanCleanupActive(state, now, settings.OrphanInterval) {
			interval = settings.OrphanInactiveInterval
		}
	}
	jitterWindow := interval / 10
	if jitterWindow > time.Hour {
		jitterWindow = time.Hour
	}
	return now.Add(interval).Add(deterministicJitter(tableKey+"|"+operation+"|next", jitterWindow))
}

func orphanCleanupActive(state meta.IcebergMaintenanceState, now time.Time, activeInterval time.Duration) bool {
	if state.LastWriteAt == nil || activeInterval <= 0 {
		return false
	}
	lastWrite := state.LastWriteAt.UTC()
	return !lastWrite.After(now) && now.Sub(lastWrite) < activeInterval
}

func stateDueTime(state meta.IcebergMaintenanceState, operation string) *time.Time {
	switch operation {
	case "compact":
		return state.NextCompactionCheckAt
	case "expire_snapshots":
		return state.NextExpireCheckAt
	case "remove_orphan_files":
		return state.NextOrphanCheckAt
	default:
		return nil
	}
}

func maintenanceRetryBackoff(attempt int, base time.Duration) time.Duration {
	if base <= 0 {
		base = defaultMaintenanceRetryBackoff
	}
	if attempt < 1 {
		attempt = 1
	}
	power := math.Pow(2, float64(minInt(attempt-1, 6)))
	return time.Duration(float64(base) * power)
}

func retryLimitFromRaw(jobCfg *config.JobConfig) int {
	if jobCfg == nil {
		return defaultMaintenanceRetryLimit
	}
	_, sinkCfg := jobSinkSpec(jobCfg)
	return rawInt(rawMaintenanceMap(sinkCfg), "retry_limit", defaultMaintenanceRetryLimit)
}

func retryBackoffFromRaw(jobCfg *config.JobConfig) time.Duration {
	if jobCfg == nil {
		return defaultMaintenanceRetryBackoff
	}
	_, sinkCfg := jobSinkSpec(jobCfg)
	seconds := rawInt(rawMaintenanceMap(sinkCfg), "retry_base_backoff_seconds", int(defaultMaintenanceRetryBackoff/time.Second))
	return time.Duration(seconds) * time.Second
}

func normalizeMaintenanceMode(mode config.JobMode) config.JobMode {
	switch mode {
	case config.JobModeInitial, config.JobModeSnapshotOnly, config.JobModeResume, config.JobModeLatestOffset, config.JobModeLatest:
		return mode
	default:
		return config.JobModeInitial
	}
}

type maintenanceMetaKeyPayload struct {
	Version int    `json:"v"`
	JobID   string `json:"job_id"`
	Mode    string `json:"mode"`
	Source  struct {
		Type   string `json:"type"`
		Config any    `json:"config"`
	} `json:"source"`
	Sink struct {
		Type   string `json:"type"`
		Config any    `json:"config"`
	} `json:"sink"`
}

func maintenanceMetaKey(jobID, mode, sourceType string, sourceCfg any, sinkType string, sinkCfg any) string {
	var payload maintenanceMetaKeyPayload
	payload.Version = 1
	payload.JobID = jobID
	payload.Mode = mode
	payload.Source.Type = sourceType
	payload.Source.Config = sourceCfg
	payload.Sink.Type = sinkType
	payload.Sink.Config = stableMaintenanceSinkConfig(sinkCfg)
	encoded, _ := json.Marshal(payload)
	sum := sha256.Sum256(encoded)
	return "rivus/v1/" + hex.EncodeToString(sum[:])
}

func stableMaintenanceSinkConfig(cfg any) any {
	m, ok := cfg.(map[string]any)
	if !ok {
		return cfg
	}
	return copyMaintenanceMapWithoutKey(m, "cdc_delete_executor")
}

func copyMaintenanceMapWithoutKey(in map[string]any, skip string) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		if key == skip {
			continue
		}
		switch nested := value.(type) {
		case map[string]any:
			out[key] = copyMaintenanceMapWithoutKey(nested, skip)
		case []any:
			items := make([]any, len(nested))
			for i, item := range nested {
				if child, ok := item.(map[string]any); ok {
					items[i] = copyMaintenanceMapWithoutKey(child, skip)
				} else {
					items[i] = item
				}
			}
			out[key] = items
		default:
			out[key] = value
		}
	}
	return out
}

func maintenanceWorkerID() string {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "rivus"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func intEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func timePtr(t time.Time) *time.Time { return &t }

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
