package iceberg

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	maintenanceOperationCompact = "compact"
	maintenanceOperationExpire  = "expire_snapshots"
	maintenanceOperationOrphan  = "remove_orphan_files"
)

const maintenanceShutdownFinalizeTimeout = 10 * time.Second

type maintenanceConcurrency struct {
	Compact         int
	ExpireSnapshots int
	OrphanCleanup   int
}

func maintenanceConcurrencyFromEnv() maintenanceConcurrency {
	return maintenanceConcurrency{
		Compact:         boundedMaintenanceConcurrency("RIVUS_MAINTENANCE_COMPACT_CONCURRENCY", 1),
		ExpireSnapshots: boundedMaintenanceConcurrency("RIVUS_MAINTENANCE_EXPIRE_CONCURRENCY", 4),
		OrphanCleanup:   boundedMaintenanceConcurrency("RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY", 1),
	}
}

func boundedMaintenanceConcurrency(key string, fallback int) int {
	value := intEnv(key, fallback)
	if value < 1 {
		return fallback
	}
	if value > 32 {
		return 32
	}
	return value
}

// maintenanceWorkerRegistry lets the scheduler replace the immutable job map
// after its periodic refresh while executor goroutines keep reading the last
// complete snapshot without racing on the map itself.
type maintenanceWorkerRegistry struct {
	mu   sync.RWMutex
	jobs map[string]maintenanceWorkerJob
}

func (r *maintenanceWorkerRegistry) Replace(jobs map[string]maintenanceWorkerJob) {
	r.mu.Lock()
	r.jobs = jobs
	r.mu.Unlock()
}

func (r *maintenanceWorkerRegistry) Snapshot() map[string]maintenanceWorkerJob {
	r.mu.RLock()
	jobs := r.jobs
	r.mu.RUnlock()
	return jobs
}

func startMaintenanceExecutorPools(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobStore meta.JobStore,
	registry *maintenanceWorkerRegistry,
	opts MaintenanceWorkerOptions,
	limits maintenanceConcurrency,
	wg *sync.WaitGroup,
) {
	specs := []struct {
		operation string
		workers   int
	}{
		{operation: maintenanceOperationCompact, workers: limits.Compact},
		{operation: maintenanceOperationExpire, workers: limits.ExpireSnapshots},
		{operation: maintenanceOperationOrphan, workers: limits.OrphanCleanup},
	}
	for _, spec := range specs {
		for index := 0; index < spec.workers; index++ {
			wg.Add(1)
			go func(operation string, workerIndex int) {
				defer wg.Done()
				runMaintenanceOperationWorker(ctx, store, jobStore, registry, opts, operation, workerIndex)
			}(spec.operation, index)
		}
	}
}

func runMaintenanceOperationWorker(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobStore meta.JobStore,
	registry *maintenanceWorkerRegistry,
	opts MaintenanceWorkerOptions,
	operation string,
	workerIndex int,
) {
	idlePoll := durationEnv("RIVUS_MAINTENANCE_EXECUTOR_IDLE_POLL_SECONDS", 5*time.Second)
	workerID := fmt.Sprintf("%s-%s-%d", opts.WorkerID, operation, workerIndex+1)

	for {
		if ctx.Err() != nil {
			return
		}

		tasks, err := store.ClaimTasksForOperation(ctx, workerID, time.Now().UTC(), opts.LeaseDuration, operation, 1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[maintenance-worker %s] claim operation=%s error=%v", workerID, operation, err)
			if !waitMaintenanceExecutor(ctx, idlePoll) {
				return
			}
			continue
		}
		if len(tasks) == 0 {
			if !waitMaintenanceExecutor(ctx, idlePoll) {
				return
			}
			continue
		}

		if _, err := processClaimedMaintenanceTasks(
			ctx,
			store,
			jobStore,
			registry.Snapshot(),
			opts,
			workerID,
			tasks,
			time.Now().UTC(),
		); err != nil {
			if ctx.Err() != nil {
				log.Printf("[maintenance-worker %s] shutdown finalization operation=%s error=%v", workerID, operation, err)
				return
			}
			log.Printf("[maintenance-worker %s] process operation=%s error=%v", workerID, operation, err)
		}
	}
}

func waitMaintenanceExecutor(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		delay = 5 * time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func maintenanceFinalizeContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent != nil && parent.Err() == nil {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(context.Background(), maintenanceShutdownFinalizeTimeout)
}

func processClaimedMaintenanceTasks(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobStore meta.JobStore,
	jobs map[string]maintenanceWorkerJob,
	opts MaintenanceWorkerOptions,
	workerID string,
	tasks []meta.IcebergMaintenanceTask,
	now time.Time,
) (int, error) {
	if len(tasks) == 0 {
		return 0, nil
	}

	runID, err := store.CreateRunForTasks(ctx, workerID, tasks, now)
	if err != nil {
		for _, task := range tasks {
			releaseMaintenanceTableLease(store, task.TableKey, workerID)
		}
		return 0, fmt.Errorf("create maintenance run: %w", err)
	}

	successes, skipped, failures := 0, 0, 0
	for _, task := range tasks {
		status, err := processClaimedMaintenanceTask(ctx, store, jobStore, jobs, opts, workerID, runID, task)
		if err != nil {
			failures++
			finalizeCtx, cancel := maintenanceFinalizeContext(ctx)
			_ = store.FinishRun(finalizeCtx, runID, successes, skipped, failures, time.Now().UTC())
			cancel()
			return len(tasks), err
		}
		switch status {
		case "succeeded":
			successes++
		case "skipped":
			skipped++
		default:
			failures++
		}
	}

	finalizeCtx, cancel := maintenanceFinalizeContext(ctx)
	defer cancel()
	if err := store.FinishRun(finalizeCtx, runID, successes, skipped, failures, time.Now().UTC()); err != nil {
		return len(tasks), err
	}
	return len(tasks), nil
}

func processClaimedMaintenanceTask(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobStore meta.JobStore,
	jobs map[string]maintenanceWorkerJob,
	opts MaintenanceWorkerOptions,
	workerID string,
	runID int64,
	task meta.IcebergMaintenanceTask,
) (string, error) {
	defer releaseMaintenanceTableLease(store, task.TableKey, workerID)

	state, err := store.GetState(ctx, task.TableKey)
	if err != nil {
		return "failed", err
	}
	if state == nil || !state.SnapshotComplete {
		message := "snapshot barrier is not complete"
		if err := store.InsertResult(ctx, maintenancePreflightFailureResult(runID, task, message)); err != nil {
			return "failed", fmt.Errorf("store maintenance preflight result task=%d: %w", task.ID, err)
		}
		if err := store.FinishTask(ctx, task.ID, workerID, meta.MaintenanceTaskRetry, message, timePtr(time.Now().Add(time.Minute))); err != nil {
			return "failed", err
		}
		return "failed", nil
	}

	job, ok, resolveErr := resolveMaintenanceWorkerJob(ctx, jobStore, jobs, task.OwnerJobID)
	if resolveErr != nil {
		return "failed", fmt.Errorf("load owner job configuration task=%d: %w", task.ID, resolveErr)
	}
	if !ok || job.Job.Config == nil {
		message := "owner job configuration is unavailable"
		if err := store.InsertResult(ctx, maintenancePreflightFailureResult(runID, task, message)); err != nil {
			return "failed", fmt.Errorf("store maintenance preflight result task=%d: %w", task.ID, err)
		}
		if err := store.FinishTask(ctx, task.ID, workerID, meta.MaintenanceTaskFailed, message, nil); err != nil {
			return "failed", err
		}
		return "failed", nil
	}

	taskCtx, taskCancel := context.WithCancel(ctx)
	var leaseWG sync.WaitGroup
	leaseWG.Add(1)
	go func() {
		defer leaseWG.Done()
		renewEvery := opts.LeaseDuration / 3
		if renewEvery < 10*time.Second {
			renewEvery = 10 * time.Second
		}
		ticker := time.NewTicker(renewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-ticker.C:
				if err := store.RenewTaskAndTableLease(
					taskCtx,
					task.ID,
					task.TableKey,
					workerID,
					time.Now().Add(opts.LeaseDuration),
				); err != nil {
					log.Printf("[maintenance-worker %s] lease renewal task=%d table=%s error=%v", workerID, task.ID, task.TableKey, err)
					taskCancel()
					return
				}
			}
		}
	}()

	outcome := executeNativeMaintenanceTask(taskCtx, store, task.OwnerJobID, job.Job.Config, *state, task, job.Settings)
	taskCancel()
	leaseWG.Wait()
	outcome.Result.RunID = runID

	// When SIGTERM cancels the worker context, using that same context for the
	// result/task updates would leave the durable row leased until lease expiry.
	// Finalize through a fresh bounded context so a replacement container can
	// pick the retry promptly.
	finalizeCtx, finalizeCancel := maintenanceFinalizeContext(ctx)
	defer finalizeCancel()
	if err := store.InsertResult(finalizeCtx, outcome.Result); err != nil {
		return "failed", fmt.Errorf("store maintenance result task=%d: %w", task.ID, err)
	}

	switch outcome.Result.Status {
	case "succeeded":
		_ = store.RecordStateSuccess(finalizeCtx, state.TableKey, task.Operation, time.Now().UTC(), task.Operation == maintenanceOperationCompact)
		if err := store.FinishTask(finalizeCtx, task.ID, workerID, meta.MaintenanceTaskSucceeded, "", nil); err != nil {
			return "failed", err
		}
		return "succeeded", nil
	case "skipped":
		_ = store.RecordStateSuccess(finalizeCtx, state.TableKey, task.Operation, time.Now().UTC(), false)
		if err := store.FinishTask(finalizeCtx, task.ID, workerID, meta.MaintenanceTaskSkipped, "", nil); err != nil {
			return "failed", err
		}
		return "skipped", nil
	default:
		_ = store.RecordStateError(finalizeCtx, state.TableKey, outcome.Result.Error)
		retryLimit := retryLimitFromRaw(job.Job.Config)
		if retryLimit <= 0 {
			retryLimit = defaultMaintenanceRetryLimit
		}
		interrupted := ctx.Err() != nil
		if interrupted || (outcome.Retryable && task.AttemptCount < retryLimit) {
			retryAt := time.Now().Add(maintenanceRetryBackoff(task.AttemptCount, retryBackoffFromRaw(job.Job.Config)))
			if interrupted {
				// A deployment interruption is operational, not a terminal
				// maintenance failure. Always leave it retryable even if it happened
				// on the configured final attempt.
				retryAt = time.Now().Add(time.Minute)
			}
			if err := store.FinishTask(finalizeCtx, task.ID, workerID, meta.MaintenanceTaskRetry, outcome.Result.Error, &retryAt); err != nil {
				return "failed", err
			}
		} else if err := store.FinishTask(finalizeCtx, task.ID, workerID, meta.MaintenanceTaskFailed, outcome.Result.Error, nil); err != nil {
			return "failed", err
		}
		return "failed", nil
	}
}

func releaseMaintenanceTableLease(store *meta.IcebergMaintenanceStore, tableKey, workerID string) {
	if store == nil || tableKey == "" || workerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.ReleaseMaintenanceTableLease(ctx, tableKey, workerID); err != nil {
		log.Printf("[maintenance-worker %s] release table lease table=%s error=%v", workerID, tableKey, err)
	}
}
