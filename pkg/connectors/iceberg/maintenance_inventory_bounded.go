package iceberg

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

// scanOnePendingInventoryBounded is the queue-mode equivalent of
// scanOnePendingInventory. Its store claim also checks the table execution
// lease so inventory cannot overlap a compaction/expiration/orphan task on the
// same table.
func scanOnePendingInventoryBounded(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobStore meta.JobStore,
	jobs map[string]maintenanceWorkerJob,
	opts MaintenanceWorkerOptions,
	now time.Time,
	minimumPriority int,
) (bool, error) {
	state, err := store.ClaimPendingInventoryStateForMaintenance(ctx, opts.WorkerID, now, opts.LeaseDuration, minimumPriority)
	if err != nil || state == nil {
		return false, err
	}
	return true, scanClaimedInventory(ctx, store, jobStore, jobs, opts, now, *state)
}

// scanPriorityInventoryBatchBounded drains explicit and commit-triggered
// inventory refreshes concurrently across different tables, while the durable
// per-table lease prevents those scans from racing maintenance execution.
func scanPriorityInventoryBatchBounded(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobStore meta.JobStore,
	jobs map[string]maintenanceWorkerJob,
	opts MaintenanceWorkerOptions,
	now time.Time,
	minimumPriority, limit int,
) (int, error) {
	if limit <= 0 {
		limit = interactiveInventoryBatchSize
	}
	type claimedState struct {
		state meta.IcebergMaintenanceState
	}
	claimed := make([]claimedState, 0, limit)
	for len(claimed) < limit {
		state, err := store.ClaimPendingInventoryStateForMaintenance(ctx, opts.WorkerID, now, opts.LeaseDuration, minimumPriority)
		if err != nil {
			return len(claimed), err
		}
		if state == nil {
			break
		}
		claimed = append(claimed, claimedState{state: *state})
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	var wg sync.WaitGroup
	for _, item := range claimed {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := scanClaimedInventory(ctx, store, jobStore, jobs, opts, now, item.state); err != nil {
				log.Printf("[maintenance-worker %s] priority inventory table=%s error: %v", opts.WorkerID, item.state.TableKey, err)
			}
		}()
	}
	wg.Wait()
	return len(claimed), nil
}
