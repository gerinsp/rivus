package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// RecoverExpiredMaintenanceLeases moves expired task leases back to retry and
// clears the corresponding per-table execution leases. Queue-mode workers call
// this from the scheduler loop instead of making every executor goroutine run
// the same recovery UPDATE while the queue is idle.
func (s *IcebergMaintenanceStore) RecoverExpiredMaintenanceLeases(ctx context.Context, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
	SET status='retry', lease_owner=NULL, lease_until=NULL, not_before=?, updated_at=UTC_TIMESTAMP(6)
	WHERE status='leased' AND lease_until IS NOT NULL AND lease_until < ?`, now.UTC(), now.UTC()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks AS task
	LEFT JOIN iceberg_maintenance_monitors AS monitor
	  ON monitor.monitor_id=SUBSTRING(task.owner_job_id, 9)
	SET task.status='cancelled', task.lease_owner=NULL, task.lease_until=NULL, task.updated_at=UTC_TIMESTAMP(6)
	WHERE task.status IN ('queued','retry') AND (
	  task.owner_job_id LIKE 'deleted-monitor:%'
	  OR (task.owner_job_id LIKE 'monitor:%' AND (monitor.monitor_id IS NULL OR monitor.status <> 'ACTIVE'))
	)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET lease_owner=NULL, lease_until=NULL, updated_at=UTC_TIMESTAMP(6)
	WHERE lease_until IS NOT NULL AND lease_until < ?`, now.UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

// ClaimPendingInventoryStateForMaintenance is the queue-mode inventory claim.
// In addition to the normal inventory lease, it refuses tables that currently
// have a maintenance execution lease. This prevents a metadata scan from
// racing a compaction/expiration commit and overwriting freshly refreshed
// inventory with a stale snapshot view.
func (s *IcebergMaintenanceStore) ClaimPendingInventoryStateForMaintenance(
	ctx context.Context,
	workerID string,
	now time.Time,
	lease time.Duration,
	minimumPriority int,
) (*IcebergMaintenanceState, error) {
	if lease <= 0 {
		lease = 15 * time.Minute
	}
	if minimumPriority < 0 {
		minimumPriority = 0
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, inventory_snapshot_id, last_inventory_at, last_write_at, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, inventory_lease_owner, inventory_lease_until, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at
	FROM iceberg_maintenance_state
	WHERE snapshot_complete=1 AND next_inventory_check_at IS NOT NULL AND next_inventory_check_at <= ?
	  AND inventory_priority >= ?
	  AND (inventory_lease_until IS NULL OR inventory_lease_until < ?)
	  AND (lease_until IS NULL OR lease_until < ?)
	  AND (owner_type <> 'monitor' OR EXISTS (
	    SELECT 1 FROM iceberg_maintenance_monitors AS monitor
	    WHERE monitor.monitor_id=SUBSTRING(iceberg_maintenance_state.owner_job_id, 9) AND monitor.status='ACTIVE'
	  ))
	ORDER BY inventory_priority DESC, next_inventory_check_at ASC, table_key
	LIMIT 1 FOR UPDATE SKIP LOCKED`, now.UTC(), minimumPriority, now.UTC(), now.UTC())
	state, err := scanMaintenanceState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	until := now.Add(lease).UTC()
	res, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET inventory_lease_owner=?, inventory_lease_until=?, next_inventory_check_at=?, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND next_inventory_check_at IS NOT NULL AND next_inventory_check_at <= ?
	  AND (inventory_lease_until IS NULL OR inventory_lease_until < ?)
	  AND (lease_until IS NULL OR lease_until < ?)`,
		workerID, until, until, state.TableKey, now.UTC(), now.UTC(), now.UTC())
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("inventory state %s was lost while claiming", state.TableKey)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &state, nil
}

// ClaimTasksForOperation claims only one maintenance operation. It also takes
// the table-level execution lease in the same transaction. Separate operation
// pools can therefore run concurrently across different tables, but compact,
// expire_snapshots, orphan cleanup, and inventory scanning never overlap on the
// same table.
func (s *IcebergMaintenanceStore) ClaimTasksForOperation(
	ctx context.Context,
	workerID string,
	now time.Time,
	lease time.Duration,
	operation string,
	limit int,
) ([]IcebergMaintenanceTask, error) {
	operation = strings.TrimSpace(operation)
	if operation == "" {
		return nil, fmt.Errorf("maintenance operation is required")
	}
	if limit <= 0 || limit > 100 {
		limit = 1
	}
	if lease <= 0 {
		lease = 15 * time.Minute
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT task.id, task.idempotency_key, task.table_key, task.owner_job_id, task.operation,
	 task.priority, task.status, task.attempt_count, task.not_before, task.schedule_window, task.payload_json, task.last_error,
	 task.created_at, task.updated_at
	FROM iceberg_maintenance_tasks AS task
	JOIN iceberg_maintenance_state AS state ON state.table_key=task.table_key
	WHERE task.status IN ('queued','retry') AND task.not_before <= ? AND task.operation=?
	  AND task.owner_job_id NOT LIKE 'deleted-monitor:%'
	  AND (state.lease_until IS NULL OR state.lease_until < ?)
	  AND (state.inventory_lease_until IS NULL OR state.inventory_lease_until < ?)
	  AND (task.owner_job_id NOT LIKE 'monitor:%' OR EXISTS (
	    SELECT 1 FROM iceberg_maintenance_monitors AS monitor
	    WHERE monitor.monitor_id=SUBSTRING(task.owner_job_id, 9) AND monitor.status='ACTIVE'
	  ))
	ORDER BY task.priority ASC, task.not_before ASC, task.id ASC
	LIMIT ? FOR UPDATE SKIP LOCKED`, now.UTC(), operation, now.UTC(), now.UTC(), limit)
	if err != nil {
		return nil, err
	}

	var tasks []IcebergMaintenanceTask
	for rows.Next() {
		var task IcebergMaintenanceTask
		var payloadJSON, lastError sql.NullString
		if err := rows.Scan(
			&task.ID,
			&task.IdempotencyKey,
			&task.TableKey,
			&task.OwnerJobID,
			&task.Operation,
			&task.Priority,
			&task.Status,
			&task.AttemptCount,
			&task.NotBefore,
			&task.ScheduleWindow,
			&payloadJSON,
			&lastError,
			&task.CreatedAt,
			&task.UpdatedAt,
		); err != nil {
			rows.Close()
			return nil, err
		}
		if payloadJSON.Valid && strings.TrimSpace(payloadJSON.String) != "" {
			_ = json.Unmarshal([]byte(payloadJSON.String), &task.Payload)
		}
		if lastError.Valid {
			task.LastError = lastError.String
		}
		tasks = append(tasks, task)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	until := now.Add(lease).UTC()
	for i := range tasks {
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state
		SET lease_owner=?, lease_until=?, updated_at=UTC_TIMESTAMP(6)
		WHERE table_key=?
		  AND (lease_until IS NULL OR lease_until < ?)
		  AND (inventory_lease_until IS NULL OR inventory_lease_until < ?)`,
			workerID, until, tasks[i].TableKey, now.UTC(), now.UTC())
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil, fmt.Errorf("maintenance table %s was lost while claiming", tasks[i].TableKey)
		}

		res, err = tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
		SET status='leased', lease_owner=?, lease_until=?, attempt_count=attempt_count+1, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND status IN ('queued','retry')`, workerID, until, tasks[i].ID)
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil, fmt.Errorf("maintenance task %d lost while claiming", tasks[i].ID)
		}
		tasks[i].Status = MaintenanceTaskLeased
		tasks[i].LeaseOwner = workerID
		tasks[i].LeaseUntil = &until
		tasks[i].AttemptCount++
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tasks, nil
}

// RenewTaskAndTableLease keeps the task lease and its table-level execution
// lease aligned so another operation cannot enter the same table while a long
// compaction or Spark fallback is still running.
func (s *IcebergMaintenanceStore) RenewTaskAndTableLease(
	ctx context.Context,
	taskID int64,
	tableKey string,
	workerID string,
	until time.Time,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks SET lease_until=?, updated_at=UTC_TIMESTAMP(6)
	WHERE id=? AND status='leased' AND lease_owner=?`, until.UTC(), taskID, workerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("maintenance task %d lease is no longer owned by %s", taskID, workerID)
	}

	res, err = tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET lease_until=?, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND lease_owner=?`, until.UTC(), tableKey, workerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("maintenance table %s lease is no longer owned by %s", tableKey, workerID)
	}
	return tx.Commit()
}

// ReleaseMaintenanceTableLease marks the end of actual work on a table. Task
// bookkeeping may still need to retry after an unrelated persistence error,
// but any later retry must acquire this table lease again before executing.
func (s *IcebergMaintenanceStore) ReleaseMaintenanceTableLease(ctx context.Context, tableKey, workerID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET lease_owner=NULL, lease_until=NULL, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND lease_owner=?`, tableKey, workerID)
	return err
}
