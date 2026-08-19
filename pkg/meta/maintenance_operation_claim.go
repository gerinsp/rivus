package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// RecoverExpiredMaintenanceLeases moves expired task leases back to retry so
// an executor pool can pick them up again. Queue-mode workers call this from
// the scheduler loop instead of making every executor goroutine run the same
// recovery UPDATE while the queue is idle.
func (s *IcebergMaintenanceStore) RecoverExpiredMaintenanceLeases(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
	SET status='retry', lease_owner=NULL, lease_until=NULL, not_before=?, updated_at=UTC_TIMESTAMP(6)
	WHERE status='leased' AND lease_until IS NOT NULL AND lease_until < ?`, now.UTC(), now.UTC())
	return err
}

// ClaimTasksForOperation claims only one maintenance operation. Queue-mode
// executors use separate bounded pools for compact, expire_snapshots, and
// remove_orphan_files so lightweight expiration work is not serialized behind
// compaction while heavy operations still have a hard concurrency ceiling.
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

	rows, err := tx.QueryContext(ctx, `SELECT id, idempotency_key, table_key, owner_job_id, operation,
	 priority, status, attempt_count, not_before, schedule_window, payload_json, last_error,
	 created_at, updated_at
	FROM iceberg_maintenance_tasks
	WHERE status IN ('queued','retry') AND not_before <= ? AND operation=?
	ORDER BY priority ASC, not_before ASC, id ASC
	LIMIT ? FOR UPDATE SKIP LOCKED`, now.UTC(), operation, limit)
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
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
		SET status='leased', lease_owner=?, lease_until=?, attempt_count=attempt_count+1, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND status IN ('queued','retry')`, workerID, until, tasks[i].ID)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
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
