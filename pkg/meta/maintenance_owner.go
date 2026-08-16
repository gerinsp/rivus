package meta

import (
	"context"
	"database/sql"
	"strings"
)

// IcebergMaintenanceOwnerSummary is the small, owner-scoped queue snapshot
// used by the job-details UI. It is intentionally computed from durable MySQL
// state rather than worker process memory.
type IcebergMaintenanceOwnerSummary struct {
	Tables       int `json:"tables"`
	Blocked      int `json:"snapshot_blocked"`
	QueuedTasks  int `json:"queued_tasks"`
	RetryTasks   int `json:"retry_tasks"`
	ActiveLeases int `json:"active_leases"`
	FailedTasks  int `json:"failed_tasks"`
}

func (s *IcebergMaintenanceStore) ListStatesForOwner(ctx context.Context, ownerJobID string, limit int) ([]IcebergMaintenanceState, error) {
	ownerJobID = strings.TrimSpace(ownerJobID)
	if ownerJobID == "" {
		return []IcebergMaintenanceState{}, nil
	}
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, last_write_at, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at
	FROM iceberg_maintenance_state
	WHERE owner_job_id=?
	ORDER BY namespace_name, table_name
	LIMIT ?`, ownerJobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]IcebergMaintenanceState, 0)
	for rows.Next() {
		state, err := scanMaintenanceState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, state)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) SummaryForOwner(ctx context.Context, ownerJobID string) (IcebergMaintenanceOwnerSummary, error) {
	var out IcebergMaintenanceOwnerSummary
	ownerJobID = strings.TrimSpace(ownerJobID)
	if ownerJobID == "" {
		return out, nil
	}

	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(snapshot_complete=0),0)
	FROM iceberg_maintenance_state WHERE owner_job_id=?`, ownerJobID).Scan(&out.Tables, &out.Blocked); err != nil {
		return out, err
	}

	if err := s.db.QueryRowContext(ctx, `SELECT
	 COALESCE(SUM(status='queued'),0),
	 COALESCE(SUM(status='retry'),0),
	 COALESCE(SUM(status='leased' AND lease_until > UTC_TIMESTAMP(6)),0),
	 COALESCE(SUM(status='failed'),0)
	FROM iceberg_maintenance_tasks WHERE owner_job_id=?`, ownerJobID).Scan(
		&out.QueuedTasks, &out.RetryTasks, &out.ActiveLeases, &out.FailedTasks,
	); err != nil && err != sql.ErrNoRows {
		return out, err
	}
	return out, nil
}

func (s *IcebergMaintenanceStore) LatestResultForTable(ctx context.Context, tableKey string) (*IcebergMaintenanceResult, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,run_id,task_id,table_key,operation,engine,routing_reason,status,input_files,input_bytes,
	 output_files,output_bytes,delete_files,expired_snapshots,orphan_candidates,deleted_files,deleted_bytes,duration_ms,attempt,
	 submission_id,details_json,error_text,created_at
	FROM iceberg_maintenance_results WHERE table_key=? ORDER BY id DESC LIMIT 1`, tableKey)
	result, err := scanMaintenanceResult(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &result, nil
}
