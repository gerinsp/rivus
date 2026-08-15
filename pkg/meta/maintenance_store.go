package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	MaintenanceTaskQueued    = "queued"
	MaintenanceTaskLeased    = "leased"
	MaintenanceTaskSucceeded = "succeeded"
	MaintenanceTaskSkipped   = "skipped"
	MaintenanceTaskFailed    = "failed"
	MaintenanceTaskRetry     = "retry"
)

type IcebergMaintenanceState struct {
	TableKey                  string     `json:"table_key"`
	Catalog                   string     `json:"catalog"`
	Namespace                 string     `json:"namespace"`
	Table                     string     `json:"table"`
	OwnerType                 string     `json:"owner_type"`
	OwnerJobID                string     `json:"owner_job_id"`
	SnapshotComplete          bool       `json:"snapshot_complete"`
	LastSnapshotID            int64      `json:"last_snapshot_id"`
	NewDataFiles              int        `json:"new_data_files"`
	NewEqualityDeleteFiles    int        `json:"new_equality_delete_files"`
	ActiveDataFiles           int        `json:"active_data_files"`
	ActiveSmallFiles          int        `json:"active_small_files"`
	ActiveSmallBytes          int64      `json:"active_small_bytes"`
	ActiveEqualityDeleteFiles int        `json:"active_equality_delete_files"`
	ActivePositionDeleteFiles int        `json:"active_position_delete_files"`
	NextCompactionCheckAt     *time.Time `json:"next_compaction_check_at,omitempty"`
	NextExpireCheckAt         *time.Time `json:"next_expire_check_at,omitempty"`
	NextOrphanCheckAt         *time.Time `json:"next_orphan_check_at,omitempty"`
	LastCompactionAt          *time.Time `json:"last_compaction_at,omitempty"`
	LastExpireAt              *time.Time `json:"last_expire_at,omitempty"`
	LastOrphanAt              *time.Time `json:"last_orphan_at,omitempty"`
	LeaseOwner                string     `json:"lease_owner,omitempty"`
	LeaseUntil                *time.Time `json:"lease_until,omitempty"`
	AttemptCount              int        `json:"attempt_count"`
	LastError                 string     `json:"last_error,omitempty"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
}

type IcebergMaintenanceTask struct {
	ID             int64          `json:"id"`
	IdempotencyKey string         `json:"idempotency_key"`
	TableKey       string         `json:"table_key"`
	OwnerJobID     string         `json:"owner_job_id"`
	Operation      string         `json:"operation"`
	Priority       int            `json:"priority"`
	Status         string         `json:"status"`
	LeaseOwner     string         `json:"lease_owner,omitempty"`
	LeaseUntil     *time.Time     `json:"lease_until,omitempty"`
	AttemptCount   int            `json:"attempt_count"`
	NotBefore      time.Time      `json:"not_before"`
	ScheduleWindow string         `json:"schedule_window"`
	Payload        map[string]any `json:"payload,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type IcebergMaintenanceRun struct {
	ID           int64      `json:"id"`
	WorkerID     string     `json:"worker_id"`
	Status       string     `json:"status"`
	TaskCount    int        `json:"task_count"`
	SuccessCount int        `json:"success_count"`
	SkippedCount int        `json:"skipped_count"`
	FailedCount  int        `json:"failed_count"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

type IcebergMaintenanceResult struct {
	ID               int64          `json:"id"`
	RunID            int64          `json:"run_id"`
	TaskID           int64          `json:"task_id"`
	TableKey         string         `json:"table_key"`
	Operation        string         `json:"operation"`
	Engine           string         `json:"engine"`
	RoutingReason    string         `json:"routing_reason,omitempty"`
	Status           string         `json:"status"`
	InputFiles       int            `json:"input_files"`
	InputBytes       int64          `json:"input_bytes"`
	OutputFiles      int            `json:"output_files"`
	OutputBytes      int64          `json:"output_bytes"`
	DeleteFiles      int            `json:"delete_files"`
	ExpiredSnapshots int            `json:"expired_snapshots"`
	OrphanCandidates int            `json:"orphan_candidates"`
	DeletedFiles     int            `json:"deleted_files"`
	DeletedBytes     int64          `json:"deleted_bytes"`
	DurationMillis   int64          `json:"duration_ms"`
	Attempt          int            `json:"attempt"`
	SubmissionID     string         `json:"submission_id,omitempty"`
	Details          map[string]any `json:"details,omitempty"`
	Error            string         `json:"error,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

type IcebergMaintenanceSummary struct {
	Tables             int        `json:"tables"`
	SnapshotBlocked    int        `json:"snapshot_blocked"`
	QueuedTasks        int        `json:"queued_tasks"`
	RetryTasks         int        `json:"retry_tasks"`
	ActiveLeases       int        `json:"active_leases"`
	FailedTasks        int        `json:"failed_tasks"`
	OldestQueuedAt     *time.Time `json:"oldest_queued_at,omitempty"`
	OldestQueuedAgeSec int64      `json:"oldest_queued_age_seconds"`
}

type IcebergMaintenanceStore struct {
	db *sql.DB
}

func NewIcebergMaintenanceStore(dsn string) (*IcebergMaintenanceStore, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("maintenance MySQL DSN is empty")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &IcebergMaintenanceStore{db: db}, nil
}

func (s *IcebergMaintenanceStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *IcebergMaintenanceStore) Init(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("maintenance store is nil")
	}
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_state (
		  table_key VARCHAR(512) NOT NULL PRIMARY KEY,
		  catalog VARCHAR(255) NOT NULL,
		  namespace_name VARCHAR(512) NOT NULL,
		  table_name VARCHAR(255) NOT NULL,
		  owner_type VARCHAR(32) NOT NULL DEFAULT 'job',
		  owner_job_id VARCHAR(255) NOT NULL,
		  snapshot_complete TINYINT(1) NOT NULL DEFAULT 0,
		  last_snapshot_id BIGINT NOT NULL DEFAULT 0,
		  new_data_files INT NOT NULL DEFAULT 0,
		  new_equality_delete_files INT NOT NULL DEFAULT 0,
		  active_data_files INT NOT NULL DEFAULT 0,
		  active_small_files INT NOT NULL DEFAULT 0,
		  active_small_bytes BIGINT NOT NULL DEFAULT 0,
		  active_equality_delete_files INT NOT NULL DEFAULT 0,
		  active_position_delete_files INT NOT NULL DEFAULT 0,
		  next_compaction_check_at DATETIME(6) NULL,
		  next_expire_check_at DATETIME(6) NULL,
		  next_orphan_check_at DATETIME(6) NULL,
		  last_compaction_at DATETIME(6) NULL,
		  last_expire_at DATETIME(6) NULL,
		  last_orphan_at DATETIME(6) NULL,
		  lease_owner VARCHAR(255) NULL,
		  lease_until DATETIME(6) NULL,
		  attempt_count INT NOT NULL DEFAULT 0,
		  last_error LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  updated_at DATETIME(6) NOT NULL,
		  INDEX idx_maintenance_state_compaction (snapshot_complete, next_compaction_check_at),
		  INDEX idx_maintenance_state_expire (snapshot_complete, next_expire_check_at),
		  INDEX idx_maintenance_state_orphan (snapshot_complete, next_orphan_check_at),
		  INDEX idx_maintenance_state_owner (owner_job_id),
		  INDEX idx_maintenance_state_lease (lease_until)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_tasks (
		  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		  idempotency_key VARCHAR(64) NOT NULL,
		  table_key VARCHAR(512) NOT NULL,
		  owner_job_id VARCHAR(255) NOT NULL,
		  operation VARCHAR(64) NOT NULL,
		  priority INT NOT NULL DEFAULT 100,
		  status VARCHAR(32) NOT NULL,
		  lease_owner VARCHAR(255) NULL,
		  lease_until DATETIME(6) NULL,
		  attempt_count INT NOT NULL DEFAULT 0,
		  not_before DATETIME(6) NOT NULL,
		  schedule_window VARCHAR(64) NOT NULL,
		  payload_json LONGTEXT NULL,
		  last_error LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  updated_at DATETIME(6) NOT NULL,
		  UNIQUE KEY uq_maintenance_task_idempotency (idempotency_key),
		  INDEX idx_maintenance_task_due (status, not_before, priority, id),
		  INDEX idx_maintenance_task_lease (status, lease_until),
		  INDEX idx_maintenance_task_table (table_key, operation, status)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_runs (
		  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		  worker_id VARCHAR(255) NOT NULL,
		  status VARCHAR(32) NOT NULL,
		  task_count INT NOT NULL DEFAULT 0,
		  success_count INT NOT NULL DEFAULT 0,
		  skipped_count INT NOT NULL DEFAULT 0,
		  failed_count INT NOT NULL DEFAULT 0,
		  started_at DATETIME(6) NOT NULL,
		  finished_at DATETIME(6) NULL,
		  created_at DATETIME(6) NOT NULL,
		  INDEX idx_maintenance_runs_started (started_at),
		  INDEX idx_maintenance_runs_status (status)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_results (
		  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		  run_id BIGINT NOT NULL,
		  task_id BIGINT NOT NULL,
		  table_key VARCHAR(512) NOT NULL,
		  operation VARCHAR(64) NOT NULL,
		  engine VARCHAR(32) NOT NULL,
		  routing_reason VARCHAR(255) NULL,
		  status VARCHAR(32) NOT NULL,
		  input_files INT NOT NULL DEFAULT 0,
		  input_bytes BIGINT NOT NULL DEFAULT 0,
		  output_files INT NOT NULL DEFAULT 0,
		  output_bytes BIGINT NOT NULL DEFAULT 0,
		  delete_files INT NOT NULL DEFAULT 0,
		  expired_snapshots INT NOT NULL DEFAULT 0,
		  orphan_candidates INT NOT NULL DEFAULT 0,
		  deleted_files INT NOT NULL DEFAULT 0,
		  deleted_bytes BIGINT NOT NULL DEFAULT 0,
		  duration_ms BIGINT NOT NULL DEFAULT 0,
		  attempt INT NOT NULL DEFAULT 0,
		  submission_id VARCHAR(255) NULL,
		  details_json LONGTEXT NULL,
		  error_text LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  UNIQUE KEY uq_maintenance_result_task_attempt (task_id, attempt),
		  INDEX idx_maintenance_results_run (run_id, id),
		  INDEX idx_maintenance_results_table (table_key, created_at),
		  INDEX idx_maintenance_results_status (status, created_at)
		)`,
	}
	for _, ddl := range ddls {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

func (s *IcebergMaintenanceStore) SnapshotDone(ctx context.Context, metaKey string) (bool, bool, error) {
	var done int
	err := s.db.QueryRowContext(ctx, `SELECT done FROM job_snapshots WHERE job_id = ?`, metaKey).Scan(&done)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return done != 0, true, nil
}

func (s *IcebergMaintenanceStore) UpsertState(ctx context.Context, state IcebergMaintenanceState, initialCompaction, initialExpire, initialOrphan time.Time) error {
	if strings.TrimSpace(state.TableKey) == "" {
		return fmt.Errorf("maintenance state table key is empty")
	}
	const stmt = `INSERT INTO iceberg_maintenance_state (
	  table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	  snapshot_complete, last_snapshot_id,
	  next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	  created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
	ON DUPLICATE KEY UPDATE
	  catalog = VALUES(catalog), namespace_name = VALUES(namespace_name), table_name = VALUES(table_name),
	  owner_type = VALUES(owner_type), owner_job_id = VALUES(owner_job_id),
	  snapshot_complete = GREATEST(snapshot_complete, VALUES(snapshot_complete)),
	  last_snapshot_id = GREATEST(last_snapshot_id, VALUES(last_snapshot_id)),
	  next_compaction_check_at = COALESCE(next_compaction_check_at, VALUES(next_compaction_check_at)),
	  next_expire_check_at = COALESCE(next_expire_check_at, VALUES(next_expire_check_at)),
	  next_orphan_check_at = COALESCE(next_orphan_check_at, VALUES(next_orphan_check_at)),
	  updated_at = UTC_TIMESTAMP(6)`
	_, err := s.db.ExecContext(ctx, stmt,
		state.TableKey, state.Catalog, state.Namespace, state.Table,
		firstNonEmptyMeta(state.OwnerType, "job"), state.OwnerJobID,
		boolInt(state.SnapshotComplete), state.LastSnapshotID,
		initialCompaction.UTC(), initialExpire.UTC(), initialOrphan.UTC(),
	)
	return err
}

func (s *IcebergMaintenanceStore) CoalesceSignal(ctx context.Context, tableKey string, snapshotID int64, newDataFiles, newEqDeleteFiles int, snapshotComplete bool, nextCompaction time.Time) error {
	const stmt = `UPDATE iceberg_maintenance_state
	SET last_snapshot_id = GREATEST(last_snapshot_id, ?),
	    new_data_files = new_data_files + GREATEST(?, 0),
	    new_equality_delete_files = new_equality_delete_files + GREATEST(?, 0),
	    snapshot_complete = GREATEST(snapshot_complete, ?),
	    next_compaction_check_at = CASE
	      WHEN ? IS NULL THEN next_compaction_check_at
	      WHEN next_compaction_check_at IS NULL OR next_compaction_check_at > ? THEN ?
	      ELSE next_compaction_check_at END,
	    updated_at = UTC_TIMESTAMP(6)
	WHERE table_key = ?`
	var due any
	if !nextCompaction.IsZero() {
		due = nextCompaction.UTC()
	}
	_, err := s.db.ExecContext(ctx, stmt, snapshotID, newDataFiles, newEqDeleteFiles, boolInt(snapshotComplete), due, due, due, tableKey)
	return err
}

func (s *IcebergMaintenanceStore) DueStates(ctx context.Context, operation string, now time.Time, limit int) ([]IcebergMaintenanceState, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	column, err := dueColumn(operation)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at
	FROM iceberg_maintenance_state
	WHERE snapshot_complete = 1 AND %s IS NOT NULL AND %s <= ?
	ORDER BY %s ASC, table_key ASC LIMIT ?`, column, column, column)
	rows, err := s.db.QueryContext(ctx, query, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]IcebergMaintenanceState, 0, limit)
	for rows.Next() {
		state, err := scanMaintenanceState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, state)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) EnqueueTask(ctx context.Context, state IcebergMaintenanceState, operation string, priority int, scheduleWindow string, notBefore time.Time, payload map[string]any) (bool, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	rawIdempotency := fmt.Sprintf("%s|%s|%s", state.TableKey, operation, scheduleWindow)
	sum := sha256.Sum256([]byte(rawIdempotency))
	idempotency := hex.EncodeToString(sum[:])
	const stmt = `INSERT IGNORE INTO iceberg_maintenance_tasks (
	 idempotency_key, table_key, owner_job_id, operation, priority, status, attempt_count,
	 not_before, schedule_window, payload_json, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))`
	res, err := s.db.ExecContext(ctx, stmt, idempotency, state.TableKey, state.OwnerJobID, operation, priority, notBefore.UTC(), scheduleWindow, string(payloadJSON))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *IcebergMaintenanceStore) AdvanceSchedule(ctx context.Context, tableKey, operation string, next time.Time) error {
	column, err := dueColumn(operation)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`UPDATE iceberg_maintenance_state SET %s = ?, updated_at = UTC_TIMESTAMP(6) WHERE table_key = ?`, column)
	_, err = s.db.ExecContext(ctx, query, next.UTC(), tableKey)
	return err
}

func (s *IcebergMaintenanceStore) ClaimTasks(ctx context.Context, workerID string, now time.Time, lease time.Duration, limit int) ([]IcebergMaintenanceTask, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if lease <= 0 {
		lease = 15 * time.Minute
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
	SET status = 'retry', lease_owner = NULL, lease_until = NULL, not_before = ?, updated_at = UTC_TIMESTAMP(6)
	WHERE status = 'leased' AND lease_until IS NOT NULL AND lease_until < ?`, now.UTC(), now.UTC()); err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT id, idempotency_key, table_key, owner_job_id, operation,
	 priority, status, attempt_count, not_before, schedule_window, payload_json, last_error,
	 created_at, updated_at
	FROM iceberg_maintenance_tasks
	WHERE status IN ('queued','retry') AND not_before <= ?
	ORDER BY priority ASC, not_before ASC, id ASC
	LIMIT ? FOR UPDATE SKIP LOCKED`, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	var tasks []IcebergMaintenanceTask
	for rows.Next() {
		var task IcebergMaintenanceTask
		var payloadJSON, lastError sql.NullString
		if err := rows.Scan(&task.ID, &task.IdempotencyKey, &task.TableKey, &task.OwnerJobID, &task.Operation,
			&task.Priority, &task.Status, &task.AttemptCount, &task.NotBefore, &task.ScheduleWindow,
			&payloadJSON, &lastError, &task.CreatedAt, &task.UpdatedAt); err != nil {
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

func (s *IcebergMaintenanceStore) RenewLease(ctx context.Context, taskID int64, workerID string, until time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks SET lease_until=?, updated_at=UTC_TIMESTAMP(6)
	WHERE id=? AND status='leased' AND lease_owner=?`, until.UTC(), taskID, workerID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("maintenance task %d lease is no longer owned by %s", taskID, workerID)
	}
	return nil
}

func (s *IcebergMaintenanceStore) FinishTask(ctx context.Context, taskID int64, workerID, status, lastError string, retryAt *time.Time) error {
	if status == MaintenanceTaskRetry {
		if retryAt == nil {
			return fmt.Errorf("retry task %d requires retry time", taskID)
		}
		_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
		SET status='retry', lease_owner=NULL, lease_until=NULL, not_before=?, last_error=?, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND status='leased' AND lease_owner=?`, retryAt.UTC(), nullString(lastError), taskID, workerID)
		return err
	}
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
	SET status=?, lease_owner=NULL, lease_until=NULL, last_error=?, updated_at=UTC_TIMESTAMP(6)
	WHERE id=? AND status='leased' AND lease_owner=?`, status, nullString(lastError), taskID, workerID)
	return err
}

func (s *IcebergMaintenanceStore) UpdateInventory(ctx context.Context, tableKey string, snapshotID int64, dataFiles, smallFiles int, smallBytes int64, equalityDeletes, positionDeletes int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET
	 last_snapshot_id=GREATEST(last_snapshot_id, ?), active_data_files=?, active_small_files=?, active_small_bytes=?,
	 active_equality_delete_files=?, active_position_delete_files=?, last_error=NULL, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=?`, snapshotID, dataFiles, smallFiles, smallBytes, equalityDeletes, positionDeletes, tableKey)
	return err
}

func (s *IcebergMaintenanceStore) RecordStateSuccess(ctx context.Context, tableKey, operation string, at time.Time) error {
	column := ""
	switch operation {
	case "compact":
		column = "last_compaction_at"
	case "expire_snapshots":
		column = "last_expire_at"
	case "remove_orphan_files":
		column = "last_orphan_at"
	default:
		return fmt.Errorf("unsupported maintenance operation %q", operation)
	}
	extra := ""
	if operation == "compact" {
		extra = ", new_data_files=0, new_equality_delete_files=0"
	}
	query := fmt.Sprintf(`UPDATE iceberg_maintenance_state SET %s=?, attempt_count=0, last_error=NULL%s, updated_at=UTC_TIMESTAMP(6) WHERE table_key=?`, column, extra)
	_, err := s.db.ExecContext(ctx, query, at.UTC(), tableKey)
	return err
}

func (s *IcebergMaintenanceStore) RecordStateError(ctx context.Context, tableKey, message string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET attempt_count=attempt_count+1, last_error=?, updated_at=UTC_TIMESTAMP(6) WHERE table_key=?`, nullString(message), tableKey)
	return err
}

func (s *IcebergMaintenanceStore) CreateRun(ctx context.Context, workerID string, taskCount int, at time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `INSERT INTO iceberg_maintenance_runs
	(worker_id,status,task_count,success_count,skipped_count,failed_count,started_at,created_at)
	VALUES (?, 'running', ?, 0, 0, 0, ?, ?)`, workerID, taskCount, at.UTC(), at.UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *IcebergMaintenanceStore) FinishRun(ctx context.Context, runID int64, success, skipped, failed int, at time.Time) error {
	status := "finished"
	if failed > 0 {
		status = "finished_with_errors"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_runs SET status=?, success_count=?, skipped_count=?, failed_count=?, finished_at=? WHERE id=?`,
		status, success, skipped, failed, at.UTC(), runID)
	return err
}

func (s *IcebergMaintenanceStore) InsertResult(ctx context.Context, result IcebergMaintenanceResult) error {
	detailsJSON, err := json.Marshal(result.Details)
	if err != nil {
		return err
	}
	if result.CreatedAt.IsZero() {
		result.CreatedAt = time.Now().UTC()
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO iceberg_maintenance_results (
	 run_id,task_id,table_key,operation,engine,routing_reason,status,input_files,input_bytes,
	 output_files,output_bytes,delete_files,expired_snapshots,orphan_candidates,deleted_files,
	 deleted_bytes,duration_ms,attempt,submission_id,details_json,error_text,created_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON DUPLICATE KEY UPDATE status=VALUES(status), engine=VALUES(engine), routing_reason=VALUES(routing_reason),
	 input_files=VALUES(input_files), input_bytes=VALUES(input_bytes), output_files=VALUES(output_files),
	 output_bytes=VALUES(output_bytes), delete_files=VALUES(delete_files), expired_snapshots=VALUES(expired_snapshots),
	 orphan_candidates=VALUES(orphan_candidates), deleted_files=VALUES(deleted_files), deleted_bytes=VALUES(deleted_bytes),
	 duration_ms=VALUES(duration_ms), submission_id=VALUES(submission_id), details_json=VALUES(details_json), error_text=VALUES(error_text)`,
		result.RunID, result.TaskID, result.TableKey, result.Operation, result.Engine, nullString(result.RoutingReason), result.Status,
		result.InputFiles, result.InputBytes, result.OutputFiles, result.OutputBytes, result.DeleteFiles, result.ExpiredSnapshots,
		result.OrphanCandidates, result.DeletedFiles, result.DeletedBytes, result.DurationMillis, result.Attempt,
		nullString(result.SubmissionID), string(detailsJSON), nullString(result.Error), result.CreatedAt.UTC())
	return err
}

func (s *IcebergMaintenanceStore) Summary(ctx context.Context, now time.Time) (IcebergMaintenanceSummary, error) {
	var out IcebergMaintenanceSummary
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(snapshot_complete=0),0) FROM iceberg_maintenance_state`).Scan(&out.Tables, &out.SnapshotBlocked); err != nil {
		return out, err
	}
	var oldest sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT
	 COALESCE(SUM(status='queued'),0), COALESCE(SUM(status='retry'),0),
	 COALESCE(SUM(status='leased' AND lease_until > UTC_TIMESTAMP(6)),0), COALESCE(SUM(status='failed'),0),
	 MIN(CASE WHEN status IN ('queued','retry') THEN created_at END)
	FROM iceberg_maintenance_tasks`).Scan(&out.QueuedTasks, &out.RetryTasks, &out.ActiveLeases, &out.FailedTasks, &oldest); err != nil {
		return out, err
	}
	if oldest.Valid {
		t := oldest.Time.UTC()
		out.OldestQueuedAt = &t
		out.OldestQueuedAgeSec = int64(now.UTC().Sub(t).Seconds())
		if out.OldestQueuedAgeSec < 0 {
			out.OldestQueuedAgeSec = 0
		}
	}
	return out, nil
}

func (s *IcebergMaintenanceStore) ListRuns(ctx context.Context, limit, offset int) ([]IcebergMaintenanceRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,worker_id,status,task_count,success_count,skipped_count,failed_count,started_at,finished_at,created_at
	FROM iceberg_maintenance_runs ORDER BY id DESC LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IcebergMaintenanceRun
	for rows.Next() {
		var run IcebergMaintenanceRun
		var finished sql.NullTime
		if err := rows.Scan(&run.ID, &run.WorkerID, &run.Status, &run.TaskCount, &run.SuccessCount, &run.SkippedCount, &run.FailedCount, &run.StartedAt, &finished, &run.CreatedAt); err != nil {
			return nil, err
		}
		if finished.Valid {
			t := finished.Time.UTC()
			run.FinishedAt = &t
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) ListResultsForRun(ctx context.Context, runID int64, limit int) ([]IcebergMaintenanceResult, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,run_id,task_id,table_key,operation,engine,routing_reason,status,input_files,input_bytes,
	 output_files,output_bytes,delete_files,expired_snapshots,orphan_candidates,deleted_files,deleted_bytes,duration_ms,attempt,
	 submission_id,details_json,error_text,created_at FROM iceberg_maintenance_results WHERE run_id=? ORDER BY id ASC LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IcebergMaintenanceResult
	for rows.Next() {
		result, err := scanMaintenanceResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) GetState(ctx context.Context, tableKey string) (*IcebergMaintenanceState, error) {
	row := s.db.QueryRowContext(ctx, `SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at FROM iceberg_maintenance_state WHERE table_key=?`, tableKey)
	state, err := scanMaintenanceState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMaintenanceState(row rowScanner) (IcebergMaintenanceState, error) {
	var state IcebergMaintenanceState
	var snapshotComplete int
	var nextCompaction, nextExpire, nextOrphan, lastCompaction, lastExpire, lastOrphan, leaseUntil sql.NullTime
	var leaseOwner, lastError sql.NullString
	err := row.Scan(&state.TableKey, &state.Catalog, &state.Namespace, &state.Table, &state.OwnerType, &state.OwnerJobID,
		&snapshotComplete, &state.LastSnapshotID, &state.NewDataFiles, &state.NewEqualityDeleteFiles,
		&state.ActiveDataFiles, &state.ActiveSmallFiles, &state.ActiveSmallBytes, &state.ActiveEqualityDeleteFiles,
		&state.ActivePositionDeleteFiles, &nextCompaction, &nextExpire, &nextOrphan,
		&lastCompaction, &lastExpire, &lastOrphan, &leaseOwner, &leaseUntil,
		&state.AttemptCount, &lastError, &state.CreatedAt, &state.UpdatedAt)
	if err != nil {
		return state, err
	}
	state.SnapshotComplete = snapshotComplete != 0
	state.NextCompactionCheckAt = nullTimePtr(nextCompaction)
	state.NextExpireCheckAt = nullTimePtr(nextExpire)
	state.NextOrphanCheckAt = nullTimePtr(nextOrphan)
	state.LastCompactionAt = nullTimePtr(lastCompaction)
	state.LastExpireAt = nullTimePtr(lastExpire)
	state.LastOrphanAt = nullTimePtr(lastOrphan)
	state.LeaseUntil = nullTimePtr(leaseUntil)
	if leaseOwner.Valid {
		state.LeaseOwner = leaseOwner.String
	}
	if lastError.Valid {
		state.LastError = lastError.String
	}
	return state, nil
}

func scanMaintenanceResult(row rowScanner) (IcebergMaintenanceResult, error) {
	var result IcebergMaintenanceResult
	var routingReason, submissionID, detailsJSON, errorText sql.NullString
	err := row.Scan(&result.ID, &result.RunID, &result.TaskID, &result.TableKey, &result.Operation, &result.Engine,
		&routingReason, &result.Status, &result.InputFiles, &result.InputBytes, &result.OutputFiles, &result.OutputBytes,
		&result.DeleteFiles, &result.ExpiredSnapshots, &result.OrphanCandidates, &result.DeletedFiles, &result.DeletedBytes,
		&result.DurationMillis, &result.Attempt, &submissionID, &detailsJSON, &errorText, &result.CreatedAt)
	if err != nil {
		return result, err
	}
	if routingReason.Valid {
		result.RoutingReason = routingReason.String
	}
	if submissionID.Valid {
		result.SubmissionID = submissionID.String
	}
	if detailsJSON.Valid && strings.TrimSpace(detailsJSON.String) != "" {
		_ = json.Unmarshal([]byte(detailsJSON.String), &result.Details)
	}
	if errorText.Valid {
		result.Error = errorText.String
	}
	return result, nil
}

func dueColumn(operation string) (string, error) {
	switch operation {
	case "compact":
		return "next_compaction_check_at", nil
	case "expire_snapshots":
		return "next_expire_check_at", nil
	case "remove_orphan_files":
		return "next_orphan_check_at", nil
	default:
		return "", fmt.Errorf("unsupported maintenance operation %q", operation)
	}
}

func nullTimePtr(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time.UTC()
	return &t
}

func nullString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func firstNonEmptyMeta(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
