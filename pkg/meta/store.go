package meta

import (
	"context"
	"database/sql"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Offset struct {
	BinlogFile string
	BinlogPos  uint32
	UpdatedAt  time.Time
}

type SnapshotState struct {
	JobID     string
	StartFile string
	StartPos  uint32
	Done      bool
	UpdatedAt time.Time
}

type SnapshotProgress struct {
	JobID      string
	TableName  string
	NextOffset int64
	CursorJSON string
	UpdatedAt  time.Time
}

type OffsetStore interface {
	GetOffset(ctx context.Context, jobID string) (*Offset, error)
	SaveOffset(ctx context.Context, jobID string, offset Offset) error

	GetSnapshotState(ctx context.Context, jobID string) (*SnapshotState, error)
	SaveSnapshotStart(ctx context.Context, jobID string, start Offset) error
	MarkSnapshotDone(ctx context.Context, jobID string) error
	GetSnapshotProgress(ctx context.Context, jobID string) (*SnapshotProgress, error)
	SaveSnapshotProgress(ctx context.Context, jobID string, tableName string, nextOffset int64, cursorJSON string) error
	ClearSnapshotProgress(ctx context.Context, jobID string) error

	// NEW: buat bersihin meta saat delete job
	DeleteJobState(ctx context.Context, jobID string) error
}

type MySQLOffsetStore struct {
	db *sql.DB
}

func NewMySQLOffsetStore(dsn string) (*MySQLOffsetStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	return &MySQLOffsetStore{db: db}, nil
}

func (s *MySQLOffsetStore) Init(ctx context.Context) error {
	ddl := `
CREATE TABLE IF NOT EXISTS job_offsets (
  job_id      VARCHAR(255) NOT NULL PRIMARY KEY,
  binlog_file VARCHAR(255) NOT NULL,
  binlog_pos  BIGINT NOT NULL,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return err
	}

	ddl2 := `
CREATE TABLE IF NOT EXISTS job_snapshots (
  job_id      VARCHAR(255) NOT NULL PRIMARY KEY,
  start_file  VARCHAR(255) NOT NULL,
  start_pos   BIGINT NOT NULL,
  done        TINYINT(1) NOT NULL DEFAULT 0,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`
	if _, err := s.db.ExecContext(ctx, ddl2); err != nil {
		return err
	}

	ddl3 := `
CREATE TABLE IF NOT EXISTS job_snapshot_progress (
  job_id       VARCHAR(255) NOT NULL PRIMARY KEY,
  table_name   VARCHAR(255) NOT NULL,
  next_offset  BIGINT NOT NULL,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`
	if _, err := s.db.ExecContext(ctx, ddl3); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, `ALTER TABLE job_snapshot_progress ADD COLUMN cursor_json LONGTEXT NULL AFTER next_offset`); err != nil && !isDuplicateColumnError(err) {
		return err
	}

	return nil
}

func (s *MySQLOffsetStore) GetOffset(ctx context.Context, jobID string) (*Offset, error) {
	const q = `SELECT binlog_file, binlog_pos, updated_at FROM job_offsets WHERE job_id = ?`
	row := s.db.QueryRowContext(ctx, q, jobID)

	var off Offset
	if err := row.Scan(&off.BinlogFile, &off.BinlogPos, &off.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &off, nil
}

func (s *MySQLOffsetStore) SaveOffset(ctx context.Context, jobID string, off Offset) error {
	const stmt = `
INSERT INTO job_offsets (job_id, binlog_file, binlog_pos, updated_at)
VALUES (?, ?, ?, NOW())
ON DUPLICATE KEY UPDATE
  binlog_file = VALUES(binlog_file),
  binlog_pos = VALUES(binlog_pos),
  updated_at = NOW();
`
	_, err := s.db.ExecContext(ctx, stmt, jobID, off.BinlogFile, off.BinlogPos)
	return err
}

func (s *MySQLOffsetStore) GetSnapshotState(ctx context.Context, jobID string) (*SnapshotState, error) {
	const q = `SELECT job_id, start_file, start_pos, done, updated_at FROM job_snapshots WHERE job_id = ?`
	row := s.db.QueryRowContext(ctx, q, jobID)

	var st SnapshotState
	var doneInt int
	if err := row.Scan(&st.JobID, &st.StartFile, &st.StartPos, &doneInt, &st.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	st.Done = doneInt != 0
	return &st, nil
}

func (s *MySQLOffsetStore) SaveSnapshotStart(ctx context.Context, jobID string, start Offset) error {
	const stmt = `
INSERT INTO job_snapshots (job_id, start_file, start_pos, done, updated_at)
VALUES (?, ?, ?, 0, NOW())
ON DUPLICATE KEY UPDATE
  start_file = VALUES(start_file),
  start_pos = VALUES(start_pos),
  done = 0,
  updated_at = NOW();
`
	_, err := s.db.ExecContext(ctx, stmt, jobID, start.BinlogFile, start.BinlogPos)
	return err
}

func (s *MySQLOffsetStore) MarkSnapshotDone(ctx context.Context, jobID string) error {
	const stmt = `
UPDATE job_snapshots
SET done = 1, updated_at = NOW()
WHERE job_id = ?;
`
	_, err := s.db.ExecContext(ctx, stmt, jobID)
	return err
}

func (s *MySQLOffsetStore) GetSnapshotProgress(ctx context.Context, jobID string) (*SnapshotProgress, error) {
	const q = `SELECT job_id, table_name, next_offset, cursor_json, updated_at FROM job_snapshot_progress WHERE job_id = ?`
	row := s.db.QueryRowContext(ctx, q, jobID)

	var st SnapshotProgress
	var cursorJSON sql.NullString
	if err := row.Scan(&st.JobID, &st.TableName, &st.NextOffset, &cursorJSON, &st.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if cursorJSON.Valid {
		st.CursorJSON = cursorJSON.String
	}
	return &st, nil
}

func (s *MySQLOffsetStore) SaveSnapshotProgress(ctx context.Context, jobID string, tableName string, nextOffset int64, cursorJSON string) error {
	const stmt = `
INSERT INTO job_snapshot_progress (job_id, table_name, next_offset, cursor_json, updated_at)
VALUES (?, ?, ?, ?, NOW())
ON DUPLICATE KEY UPDATE
  table_name = VALUES(table_name),
  next_offset = VALUES(next_offset),
  cursor_json = VALUES(cursor_json),
  updated_at = NOW();
`
	_, err := s.db.ExecContext(ctx, stmt, jobID, tableName, nextOffset, nullableString(cursorJSON))
	return err
}

func (s *MySQLOffsetStore) ClearSnapshotProgress(ctx context.Context, jobID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM job_snapshot_progress WHERE job_id = ?`, jobID)
	return err
}

func (s *MySQLOffsetStore) DeleteJobState(ctx context.Context, jobID string) error {
	// best-effort: hapus snapshot & offset
	_, _ = s.db.ExecContext(ctx, `DELETE FROM job_snapshots WHERE job_id = ?`, jobID)
	_, _ = s.db.ExecContext(ctx, `DELETE FROM job_snapshot_progress WHERE job_id = ?`, jobID)
	_, _ = s.db.ExecContext(ctx, `DELETE FROM job_offsets WHERE job_id = ?`, jobID)
	return nil
}

func nullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func isDuplicateColumnError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate column") || strings.Contains(msg, "already exists")
}
