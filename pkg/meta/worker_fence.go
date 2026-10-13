package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	drivermysql "github.com/go-sql-driver/mysql"
)

var ErrWorkerOwnershipLost = errors.New("worker ownership lost")

type OwnershipChecker interface {
	CheckOwnership(context.Context) error
}

type WorkerOwnership struct {
	JobID, SubmissionID, Owner, StateKey string
}

type FencedOffsetStore struct {
	*MySQLOffsetStore
	ownership WorkerOwnership
}

type sqlRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *MySQLOffsetStore) sqlRunner() sqlRunner {
	if s.runner != nil {
		return s.runner
	}
	return s.db
}

func mysqlStoreTarget(dsn string) (string, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse metadata store target: %w", err)
	}
	return cfg.Net + ":" + cfg.Addr + "/" + cfg.DBName, nil
}

type WorkerOffsetFencer interface {
	FenceWorkerOffsets(OffsetStore, WorkerOwnership) (OffsetStore, error)
}

func (s *MySQLJobStore) FenceWorkerOffsets(base OffsetStore, ownership WorkerOwnership) (OffsetStore, error) {
	store, ok := base.(*MySQLOffsetStore)
	if !ok || s.target == "" || store.target != s.target {
		return nil, fmt.Errorf("Iceberg worker fencing requires job registry and checkpoints in the same MySQL endpoint/database")
	}
	if ownership.JobID == "" || ownership.SubmissionID == "" || ownership.Owner == "" || ownership.StateKey == "" {
		return nil, fmt.Errorf("incomplete worker ownership token")
	}
	return &FencedOffsetStore{MySQLOffsetStore: store, ownership: ownership}, nil
}

func CheckOwnership(ctx context.Context, store OffsetStore) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if checker, ok := store.(OwnershipChecker); ok {
		return checker.CheckOwnership(ctx)
	}
	return nil
}

func (s *FencedOffsetStore) check(ctx context.Context, runner sqlRunner, lock bool) error {
	query := `SELECT 1 FROM job_registry WHERE job_id=? AND submission_id=? AND lease_owner=?
	 AND lease_until >= UTC_TIMESTAMP(6) AND (desired_state='RUNNING' OR last_status='PAUSING')`
	if lock {
		query += " FOR UPDATE"
	}
	var owned int
	err := runner.QueryRowContext(ctx, query, s.ownership.JobID, s.ownership.SubmissionID, s.ownership.Owner).Scan(&owned)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrWorkerOwnershipLost
	}
	return err
}

func (s *FencedOffsetStore) CheckOwnership(ctx context.Context) error {
	return s.check(ctx, s.db, false)
}

// Lock the authoritative registry row and mutate checkpoint/intent in the
// same transaction. A check-before-write callback alone has a takeover race.
func (s *FencedOffsetStore) write(ctx context.Context, key string, fn func(*MySQLOffsetStore) error) error {
	if key != s.ownership.StateKey && key != s.ownership.JobID {
		return ErrWorkerOwnershipLost
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.check(ctx, tx, true); err != nil {
		return err
	}
	store := &MySQLOffsetStore{db: s.db, runner: tx}
	if err := fn(store); err != nil {
		return err
	}
	// A slow SQL operation may have outlived its lease. Roll back in that case.
	if err := s.check(ctx, tx, true); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *FencedOffsetStore) SaveOffset(ctx context.Context, key string, off Offset) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.SaveOffset(ctx, key, off) })
}
func (s *FencedOffsetStore) SaveSnapshotStart(ctx context.Context, key string, off Offset) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.SaveSnapshotStart(ctx, key, off) })
}
func (s *FencedOffsetStore) MarkSnapshotDone(ctx context.Context, key string) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.MarkSnapshotDone(ctx, key) })
}
func (s *FencedOffsetStore) SaveSnapshotProgress(ctx context.Context, key, table string, offset int64, cursor string) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error {
		return store.SaveSnapshotProgress(ctx, key, table, offset, cursor)
	})
}
func (s *FencedOffsetStore) ClearSnapshotProgress(ctx context.Context, key string) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.ClearSnapshotProgress(ctx, key) })
}
func (s *FencedOffsetStore) DeleteJobState(ctx context.Context, key string) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.DeleteJobState(ctx, key) })
}
func (s *FencedOffsetStore) BeginIcebergCommit(ctx context.Context, intent IcebergCommitIntent) error {
	if intent.JobID != s.ownership.JobID {
		return ErrWorkerOwnershipLost
	}
	return s.write(ctx, intent.StateKey, func(store *MySQLOffsetStore) error { return store.BeginIcebergCommit(ctx, intent) })
}
func (s *FencedOffsetStore) MarkIcebergCommitCommitted(ctx context.Context, key, id string) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.MarkIcebergCommitCommitted(ctx, key, id) })
}
func (s *FencedOffsetStore) DeleteIcebergCommit(ctx context.Context, key, id string) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.DeleteIcebergCommit(ctx, key, id) })
}

func (s *FencedOffsetStore) RecordIcebergTrinoQuery(ctx context.Context, key, id, queryID string) error {
	return s.write(ctx, key, func(store *MySQLOffsetStore) error { return store.RecordIcebergTrinoQuery(ctx, key, id, queryID) })
}
