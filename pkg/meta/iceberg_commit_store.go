package meta

import (
	"context"
	"fmt"
)

const (
	IcebergCommitPrepared  = "PREPARED"
	IcebergCommitCommitted = "COMMITTED"
)

// IcebergCommitIntent records one catalog operation until the corresponding
// source checkpoint durably covers it. PREPARED includes uncertain outcomes.
type IcebergCommitIntent struct {
	OperationID         string
	SourceTable         string
	TargetNamespace     string
	TargetTable         string
	Operation           string
	ReplayKeys          string
	JobID               string
	StateKey            string
	BatchID             string
	Kind                string
	SourceStartFile     string
	SourceStartPos      uint32
	SourceEndFile       string
	SourceEndPos        uint32
	SnapshotTable       string
	SnapshotStartOffset int64
	SnapshotEndOffset   int64
	Status              string
}

type IcebergCommitIntentStore interface {
	BeginIcebergCommit(context.Context, IcebergCommitIntent) error
	MarkIcebergCommitCommitted(ctx context.Context, stateKey, operationID string) error
	DeleteIcebergCommit(ctx context.Context, stateKey, operationID string) error
	ListIcebergCommits(ctx context.Context, stateKey string) ([]IcebergCommitIntent, error)
}

func (s *MySQLOffsetStore) BeginIcebergCommit(ctx context.Context, intent IcebergCommitIntent) error {
	const stmt = `INSERT INTO iceberg_commit_intents
		(operation_id, job_id, state_key, batch_id, kind, source_start_file, source_start_pos,
		 source_end_file, source_end_pos, snapshot_table, snapshot_start_offset, snapshot_end_offset, source_table, target_namespace, target_table, operation, replay_keys, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PREPARED')`
	_, err := s.sqlRunner().ExecContext(ctx, stmt, intent.OperationID, intent.JobID, intent.StateKey, intent.BatchID,
		intent.Kind, intent.SourceStartFile, intent.SourceStartPos, intent.SourceEndFile, intent.SourceEndPos,
		intent.SnapshotTable, intent.SnapshotStartOffset, intent.SnapshotEndOffset, intent.SourceTable, intent.TargetNamespace, intent.TargetTable, intent.Operation, intent.ReplayKeys)
	return err
}

func (s *MySQLOffsetStore) MarkIcebergCommitCommitted(ctx context.Context, stateKey, operationID string) error {
	result, err := s.sqlRunner().ExecContext(ctx, `UPDATE iceberg_commit_intents SET status = 'COMMITTED' WHERE state_key = ? AND operation_id = ? AND status = 'PREPARED'`, stateKey, operationID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("Iceberg commit intent %s state=%s cannot be marked committed: rows=%d", operationID, stateKey, count)
	}
	return nil
}

func (s *MySQLOffsetStore) DeleteIcebergCommit(ctx context.Context, stateKey, operationID string) error {
	_, err := s.sqlRunner().ExecContext(ctx, `DELETE FROM iceberg_commit_intents WHERE state_key = ? AND operation_id = ?`, stateKey, operationID)
	return err
}

func (s *MySQLOffsetStore) ListIcebergCommits(ctx context.Context, stateKey string) ([]IcebergCommitIntent, error) {
	const q = `SELECT operation_id, job_id, state_key, batch_id, kind, source_start_file,
		source_start_pos, source_end_file, source_end_pos, snapshot_table,
		snapshot_start_offset, snapshot_end_offset, status, source_table, target_namespace, target_table, operation, COALESCE(replay_keys, '')
		FROM iceberg_commit_intents WHERE state_key = ? ORDER BY created_at, operation_id`
	rows, err := s.sqlRunner().QueryContext(ctx, q, stateKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var intents []IcebergCommitIntent
	for rows.Next() {
		var intent IcebergCommitIntent
		if err := rows.Scan(&intent.OperationID, &intent.JobID, &intent.StateKey, &intent.BatchID,
			&intent.Kind, &intent.SourceStartFile, &intent.SourceStartPos, &intent.SourceEndFile,
			&intent.SourceEndPos, &intent.SnapshotTable, &intent.SnapshotStartOffset,
			&intent.SnapshotEndOffset, &intent.Status, &intent.SourceTable, &intent.TargetNamespace, &intent.TargetTable, &intent.Operation, &intent.ReplayKeys); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}
