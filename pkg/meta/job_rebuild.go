package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/google/uuid"
)

type JobRebuildResult struct{ ArchiveID, SubmissionID string }

type JobRebuildStore interface {
	RequestJobRebuild(ctx context.Context, offsets OffsetStore, jobID, submissionID, stateKey string) (JobRebuildResult, error)
}

const jobRecoveryArchiveDDL = `CREATE TABLE IF NOT EXISTS job_recovery_archives (
	archive_id CHAR(36) NOT NULL PRIMARY KEY,
	job_id VARCHAR(255) NOT NULL,
	submission_id VARCHAR(64) NOT NULL,
	state_key VARCHAR(255) NOT NULL,
	reason VARCHAR(64) NOT NULL,
	payload_json LONGTEXT NOT NULL,
	created_at DATETIME(6) NOT NULL,
	INDEX idx_job_recovery_archive_job (job_id, created_at)
)`

// Only an explicit resubmit after a confirmed binlog purge calls this method.
// Archive, checkpoint reset, new submission fence and snapshot queueing share
// one transaction. PREPARED outcomes and workers that still hold a lease block
// the entire operation. Ordinary DeleteJobState retains its stricter guard.
func (s *MySQLJobStore) RequestJobRebuild(ctx context.Context, offsets OffsetStore, jobID, submissionID, stateKey string) (JobRebuildResult, error) {
	var result JobRebuildResult
	base, ok := offsets.(*MySQLOffsetStore)
	if !ok || base.target == "" || base.target != s.target {
		return result, fmt.Errorf("snapshot rebuild requires registry and checkpoints in the same MySQL database")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	var currentSubmission, currentKey, desired, status, owner, rawConfig, role string
	err = tx.QueryRowContext(ctx, `SELECT submission_id, COALESCE(meta_key,''), desired_state, last_status, COALESCE(lease_owner,''), config_json, execution_role FROM job_registry WHERE job_id=? FOR UPDATE`, jobID).Scan(&currentSubmission, &currentKey, &desired, &status, &owner, &rawConfig, &role)
	if err != nil {
		return result, err
	}
	if currentSubmission != submissionID || currentKey != stateKey || owner != "" || role == string(JobExecutionRoleAll) || desired != string(DesiredStateStopped) || (status != "FAILED" && status != "STOPPED" && status != "PAUSED") {
		return result, fmt.Errorf("snapshot rebuild BLOCKED: job changed or previous worker has not released ownership: %w", ErrWorkerOwnershipLost)
	}
	var cfg config.JobConfig
	if err := json.Unmarshal([]byte(rawConfig), &cfg); err != nil {
		return result, err
	}
	if err := config.ValidateIcebergSnapshotRebuild(&cfg); err != nil {
		return result, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT operation_id,state_key,status,job_id FROM iceberg_commit_intents WHERE state_key=? OR job_id=? FOR UPDATE`, stateKey, jobID)
	if err != nil {
		return result, err
	}
	var blocked error
	for rows.Next() {
		var operation, key, outcome, receiptJob string
		if err := rows.Scan(&operation, &key, &outcome, &receiptJob); err != nil {
			blocked = err
			break
		}
		if outcome != IcebergCommitCommitted {
			blocked = fmt.Errorf("snapshot rebuild BLOCKED operation=%s: commit outcome %s must be resolved before target reset", operation, outcome)
			break
		}
		if receiptJob != jobID || (key != stateKey && key != jobID) {
			blocked = fmt.Errorf("snapshot rebuild BLOCKED operation=%s: receipt belongs to another state key", operation)
			break
		}
	}
	if err := rows.Err(); blocked == nil {
		blocked = err
	}
	rows.Close()
	if blocked != nil {
		return result, blocked
	}
	reader := &MySQLOffsetStore{db: s.db, runner: tx}
	type archivedState struct {
		Key      string
		Offset   *Offset
		Snapshot *SnapshotState
		Progress *SnapshotProgress
		Intents  []IcebergCommitIntent
	}
	states := []archivedState{}
	keys := []string{stateKey}
	if jobID != stateKey {
		keys = append(keys, jobID)
	}
	for _, key := range keys {
		state := archivedState{Key: key}
		if state.Offset, err = reader.GetOffset(ctx, key); err != nil {
			return result, err
		}
		if state.Snapshot, err = reader.GetSnapshotState(ctx, key); err != nil {
			return result, err
		}
		if state.Progress, err = reader.GetSnapshotProgress(ctx, key); err != nil {
			return result, err
		}
		if state.Intents, err = reader.ListIcebergCommits(ctx, key); err != nil {
			return result, err
		}
		states = append(states, state)
	}
	payload, err := json.Marshal(struct {
		Config json.RawMessage
		States []archivedState
	}{json.RawMessage(rawConfig), states})
	if err != nil {
		return result, err
	}
	result = JobRebuildResult{ArchiveID: uuid.NewString(), SubmissionID: uuid.NewString()}
	if _, err = tx.ExecContext(ctx, `INSERT INTO job_recovery_archives (archive_id,job_id,submission_id,state_key,reason,payload_json,created_at) VALUES (?,?,?,?, 'purged_binlog_snapshot_rebuild',?,UTC_TIMESTAMP(6))`, result.ArchiveID, jobID, submissionID, stateKey, string(payload)); err != nil {
		return JobRebuildResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM iceberg_commit_intents WHERE state_key=? OR job_id=?`, stateKey, jobID); err != nil {
		return JobRebuildResult{}, err
	}
	for _, key := range keys {
		if err := deleteJobState(ctx, tx, key); err != nil {
			return JobRebuildResult{}, err
		}
	}
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	}
	cfg.Metadata[config.IcebergRebuildFullResetKey] = result.SubmissionID
	cfg.Sink.Config["snapshot_write_mode"] = config.IcebergRebuildSnapshotWriteMode
	rebuildConfig, err := json.Marshal(cfg)
	if err != nil {
		return JobRebuildResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE job_registry SET submission_id=?, config_json=?, desired_state='RUNNING', execution_role='SNAPSHOT', resume_requested=FALSE, last_status='QUEUED', lease_owner=NULL, lease_until=NULL, progress_json=NULL, updated_at=NOW() WHERE job_id=?`, result.SubmissionID, string(rebuildConfig), jobID); err != nil {
		return JobRebuildResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return JobRebuildResult{}, err
	}
	return result, nil
}
