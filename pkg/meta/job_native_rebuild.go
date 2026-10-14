package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/google/uuid"
)

type JobNativeRebuildStore interface {
	UpgradeJobNativeRebuild(context.Context, OffsetStore, string, string, string) (*config.JobConfig, error)
}

// Upgrade only an inactive, unfinished scoped rebuild. Keep the same epoch,
// cursor, reset receipts and ambiguous outcomes. The next fenced worker must
// prove every retained PREPARED operation before it can read/ACK source rows.
func (s *MySQLJobStore) UpgradeJobNativeRebuild(ctx context.Context, offsets OffsetStore, id, submission, key string) (*config.JobConfig, error) {
	base, ok := offsets.(*MySQLOffsetStore)
	if !ok || base.target == "" || base.target != s.target {
		return nil, fmt.Errorf("native rebuild upgrade requires shared MySQL metadata")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var sid, stateKey, desired, status, owner, role, raw string
	if err := tx.QueryRowContext(ctx, `SELECT submission_id,COALESCE(meta_key,''),desired_state,last_status,COALESCE(lease_owner,''),execution_role,config_json FROM job_registry WHERE job_id=? FOR UPDATE`, id).Scan(&sid, &stateKey, &desired, &status, &owner, &role, &raw); err != nil {
		return nil, err
	}
	if sid != submission || stateKey != key || owner != "" || desired != "STOPPED" || role != "SNAPSHOT" || (status != "FAILED" && status != "STOPPED" && status != "PAUSED") {
		return nil, fmt.Errorf("native rebuild upgrade blocked by changed job or active owner: %w", ErrWorkerOwnershipLost)
	}
	var cfg config.JobConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	if cfg.Metadata[config.IcebergRebuildScopedKey] != sid || sid == "" || cfg.Sink == nil {
		return nil, fmt.Errorf("native rebuild upgrade requires the current scoped epoch")
	}
	mode := cfg.Sink.Config["snapshot_write_mode"]
	if mode != config.IcebergRebuildScopedWriteMode && mode != config.IcebergRebuildScopedNativeWriteMode {
		return nil, fmt.Errorf("native rebuild upgrade requires a supported scoped write protocol")
	}
	if err := config.ValidateIcebergSnapshotRebuild(&cfg); err != nil {
		return nil, err
	}
	reader := &MySQLOffsetStore{db: s.db, runner: tx}
	state, err := reader.GetSnapshotState(ctx, key)
	if err != nil {
		return nil, err
	}
	if state == nil || state.Done || state.StartFile == "" || state.StartPos == 0 {
		return nil, fmt.Errorf("native rebuild upgrade requires an unfinished snapshot")
	}
	intents, err := reader.ListIcebergCommitsForJob(ctx, key, id)
	if err != nil {
		return nil, err
	}
	for _, intent := range intents {
		validStatus := intent.Status == IcebergCommitCommitted || intent.Status == IcebergCommitPrepared
		valid := intent.JobID == id && intent.StateKey == key && intent.Kind == "rebuild" &&
			strings.HasPrefix(intent.BatchID, "rebuild:"+sid+":") && intent.TargetUUID != "" && intent.TrinoQueryID != "" &&
			(intent.Operation == "trino-reset" || intent.Operation == "trino-merge")
		if mode == config.IcebergRebuildScopedNativeWriteMode && IsScopedNativeSnapshotIntent(intent, id, key, sid) {
			valid = true
		}
		if !valid || !validStatus {
			return nil, fmt.Errorf("native rebuild upgrade blocked by unrelated or unidentifiable receipt operation=%s", intent.OperationID)
		}
	}
	if mode == config.IcebergRebuildScopedNativeWriteMode {
		return &cfg, nil
	}
	progress, err := reader.GetSnapshotProgress(ctx, key)
	if err != nil {
		return nil, err
	}
	offset, err := reader.GetOffset(ctx, key)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(struct {
		Config   json.RawMessage
		Snapshot *SnapshotState
		Progress *SnapshotProgress
		Offset   *Offset
		Intents  []IcebergCommitIntent
	}{json.RawMessage(raw), state, progress, offset, intents})
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO job_recovery_archives (archive_id,job_id,submission_id,state_key,reason,payload_json,created_at) VALUES (?,?,?,?, 'native_snapshot_writer_upgrade',?,UTC_TIMESTAMP(6))`, uuid.NewString(), id, sid, key, string(payload)); err != nil {
		return nil, err
	}
	cfg.Sink.Config["snapshot_write_mode"] = config.IcebergRebuildScopedNativeWriteMode
	cfg.Sink.Config["snapshot_rolling_enabled"] = false
	updated, err := json.Marshal(&cfg)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE job_registry SET config_json=?,updated_at=NOW() WHERE job_id=?`, updated, id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
