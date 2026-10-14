package meta

import (
	"context"
	"fmt"
	"strings"
)

// Non-mutating admission for the current rebuild epoch. Ordinary cleanup is
// never relaxed: a sink must reconcile PREPARED statements before any source ACK.
func CheckScopedRebuildIntents(ctx context.Context, store OffsetStore, jobID, key, submission string, allowPrepared bool, native ...bool) error {
	if submission == "" {
		return fmt.Errorf("scoped rebuild submission missing")
	}
	if err := CheckOwnership(ctx, store); err != nil {
		return err
	}
	intents, ok := store.(IcebergJobCommitIntentStore)
	if !ok {
		return fmt.Errorf("scoped rebuild requires durable intents")
	}
	{
		rows, err := intents.ListIcebergCommitsForJob(ctx, key, jobID)
		if err != nil {
			return err
		}
		for _, intent := range rows {
			valid := intent.Status == IcebergCommitCommitted || (allowPrepared && intent.Status == IcebergCommitPrepared)
			if len(native) > 0 && native[0] && valid && IsScopedNativeSnapshotIntent(intent, jobID, key, submission) {
				continue
			}
			if intent.JobID != jobID || intent.StateKey != key || intent.Kind != "rebuild" || !strings.HasPrefix(intent.BatchID, "rebuild:"+submission+":") || !valid {
				return fmt.Errorf("scoped rebuild startup BLOCKED by unrelated or unresolved intent operation=%s", intent.OperationID)
			}
			if !allowPrepared && (intent.TrinoQueryID == "" || intent.TargetUUID == "") {
				return fmt.Errorf("scoped rebuild reset confirmation incomplete")
			}
		}
	}
	return CheckOwnership(ctx, store)
}

func ScopedNativeSnapshotBatchID(submission, table string, start, end int64) string {
	return fmt.Sprintf("rebuild:%s:%s:native:%d-%d", submission, table, start, end)
}

func IsScopedNativeSnapshotIntent(intent IcebergCommitIntent, jobID, key, submission string) bool {
	return submission != "" && intent.JobID == jobID && intent.StateKey == key && intent.Kind == "snapshot" &&
		(intent.Operation == "append" || intent.Operation == "delete-append-equality") && intent.TargetUUID != "" &&
		intent.SourceTable == intent.SnapshotTable && intent.SnapshotStartOffset >= 0 && intent.SnapshotEndOffset > intent.SnapshotStartOffset &&
		intent.BatchID == ScopedNativeSnapshotBatchID(submission, intent.SnapshotTable, intent.SnapshotStartOffset, intent.SnapshotEndOffset)
}

func CheckScopedRebuildEmptyCheckpoint(ctx context.Context, store OffsetStore, jobID, key string) error {
	keys := []string{key}
	if key != jobID {
		keys = append(keys, jobID)
	}
	for _, stateKey := range keys {
		offset, err := store.GetOffset(ctx, stateKey)
		if err != nil {
			return err
		}
		snapshot, err := store.GetSnapshotState(ctx, stateKey)
		if err != nil {
			return err
		}
		progress, err := store.GetSnapshotProgress(ctx, stateKey)
		if err != nil {
			return err
		}
		if offset != nil || snapshot != nil || progress != nil {
			return fmt.Errorf("scoped rebuild cannot replace existing checkpoint/start/progress")
		}
	}
	return CheckOwnership(ctx, store)
}
