package connector

import (
	"context"
	"fmt"

	"github.com/gerinsp/rivus/pkg/meta"
)

// ConfirmSnapshotCompletion belongs to the sink's successful data barrier,
// before sending ACK. A durable done receipt survives a lost ACK. The source
// clears its progress only after it observes acknowledgement and this receipt.
func ConfirmSnapshotCompletion(ctx context.Context, store meta.OffsetStore, key string) error {
	if store == nil {
		return nil
	}
	if intents, ok := store.(meta.IcebergCommitIntentStore); ok {
		pending, err := intents.ListIcebergCommits(ctx, key)
		if err != nil {
			return fmt.Errorf("read intents before snapshot completion: %w", err)
		}
		for _, intent := range pending {
			if intent.Status != meta.IcebergCommitCommitted {
				return fmt.Errorf("snapshot completion BLOCKED by unresolved commit %s", intent.OperationID)
			}
		}
	}
	return store.MarkSnapshotDone(ctx, key)
}
