package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

// A proven Iceberg commit still needs its source replay receipt until the
// checkpoint covers it. Reconciliation alone does not authorize a reset.
func (j *Job) checkFreshSnapshotRecovery(ctx context.Context, store meta.OffsetStore, key string) error {
	sinkType, _ := j.pickSink()
	if sinkType != "iceberg_native" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	blocked := func(err error) error {
		return fmt.Errorf("fresh snapshot BLOCKED job_id=%s: %w; preserve commit intents and checkpoint; resume requires the saved source binlog history, otherwise a controlled target rebuild is required; see docs/iceberg-checkpoint-recovery.md", j.Config.ID, err)
	}
	checker, ok := store.(meta.JobStateCleanupChecker)
	if !ok {
		return blocked(fmt.Errorf("metadata store cannot verify retained Iceberg intents"))
	}
	if err := meta.CheckOwnership(ctx, store); err != nil {
		return blocked(err)
	}
	// The public ID also covers intents from earlier configurations/state keys,
	// matching the protection used by legacy checkpoint cleanup.
	if err := checker.CheckJobStateCleanup(ctx, j.Config.ID); err != nil {
		return blocked(err)
	}
	if strings.TrimSpace(key) != "" && key != j.Config.ID {
		if err := checker.CheckJobStateCleanup(ctx, key); err != nil {
			return blocked(err)
		}
	}
	return meta.CheckOwnership(ctx, store)
}

func jobFreshSnapshotOnResubmit(job *Job) (bool, error) {
	if !jobCheckpointBinlogPurged(job) {
		return false, nil
	}
	sinkType, _ := job.pickSink()
	if sinkType == "iceberg_native" {
		store, key, err := job.checkpointReader()
		if err != nil {
			return false, err
		}
		if err := job.checkFreshSnapshotRecovery(context.Background(), store, key); err != nil {
			return false, fmt.Errorf("cannot resume job_id=%s: saved MySQL binlog has been purged; %w", job.Config.ID, err)
		}
	}
	return true, nil
}
