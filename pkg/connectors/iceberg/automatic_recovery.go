package iceberg

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/util"
)

const recoveryTimeout = time.Minute
const recoveryAttemptTimeout = 15 * time.Second

func automaticRecoveryPolicy(policy config.RetryPolicy) *config.RetryPolicy {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 5
	}
	if policy.MaxAttempts > 20 {
		policy.MaxAttempts = 20
	}
	if policy.BaseBackoff <= 0 {
		policy.BaseBackoff = 500 * time.Millisecond
	}
	if policy.MaxBackoff <= 0 || policy.MaxBackoff > 10*time.Second {
		policy.MaxBackoff = 10 * time.Second
	}
	if policy.BaseBackoff > policy.MaxBackoff {
		policy.BaseBackoff = policy.MaxBackoff
	}
	return &policy
}

// Retry only verification and idempotent metadata transitions. The original
// catalog write callback is never invoked by this loop.
func (s *Sink) runAutomaticRecovery(ctx context.Context, operation string, verify func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, recoveryTimeout)
	defer cancel()
	policy := *automaticRecoveryPolicy(*s.recoveryPolicy)
	started := time.Now().UTC()
	attempts := 0
	checkpoint := "unknown"
	var last error
	report := func(status string) {
		op := operation
		if op == "" {
			op = s.recoveryOperation
		}
		message := ""
		if last != nil {
			message = last.Error()
		}
		log.Printf("[iceberg][job %s] recovery status=%s operation_id=%s attempts=%d started_at=%s checkpoint=%s last_error=%q", s.jobID, status, op, attempts, started.Format(time.RFC3339Nano), checkpoint, message)
		if s.progress != nil {
			s.progress(connector.ProgressInfo{Phase: "sink_recovery", Summary: "Iceberg recovery " + status, Detail: message,
				RecoveryStatus: status, RecoveryOperationID: op, RecoveryAttempts: attempts, RecoveryLastError: message, RecoveryCheckpoint: checkpoint, RecoveryStartedAt: started})
		}
	}
	announced := operation != ""
	if announced {
		report("RECOVERING")
	}
	wait := s.recoveryWait
	if wait == nil {
		wait = func(ctx context.Context, duration time.Duration) error {
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-timer.C:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	backoff := policy.BaseBackoff
	for attempts < policy.MaxAttempts {
		if err := ctx.Err(); err != nil {
			last = err
			break
		}
		attempts++
		attemptCtx, stop := context.WithTimeout(ctx, recoveryAttemptTimeout)
		last = meta.CheckOwnership(attemptCtx, s.offsetSto)
		if last == nil {
			if off, err := s.offsetSto.GetOffset(attemptCtx, s.checkpointKey()); err != nil {
				last = err
			} else if off != nil {
				checkpoint = fmt.Sprintf("%s:%d", off.BinlogFile, off.BinlogPos)
			} else {
				checkpoint = "none"
				if progress, err := s.offsetSto.GetSnapshotProgress(attemptCtx, s.checkpointKey()); err != nil {
					last = err
				} else if progress != nil {
					checkpoint = fmt.Sprintf("snapshot:%s:%d", progress.TableName, progress.NextOffset)
				}
			}
		}
		if last == nil {
			last = verify(attemptCtx)
		}
		if last == nil {
			last = meta.CheckOwnership(attemptCtx, s.offsetSto)
		}
		stop()
		if !announced && (last != nil || s.recoveryOperation != "") {
			announced = true
			report("RECOVERING")
		}
		if last == nil {
			if announced {
				report("RECOVERED")
			}
			return nil
		}
		report("RECOVERING")
		if errors.Is(last, meta.ErrWorkerOwnershipLost) || ctx.Err() != nil || attempts == policy.MaxAttempts {
			break
		}
		if err := wait(ctx, backoff); err != nil {
			last = err
			break
		}
		backoff = min(backoff*2, policy.MaxBackoff)
	}
	report("BLOCKED")
	if operation == "" {
		operation = s.recoveryOperation
	}
	return util.Permanent(fmt.Errorf("Iceberg recovery BLOCKED job=%s operation=%s attempts=%d; durable intent/checkpoint retained: %w", s.jobID, operation, attempts, last))
}

func (s *Sink) recoverUnknownCommit(ctx context.Context, intent meta.IcebergCommitIntent, progress commitProgress, original error) error {
	if s.recoveryPolicy == nil || progress.recoveredTable == nil || (!recoverableDataOperation(intent.Operation) && !recoverableMetadataOperation(intent.Operation)) {
		return original
	}
	// A live batch is still in memory and already consumed by the source. Only
	// validate replay identities here; do not install receipt skip counters that
	// would incorrectly wait for this batch to arrive a second time.
	validator := &Sink{}
	if err := validator.retainRecoveryReceipt(intent); err != nil {
		return util.Permanent(fmt.Errorf("Iceberg recovery BLOCKED: %w", err))
	}
	return s.runAutomaticRecovery(ctx, intent.OperationID, func(ctx context.Context) error {
		store, err := s.icebergIntentStore()
		if err != nil {
			return err
		}
		intents, err := store.ListIcebergCommits(ctx, intent.StateKey)
		if err != nil {
			return err
		}
		var durable *meta.IcebergCommitIntent
		for i := range intents {
			if intents[i].OperationID == intent.OperationID {
				durable = &intents[i]
				break
			}
		}
		if durable == nil || icebergIntentFingerprint(*durable) != icebergIntentFingerprint(intent) {
			return fmt.Errorf("durable intent missing or changed")
		}
		if durable.Status != meta.IcebergCommitPrepared && durable.Status != meta.IcebergCommitCommitted {
			return fmt.Errorf("invalid durable intent status %s", durable.Status)
		}
		tbl, err := s.loadProvenIcebergTable(ctx, *durable)
		if err != nil {
			return err
		}
		if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
			return err
		}
		if durable.Status == meta.IcebergCommitPrepared {
			if err := store.MarkIcebergCommitCommitted(ctx, intent.StateKey, intent.OperationID); err != nil {
				return err
			}
		}
		if intent.Kind == "internal" {
			if err := store.DeleteIcebergCommit(ctx, intent.StateKey, intent.OperationID); err != nil {
				return err
			}
		}
		if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
			return err
		}
		*progress.recoveredTable = tbl
		return nil
	})
}

func (s *Sink) reconcileAutomatically(ctx context.Context) error {
	if s.recoveryPolicy == nil || s.offsetSto == nil {
		return s.checkIcebergRecovery(ctx)
	}
	s.recoveryOperation = ""
	return s.runAutomaticRecovery(ctx, "", s.checkIcebergRecovery)
}
