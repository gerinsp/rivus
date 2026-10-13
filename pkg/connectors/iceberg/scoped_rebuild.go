package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/util"
	"github.com/google/uuid"
)

func trinoStatementDigest(statement string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(statement)))
}

func trinoOperationComment(id string) string { return "/* rivus.operation_id=" + id + " */\n" }

// A submitted mutation is never repeated to recover a lost response. The same
// durable commit-intent store and worker fence used by native commits protect it.
func (s *Sink) executeRebuildStatement(ctx context.Context, state *tableState, phase, query string) error {
	s.trinoRebuildMu.Lock()
	defer s.trinoRebuildMu.Unlock()
	if s.scopedRebuildToken == "" || state == nil || state.table == nil {
		return util.Permanent(fmt.Errorf("scoped rebuild has no submission or target identity"))
	}
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return util.Permanent(err)
	}
	store, err := s.icebergIntentStore()
	if err != nil {
		return util.Permanent(err)
	}
	queries, ok := s.offsetSto.(meta.IcebergTrinoQueryStore)
	if !ok {
		return util.Permanent(fmt.Errorf("scoped rebuild requires durable Trino query identities"))
	}
	batch := "rebuild:" + s.scopedRebuildToken + ":" + state.sourceKey + ":" + phase
	if phase != "reset" {
		batch += ":" + trinoStatementDigest(query)
	}
	intents, err := store.ListIcebergCommits(ctx, s.checkpointKey())
	if err != nil {
		return util.Permanent(err)
	}
	for _, intent := range intents {
		if intent.BatchID != batch {
			continue
		}
		if intent.Kind != "rebuild" || intent.Operation != "trino-"+phase || intent.JobID != s.jobID || intent.ReplayKeys != trinoStatementDigest(query) || intent.TargetNamespace != state.targetNamespace || intent.TargetTable != state.targetTable || intent.TargetUUID != state.table.Metadata().TableUUID().String() {
			return util.Permanent(fmt.Errorf("scoped rebuild intent changed operation=%s", intent.OperationID))
		}
		return s.confirmTrinoRebuildIntent(ctx, intent)
	}
	intent := meta.IcebergCommitIntent{OperationID: uuid.NewString(), JobID: s.jobID, StateKey: s.checkpointKey(), BatchID: batch,
		Kind: "rebuild", Status: meta.IcebergCommitPrepared, Operation: "trino-" + phase, SourceTable: state.sourceKey,
		TargetNamespace: state.targetNamespace, TargetTable: state.targetTable, TargetUUID: state.table.Metadata().TableUUID().String(), ReplayKeys: trinoStatementDigest(query)}
	return s.withCommitSlot(ctx, commitProgress{operation: intent.Operation, sourceKey: state.sourceKey, targetNamespace: state.targetNamespace, targetTable: state.targetTable}, func(ctx context.Context) error {
		if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
			return util.Permanent(err)
		}
		if err := store.BeginIcebergCommit(ctx, intent); err != nil {
			return util.Permanent(err)
		}
		_, err := s.execTrinoStatementTracked(ctx, trinoOperationComment(intent.OperationID)+query, state.targetNamespace, func(id string) error {
			return queries.RecordIcebergTrinoQuery(ctx, intent.StateKey, intent.OperationID, id)
		})
		if err == nil {
			var currentErr error
			current, currentErr := s.catalog.LoadTable(ctx, namespaceIdentifier(intent.TargetNamespace, intent.TargetTable))
			err = currentErr
			if err == nil && current.Metadata().TableUUID().String() != intent.TargetUUID {
				err = fmt.Errorf("Trino rebuild target generation changed operation=%s", intent.OperationID)
			}
		}
		if err == nil {
			err = meta.CheckOwnership(ctx, s.offsetSto)
		}
		if err == nil {
			err = store.MarkIcebergCommitCommitted(ctx, intent.StateKey, intent.OperationID)
		}
		if err != nil {
			return util.Permanent(fmt.Errorf("scoped Trino rebuild outcome UNKNOWN operation=%s; durable intent retained: %w", intent.OperationID, err))
		}
		return nil
	})
}

func (s *Sink) confirmTrinoRebuildIntent(ctx context.Context, intent meta.IcebergCommitIntent) error {
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return util.Permanent(err)
	}
	if s.scopedRebuildToken == "" || !strings.HasPrefix(intent.BatchID, "rebuild:"+s.scopedRebuildToken+":") || intent.JobID != s.jobID || intent.StateKey != s.checkpointKey() {
		return util.Permanent(fmt.Errorf("scoped Trino rebuild belongs to another submission"))
	}
	if (intent.Status != meta.IcebergCommitPrepared && intent.Status != meta.IcebergCommitCommitted) || intent.TrinoQueryID == "" {
		return util.Permanent(fmt.Errorf("Trino rebuild BLOCKED operation=%s: no durable query identity; absent evidence does not authorize replay", intent.OperationID))
	}
	tbl, err := s.catalog.LoadTable(ctx, namespaceIdentifier(intent.TargetNamespace, intent.TargetTable))
	if err != nil {
		return err
	}
	if tbl.Metadata().TableUUID().String() != intent.TargetUUID {
		return fmt.Errorf("Trino rebuild target generation changed operation=%s", intent.OperationID)
	}
	dot := strings.LastIndexByte(intent.SourceTable, '.')
	if dot < 1 {
		return fmt.Errorf("Trino rebuild source identity missing")
	}
	namespace, target := s.ResolveTarget(intent.SourceTable[:dot], intent.SourceTable[dot+1:])
	if namespace != intent.TargetNamespace || target != intent.TargetTable {
		return fmt.Errorf("Trino rebuild target mapping changed")
	}
	if intent.Status == meta.IcebergCommitCommitted {
		return meta.CheckOwnership(ctx, s.offsetSto)
	}
	// QueryInfo is authoritative execution state, unlike diagnostic statement
	// stats. Require exact query text and the UUID of the original target.
	endpoint := strings.TrimRight(s.cfg.TrinoDelete.URI, "/") + "/v1/query/" + url.PathEscape(intent.TrinoQueryID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	s.setTrinoHeaders(req, intent.TargetNamespace)
	response, err := s.trinoHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Trino query outcome unavailable operation=%s query=%s status=%d", intent.OperationID, intent.TrinoQueryID, response.StatusCode)
	}
	var info struct{ QueryID, State, Query string }
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		return err
	}
	comment := trinoOperationComment(intent.OperationID)
	if info.QueryID != intent.TrinoQueryID || info.State != "FINISHED" || !strings.HasPrefix(info.Query, comment) || trinoStatementDigest(strings.TrimPrefix(info.Query, comment)) != intent.ReplayKeys {
		return fmt.Errorf("Trino query lacks matching terminal success operation=%s", intent.OperationID)
	}
	tbl, err = s.catalog.LoadTable(ctx, namespaceIdentifier(intent.TargetNamespace, intent.TargetTable))
	if err != nil {
		return err
	}
	if tbl.Metadata().TableUUID().String() != intent.TargetUUID {
		return fmt.Errorf("Trino rebuild target generation changed operation=%s", intent.OperationID)
	}
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return util.Permanent(err)
	}
	store, err := s.icebergIntentStore()
	if err != nil {
		return err
	}
	if err := store.MarkIcebergCommitCommitted(ctx, intent.StateKey, intent.OperationID); err != nil {
		// A response can be lost after the idempotent state transition, or a
		// concurrent startup/preflight verifier may have confirmed it already.
		intents, readErr := store.ListIcebergCommits(ctx, intent.StateKey)
		if readErr == nil {
			for _, durable := range intents {
				if durable.OperationID == intent.OperationID && durable.Status == meta.IcebergCommitCommitted && icebergIntentFingerprint(durable) == icebergIntentFingerprint(intent) && durable.TrinoQueryID == intent.TrinoQueryID {
					return meta.CheckOwnership(ctx, s.offsetSto)
				}
			}
		}
		return err
	}
	return nil
}
