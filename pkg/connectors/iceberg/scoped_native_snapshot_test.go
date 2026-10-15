package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type snapshotPayloadReleaseStore struct {
	*automaticStore
	beforeBegin func(meta.IcebergCommitIntent) error
}

func (s *snapshotPayloadReleaseStore) BeginIcebergCommit(ctx context.Context, intent meta.IcebergCommitIntent) error {
	if err := s.beforeBegin(intent); err != nil {
		return err
	}
	return s.automaticStore.BeginIcebergCommit(ctx, intent)
}

func TestScopedNativeBulkReleasesPayloadWithoutEarlyAcknowledgement(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		for _, failBegin := range []bool{false, true} {
			t.Run(fmt.Sprintf("filtered_%t_fail_%t", filtered, failBegin), func(t *testing.T) {
				ctx := context.Background()
				cat := recoveryCatalog(t)
				store := &automaticStore{guardTestStore: newGuardTestStore()}
				s := automaticSink(t, cat, store)
				s.scopedRebuildToken, s.scopedNativeWrites = "epoch", true
				s.cfg.SnapshotBatchSize = 1
				s.cfg.SnapshotSpoolDirectory = t.TempDir()
				if filtered {
					s.cfg.SnapshotReplaceFilters = map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "id", Op: ">=", Value: "1"}}
					s.cfg.TrinoDelete.Catalog = "fixture"
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						io.WriteString(w, `{"id":"lookup","data":[]}`)
					}))
					defer server.Close()
					s.cfg.TrinoDelete.URI = server.URL
				}
				state := s.states["app.orders"]
				state.snapshotReplaceApplied, state.snapshotTruncateApplied = true, true
				rows := []map[string]interface{}{{"id": int64(1), "status": strings.Repeat("payload", 10000)}, {"id": int64(2), "status": "second"}}
				if filtered {
					batch, err := s.scopedSnapshotReplacementKeys(ctx, state, rows)
					if err != nil || batch != nil {
						t.Fatalf("cleared target must append without equality deletes: batch=%+v error=%v", batch, err)
					}
				}
				ack := make(chan error, 1)
				visited := false
				s.offsetSto = &snapshotPayloadReleaseStore{automaticStore: store, beforeBegin: func(intent meta.IcebergCommitIntent) error {
					if intent.Kind != "snapshot" {
						return nil
					}
					visited = true
					for _, row := range rows {
						if row != nil {
							t.Error("source payload still retained when native write begins")
						}
					}
					if len(ack) != 0 || store.saveCount != 0 || store.snapshotProgress != nil {
						t.Error("staging acknowledged or checkpointed uncommitted data")
					}
					if failBegin {
						return errors.New("fixture metadata write failure")
					}
					return nil
				}}
				err := s.handleSnapshotBatch(ctx, model.Event{Schema: "app", Table: "orders", Rows: rows, Ack: ack})
				if !visited || (err != nil) != failBegin || (<-ack != nil) != failBegin {
					t.Fatalf("visited=%t error=%v expected failure=%t", visited, err, failBegin)
				}
				if failBegin {
					assertAutomaticData(t, cat, 0, 0)
				} else {
					assertAutomaticData(t, cat, 2, 1)
				}
			})
		}
	}
}

func TestScopedNativeSnapshotLargeRowAndPartialBatchRestart(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	s.scopedRebuildToken, s.scopedNativeWrites = "epoch", true
	s.cfg.TrinoDelete.Catalog = "fixture"
	state := s.states["app.orders"]
	state.snapshotReplaceApplied, state.snapshotTruncateApplied = true, true
	large := strings.Repeat("large-payload'", maxRebuildStatementBytes/8)
	if _, err := s.snapshotMergeTrinoQueries(state, []map[string]interface{}{{"id": int64(1), "status": large}}); err == nil || !strings.Contains(err.Error(), "bounded Trino statement size") {
		t.Fatal("fixture must exceed the old SQL row bound")
	}
	rows := []map[string]interface{}{{"id": int64(1), "status": large}, {"id": int64(2), "status": "second"}}
	chunk := withIcebergBatch(ctx, snapshotIcebergBatch("app.orders", 0, 1))
	if _, err := s.flushScopedNativeSnapshot(chunk, state, rows[:1]); err != nil {
		t.Fatal(err)
	}
	for _, intent := range store.intents {
		if !meta.IsScopedNativeSnapshotIntent(intent, "job", "state", "epoch") || intent.Status != meta.IcebergCommitCommitted {
			t.Fatalf("native receipt not epoch/range/identity bound: %+v", intent)
		}
	}
	// Source ACK/cursor was not saved. Restart with a different chunk size must
	// subtract the accepted range before writing the remaining row.
	restarted := automaticSink(t, cat, store)
	restarted.scopedRebuildToken, restarted.scopedNativeWrites = "epoch", true
	restarted.cfg.SnapshotBatchSize = 10000
	restarted.states["app.orders"].snapshotReplaceApplied, restarted.states["app.orders"].snapshotTruncateApplied = true, true
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if store.saveCount != 0 || store.snapshotProgress != nil {
		t.Fatal("proof advanced source checkpoint")
	}
	if err := restarted.handleSnapshotBatch(ctx, model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", Rows: rows}); err != nil {
		t.Fatal(err)
	}
	assertAutomaticData(t, cat, 2, 2)
}

func TestScopedTrinoExpiredHistoryRequiresPositiveMainSnapshot(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(1), "status": "accepted"}})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	tbl, err = commitAppend(ctx, tbl, reader, map[string]string{"trino_query_id": "original-query"})
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	intent := meta.IcebergCommitIntent{Kind: "rebuild", Operation: "trino-merge", SourceTable: "app.orders", ReplayKeys: digest, TargetUUID: tbl.Metadata().TableUUID().String(), TrinoQueryID: "original-query", BatchID: "rebuild:epoch:app.orders:merge:" + digest}
	if !provenTrinoMergeSnapshot(tbl, intent, "epoch") {
		t.Fatal("retained accepted RowDelta not recognized")
	}
	for _, fault := range []string{"query", "uuid", "epoch", "digest", "reset"} {
		changed := intent
		switch fault {
		case "query":
			changed.TrinoQueryID = "another-query"
		case "uuid":
			changed.TargetUUID = "replaced"
		case "epoch":
			changed.BatchID = "rebuild:another:app.orders:merge:" + digest
		case "digest":
			changed.ReplayKeys = "invalid"
		case "reset":
			changed.Operation = "trino-reset"
		}
		if provenTrinoMergeSnapshot(tbl, changed, "epoch") {
			t.Fatalf("accepted mismatched %s evidence", fault)
		}
	}
}

func TestScopedTrinoExpiredHistoryHTTPOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status       int
		missingProof bool
		wantSuccess  bool
	}{
		{http.StatusNotFound, false, true},
		{http.StatusGone, false, true},
		{http.StatusGone, true, false},
		{http.StatusNotFound, true, false},
		{http.StatusForbidden, false, false},
		{http.StatusInternalServerError, false, false},
	} {
		t.Run(fmt.Sprintf("status_%d_missing_%t", tc.status, tc.missingProof), func(t *testing.T) {
			ctx := context.Background()
			cat := recoveryCatalog(t)
			store := &automaticStore{guardTestStore: newGuardTestStore()}
			s := automaticSink(t, cat, store)
			s.scopedRebuildToken = "epoch"
			tbl := s.states["app.orders"].table
			reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(1), "status": "accepted"}})
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			tbl, err = commitAppend(ctx, tbl, reader, map[string]string{"trino_query_id": "original-query"})
			if err != nil {
				t.Fatal(err)
			}
			digest := strings.Repeat("a", 64)
			intent := meta.IcebergCommitIntent{OperationID: "op", JobID: "job", StateKey: "state", Status: meta.IcebergCommitPrepared, Kind: "rebuild", Operation: "trino-merge", SourceTable: "app.orders", TargetNamespace: "bronze", TargetTable: "orders", ReplayKeys: digest, TargetUUID: tbl.Metadata().TableUUID().String(), TrinoQueryID: "original-query", BatchID: "rebuild:epoch:app.orders:merge:" + digest}
			if tc.missingProof {
				intent.TrinoQueryID = "unproven-query"
			}
			store.intents[intent.OperationID] = intent
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("recovery attempted a mutation")
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			s.cfg.TrinoDelete.URI = server.URL
			err = s.confirmTrinoRebuildIntent(ctx, intent)
			if (err == nil) != tc.wantSuccess {
				t.Fatalf("confirmation error=%v success=%t", err, tc.wantSuccess)
			}
			if (store.intents["op"].Status == meta.IcebergCommitCommitted) != tc.wantSuccess {
				t.Fatal("incorrect durable outcome")
			}
			if store.saveCount != 0 {
				t.Fatal("commit proof advanced source checkpoint")
			}
		})
	}
}

func TestScopedNativeSnapshotRejectsInvalidKeysBeforeWrite(t *testing.T) {
	for _, rows := range [][]map[string]interface{}{
		{{"id": nil}}, {{"status": "missing"}}, {{"id": int64(1)}, {"id": int64(1)}},
	} {
		store := &automaticStore{guardTestStore: newGuardTestStore()}
		s := automaticSink(t, recoveryCatalog(t), store)
		s.scopedRebuildToken, s.scopedNativeWrites = "epoch", true
		s.cfg.SnapshotBatchSize = 1
		if err := s.handleSnapshotBatch(context.Background(), model.Event{Schema: "app", Table: "orders", Rows: rows}); err == nil {
			t.Fatal("invalid source keys admitted")
		}
		if len(store.intents) != 0 {
			t.Fatal("invalid keys caused partial writes")
		}
	}
}

func TestScopedNativeSnapshotLookupExcludesLargePayload(t *testing.T) {
	s := automaticSink(t, recoveryCatalog(t), &automaticStore{guardTestStore: newGuardTestStore()})
	s.cfg.TrinoDelete.Catalog = "fixture"
	queries, columns, err := s.scopedSnapshotLookupQueries(s.states["app.orders"], []map[string]interface{}{{"id": int64(1), "status": strings.Repeat("secret-payload", 100000)}})
	if err != nil || len(queries) != 1 || len(columns) != 1 || columns[0] != "id" {
		t.Fatalf("lookup=%v columns=%v error=%v", queries, columns, err)
	}
	if strings.Contains(queries[0], "MERGE") || strings.Contains(queries[0], "status") || strings.Contains(queries[0], "secret-payload") || len(queries[0]) > 200 {
		t.Fatal("key lookup transports row payload")
	}
}

func TestScopedNativeProtocolRemainsFencedAndNonRolling(t *testing.T) {
	// Actual factory binding is covered by the existing constructor tests.
	for _, mode := range []string{config.IcebergRebuildScopedWriteMode, config.IcebergRebuildScopedNativeWriteMode} {
		if _, err := rebuildSnapshotConfig(connector.JobContext{}, config.IcebergConfig{SnapshotWriteMode: mode}); err == nil {
			t.Fatal("unfenced protocol accepted")
		}
	}
}

func TestScopedNativeUnpartitionedDeletesOnlySurvivingKeys(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		for _, scenario := range []string{"cleared", "survivor", "legacy"} {
			t.Run(fmt.Sprintf("bulk_%t_%s", bulk, scenario), func(t *testing.T) {
				testScopedNativeUnpartitionedSurvivingKeys(t, bulk, scenario)
			})
		}
	}
}

func testScopedNativeUnpartitionedSurvivingKeys(t *testing.T, bulk bool, scenario string) {
	t.Helper()
	ctx := context.Background()
	cat := recoveryCatalog(t)
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	s.scopedRebuildToken, s.scopedNativeWrites = "epoch", true
	s.cfg.SnapshotBatchSize = 100
	if bulk {
		s.cfg.SnapshotBatchSize = 1
	}
	s.cfg.SnapshotSpoolDirectory = t.TempDir()
	s.cfg.TrinoDelete.Catalog = "fixture"
	if scenario == "legacy" {
		s.scopedLegacyMerges = map[string]bool{"app.orders": true}
	} else {
		s.cfg.SnapshotReplaceFilters = map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "id", Op: ">=", Value: "1"}}
	}
	state := s.states["app.orders"]
	state.snapshotReplaceApplied, state.snapshotTruncateApplied = true, true
	// Key 9 is retained outside this source batch. Key 1 survives only in the
	// replacement cases; all other incoming keys have already been cleared.
	seedRows := []map[string]interface{}{{"id": int64(9), "status": "retained"}}
	wantDeletes := int64(0)
	lookupData := [][]int64{}
	if scenario != "cleared" {
		seedRows = append(seedRows, map[string]interface{}{"id": int64(1), "status": "old"})
		lookupData = append(lookupData, []int64{1})
		wantDeletes = 1
	}
	seed, release, err := buildRecordReader(state.table.Schema(), seedRows)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	state.table, err = commitAppend(ctx, state.table, seed, nil)
	if err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || string(query) != `SELECT "id" FROM "fixture"."bronze"."orders" WHERE "id" IN (CAST(1 AS BIGINT),CAST(2 AS BIGINT),CAST(3 AS BIGINT))` {
			t.Errorf("unexpected key lookup: %s %s", r.Method, query)
		}
		reads.Add(1)
		json.NewEncoder(w).Encode(map[string]interface{}{"id": "lookup", "data": lookupData})
	}))
	defer server.Close()
	s.cfg.TrinoDelete.URI = server.URL
	rows := []map[string]interface{}{{"id": int64(1), "status": "updated"}, {"id": int64(2), "status": "new"}, {"id": int64(3), "status": "new"}}
	replayRows := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		replayRows[i] = cloneMap(row)
	}
	if err := s.handleSnapshotBatch(ctx, model.Event{Schema: "app", Table: "orders", Rows: rows}); err != nil {
		t.Fatal(err)
	}
	native := 0
	for _, i := range store.intents {
		if i.Kind == "snapshot" {
			native++
			if i.SnapshotStartOffset != 0 || i.SnapshotEndOffset != 3 || !meta.IsScopedNativeSnapshotIntent(i, "job", "state", "epoch") {
				t.Fatal("bulk receipt lost span/epoch")
			}
		}
	}
	if native != 1 || store.saveCount != 0 {
		t.Fatalf("native commits=%d checkpoint saves=%d", native, store.saveCount)
	}
	assertAutomaticData(t, cat, 4, 2)
	if reads.Load() != 1 {
		t.Fatalf("key lookups=%d, want one per source span", reads.Load())
	}
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deleteRecords := int64(0)
	seen := map[string]bool{}
	for _, task := range tasks {
		for _, file := range task.EqualityDeleteFiles {
			if !seen[file.FilePath()] {
				seen[file.FilePath()] = true
				deleteRecords += file.Count()
			}
		}
	}
	if deleteRecords != wantDeletes {
		t.Fatalf("equality delete records=%d, want only %d surviving keys", deleteRecords, wantDeletes)
	}
	_, records, err := tbl.Scan().ToArrowRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	visible := map[int64]string{}
	for record, err := range records {
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range arrowRecordBatchRows(record) {
			visible[row["id"].(int64)] = row["status"].(string)
		}
		record.Release()
	}
	if len(visible) != 4 || visible[1] != "updated" || visible[2] != "new" || visible[3] != "new" || visible[9] != "retained" {
		t.Fatalf("replacement lost rows or kept stale payloads: %v", visible)
	}
	restarted := automaticSink(t, cat, store)
	restarted.scopedRebuildToken, restarted.scopedNativeWrites = "epoch", true
	restarted.states["app.orders"].snapshotReplaceApplied, restarted.states["app.orders"].snapshotTruncateApplied = true, true
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleSnapshotBatch(ctx, model.Event{Schema: "app", Table: "orders", Rows: replayRows}); err != nil {
		t.Fatal(err)
	}
	assertAutomaticData(t, cat, 4, 2)
	if reads.Load() != 1 {
		t.Fatal("replayed accepted range repeated target lookup")
	}
}

func TestScopedNativeUnpartitionedLookupFailureDoesNotAppend(t *testing.T) {
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	cat := recoveryCatalog(t)
	s := automaticSink(t, cat, store)
	s.scopedRebuildToken, s.scopedNativeWrites = "epoch", true
	s.cfg.TrinoDelete.Catalog = "fixture"
	s.cfg.SnapshotReplaceFilters = map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "id", Op: ">=", Value: "1"}}
	state := s.states["app.orders"]
	state.snapshotReplaceApplied, state.snapshotTruncateApplied = true, true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	s.cfg.TrinoDelete.URI = server.URL
	ack := make(chan error, 1)
	if err := s.handleSnapshotBatch(context.Background(), model.Event{Schema: "app", Table: "orders", Rows: []map[string]interface{}{{"id": int64(1), "status": "new"}}, Ack: ack}); err == nil {
		t.Fatal("failed target lookup admitted append")
	}
	if <-ack == nil || len(store.intents) != 0 || store.saveCount != 0 || store.snapshotProgress != nil {
		t.Fatal("failed lookup acknowledged, committed or checkpointed source data")
	}
	assertAutomaticData(t, cat, 0, 0)
}

func TestSnapshotResetAppendPathsDoNotWriteEqualityDeletes(t *testing.T) {
	for _, scenario := range []string{"regular-full-reset", "regular-date-reset", "scoped-full-reset"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			cat := recoveryCatalog(t)
			store := &automaticStore{guardTestStore: newGuardTestStore()}
			s := automaticSink(t, cat, store)
			s.cfg.SnapshotWriteMode = snapshotWriteModeAuto
			s.cfg.SnapshotTruncateTables = []string{"app.*"}
			s.cfg.SnapshotTruncateAllowPatterns = true
			if scenario == "regular-date-reset" {
				s.cfg.SnapshotReplaceFilters = map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "id", Op: ">=", Value: "1"}}
			}
			if scenario == "scoped-full-reset" {
				s.scopedRebuildToken, s.scopedNativeWrites = "epoch", true
			}
			state := s.states["app.orders"]
			// Simulate the confirmed reset, before its replacement rows arrive.
			state.snapshotReplaceApplied, state.snapshotTruncateApplied = true, true
			if err := s.handleSnapshotBatch(ctx, model.Event{Schema: "app", Table: "orders", Rows: []map[string]interface{}{{"id": int64(1), "status": "new"}, {"id": int64(2), "status": "new"}}}); err != nil {
				t.Fatal(err)
			}
			assertAutomaticData(t, cat, 2, 1)
			inventory, err := scanActiveFileInventory(ctx, state.table, defaultNativeMaintenanceSettings())
			if err != nil || inventory.EqualityDeletes != 0 || inventory.PositionDeletes != 0 {
				t.Fatalf("reset-then-append introduced delete files: %+v error=%v", inventory, err)
			}
		})
	}
}
