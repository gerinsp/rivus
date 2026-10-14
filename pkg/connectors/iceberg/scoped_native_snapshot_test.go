package iceberg

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

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
