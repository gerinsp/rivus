package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	iceberglib "github.com/apache/iceberg-go"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

type trinoGuardStore struct {
	*automaticStore
	queryErr error
}

func TestScopedRebuildMergeSQLUsesPKWithoutFilterAndPreservesValues(t *testing.T) {
	s := automaticSink(t, recoveryCatalog(t), &automaticStore{guardTestStore: newGuardTestStore()})
	state := s.states["app.orders"]
	s.cfg.TrinoDelete.Catalog = "fixture"
	queries, err := s.snapshotMergeTrinoQueries(state, []map[string]interface{}{{"id": int64(1), "status": "O'Brien"}, {"id": int64(2), "status": nil}})
	if err != nil || len(queries) != 1 {
		t.Fatalf("queries=%d error=%v", len(queries), err)
	}
	for _, want := range []string{`MERGE INTO "fixture"."bronze"."orders"`, `t."id" = s."id"`, `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`, `'O''Brien'`, `CAST(NULL AS VARCHAR)`} {
		if !strings.Contains(queries[0], want) {
			t.Fatalf("merge missing %s: %s", want, queries[0])
		}
	}
	if strings.Contains(queries[0], "WHERE") {
		t.Fatal("PK match restricted to cutoff; moved row could be duplicated")
	}
	if _, err := s.snapshotMergeTrinoQueries(state, []map[string]interface{}{{"id": int64(1)}, {"id": int64(1)}}); err == nil {
		t.Fatal("duplicate source PK accepted")
	}
	if _, err := s.snapshotMergeTrinoQueries(state, []map[string]interface{}{{"id": nil}}); err == nil {
		t.Fatal("NULL primary key accepted")
	}
	literal, err := trinoRebuildLiteral(iceberglib.PrimitiveTypes.Timestamp, time.Date(2026, 9, 1, 1, 2, 3, 456789000, time.UTC))
	if err != nil || !strings.Contains(literal, "03.456789") {
		t.Fatalf("timestamp precision lost: %s %v", literal, err)
	}
}

func (s *trinoGuardStore) RecordIcebergTrinoQuery(ctx context.Context, key, id, queryID string) error {
	if err := s.CheckOwnership(ctx); err != nil {
		return err
	}
	if s.queryErr != nil {
		return s.queryErr
	}
	intent, ok := s.intents[id]
	if !ok || intent.StateKey != key || intent.Status != meta.IcebergCommitPrepared || (intent.TrinoQueryID != "" && intent.TrinoQueryID != queryID) {
		return errors.New("query identity changed")
	}
	intent.TrinoQueryID = queryID
	s.intents[id] = intent
	return nil
}

func TestScopedRebuildResetBarrierRejectsChangedIdentity(t *testing.T) {
	for _, scenario := range []string{"valid", "changed-target", "foreign-submission", "foreign-job", "changed-mapping", "missing-query", "prepared", "changed-reset-scope"} {
		t.Run(scenario, func(t *testing.T) {
			store := &automaticStore{guardTestStore: newGuardTestStore()}
			s := automaticSink(t, recoveryCatalog(t), store)
			s.scopedRebuildToken = "current"
			s.cfg.TrinoDelete.Catalog = "fixture"
			state := s.states["app.orders"]
			intent := meta.IcebergCommitIntent{OperationID: "reset", JobID: s.jobID, StateKey: s.checkpointKey(), BatchID: "rebuild:current:app.orders:reset", Kind: "rebuild", Operation: "trino-reset", Status: meta.IcebergCommitCommitted, SourceTable: "app.orders", TargetNamespace: "bronze", TargetTable: "orders", TargetUUID: state.table.Metadata().TableUUID().String(), TrinoQueryID: "accepted"}
			query, err := s.snapshotTruncateTrinoSQL(state)
			if err != nil {
				t.Fatal(err)
			}
			intent.ReplayKeys = trinoStatementDigest(query)
			switch scenario {
			case "changed-target":
				intent.TargetUUID = "another-generation"
			case "foreign-submission":
				intent.BatchID = "rebuild:old:app.orders:reset"
			case "foreign-job":
				intent.JobID = "another-job"
			case "changed-mapping":
				intent.TargetTable = "different"
			case "missing-query":
				intent.TrinoQueryID = ""
			case "prepared":
				intent.Status = meta.IcebergCommitPrepared
			case "changed-reset-scope":
				s.cfg.SnapshotReplaceFilters = map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "id", Op: ">=", Value: "2"}}
			}
			store.intents[intent.OperationID] = intent
			err = s.confirmScopedResetBarrier(context.Background())
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("unsafe reset barrier: %v", err)
			}
			if store.saveCount != 0 {
				t.Fatal("barrier advanced checkpoint")
			}
		})
	}
}

func TestScopedRebuildTrinoDurableOutcomeAndRestart(t *testing.T) {
	for _, scenario := range []string{"success", "lost-submit-response", "lost-poll-response", "mark-failed", "query-id-save-failed", "ownership-lost", "missing-query-proof", "changed-statement", "missing-query-id"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			store := &trinoGuardStore{automaticStore: &automaticStore{guardTestStore: newGuardTestStore()}}
			base := recoveryCatalog(t)
			s := automaticSink(t, base, store.automaticStore)
			s.offsetSto = store
			s.scopedRebuildToken = "submission"
			state := s.states["app.orders"]
			var posts, reads atomic.Int64
			var submitted atomic.Value
			submitted.Store("")
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
					posts.Add(1)
					body, _ := io.ReadAll(r.Body)
					submitted.Store(string(body))
					if len(store.intents) != 1 {
						t.Error("Trino write started without its durable PREPARED intent")
					}
					if scenario == "lost-submit-response" {
						conn, _, _ := w.(http.Hijacker).Hijack()
						conn.Close()
						return
					}
					if scenario == "ownership-lost" {
						store.ownershipErr = meta.ErrWorkerOwnershipLost
					}
					if scenario == "missing-query-id" {
						io.WriteString(w, `{}`)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"id": "query_1", "nextUri": server.URL + "/poll", "stats": map[string]string{"state": "RUNNING"}})
				case r.Method == http.MethodGet && r.URL.Path == "/poll":
					for _, intent := range store.intents {
						if intent.TrinoQueryID != "query_1" {
							t.Error("query was polled before its identity was durable")
						}
					}
					if scenario == "lost-poll-response" || scenario == "missing-query-proof" {
						conn, _, _ := w.(http.Hijacker).Hijack()
						conn.Close()
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"id": "query_1", "stats": map[string]string{"state": "FINISHED"}})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/query/query_1":
					reads.Add(1)
					if scenario == "missing-query-proof" {
						http.Error(w, "query history expired", http.StatusNotFound)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"queryId": "query_1", "state": "FINISHED", "query": submitted.Load().(string)})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected", 500)
				}
			}))
			defer server.Close()
			s.cfg.TrinoDelete = config.IcebergTrinoDeleteConfig{URI: server.URL, User: "fixture", Catalog: "fixture"}
			if scenario == "mark-failed" {
				store.markErr = errors.New("metadata mark unavailable")
			}
			if scenario == "query-id-save-failed" {
				store.queryErr = errors.New("metadata query identity unavailable")
			}
			query := `DELETE FROM "fixture"."bronze"."orders" WHERE "id" >= 100`
			err := s.executeRebuildStatement(ctx, state, "reset", query)
			if (err == nil) != (scenario == "success" || scenario == "changed-statement") {
				t.Fatalf("first outcome: %v", err)
			}
			if posts.Load() != 1 || store.saveCount != 0 || len(store.intents) != 1 {
				t.Fatalf("unsafe writes/checkpoint posts=%d saved=%d intents=%d", posts.Load(), store.saveCount, len(store.intents))
			}
			store.markErr, store.queryErr, store.ownershipErr = nil, nil, nil
			restarted := automaticSink(t, base, store.automaticStore)
			restarted.offsetSto, restarted.scopedRebuildToken, restarted.cfg.TrinoDelete = store, "submission", s.cfg.TrinoDelete
			if scenario == "changed-statement" {
				query = strings.Replace(query, ">= 100", ">= 200", 1)
			}
			err = restarted.executeRebuildStatement(ctx, restarted.states["app.orders"], "reset", query)
			blocked := scenario == "lost-submit-response" || scenario == "query-id-save-failed" || scenario == "ownership-lost" || scenario == "missing-query-proof" || scenario == "changed-statement" || scenario == "missing-query-id"
			if (err != nil) != blocked || posts.Load() != 1 || store.saveCount != 0 {
				t.Fatalf("restart outcome=%v posts=%d checkpoint saves=%d", err, posts.Load(), store.saveCount)
			}
			if scenario == "lost-poll-response" || scenario == "mark-failed" {
				if reads.Load() != 1 {
					t.Fatalf("restart did not verify query outcome: reads=%d", reads.Load())
				}
			}
			for _, intent := range store.intents {
				if !blocked && intent.Status != meta.IcebergCommitCommitted {
					t.Fatal("confirmed statement lacks durable COMMITTED status")
				}
			}
		})
	}
}
