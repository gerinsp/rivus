package iceberg

import (
	"encoding/json"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScopedRebuildFactoryBindsSubmissionBeforeReconciliation(t *testing.T) {
	store := &trinoGuardStore{automaticStore: &automaticStore{guardTestStore: newGuardTestStore()}}
	seed := automaticSink(t, recoveryCatalog(t), store.automaticStore)
	tbl := seed.states["app.orders"].table
	query := `DELETE FROM "fixture"."bronze"."orders"`
	intent := meta.IcebergCommitIntent{OperationID: "constructor-op", JobID: "constructor-job", StateKey: "constructor-key", BatchID: "rebuild:current:app.orders:reset", Kind: "rebuild", Status: meta.IcebergCommitPrepared, Operation: "trino-reset", SourceTable: "app.orders", TargetNamespace: "bronze", TargetTable: "orders", TargetUUID: tbl.Metadata().TableUUID().String(), TrinoQueryID: "accepted", ReplayKeys: trinoStatementDigest(query)}
	store.intents[intent.OperationID] = intent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "GET" {
			t.Errorf("unexpected mutation %s %s", r.Method, r.URL.Path)
			w.WriteHeader(405)
			return
		}
		switch {
		case r.URL.Path == "/v1/config":
			json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
		case strings.HasSuffix(r.URL.Path, "/tables/orders"):
			json.NewEncoder(w).Encode(map[string]any{"metadata-location": tbl.MetadataLocation(), "metadata": tbl.Metadata()})
		case r.URL.Path == "/v1/query/accepted":
			json.NewEncoder(w).Encode(map[string]string{"queryId": "accepted", "state": "FINISHED", "query": trinoOperationComment(intent.OperationID) + query})
		default:
			t.Errorf("unexpected read %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rolling := false
	cfg := config.IcebergConfig{RestURI: server.URL, Warehouse: "fixture", SnapshotWriteMode: config.IcebergRebuildScopedWriteMode, SnapshotRollingEnabled: &rolling, TrinoDelete: config.IcebergTrinoDeleteConfig{URI: server.URL, Catalog: "fixture", User: "fixture"}, Overrides: map[string]config.IcebergTarget{"app.orders": {Namespace: "bronze", Table: "orders"}}}
	reg := connector.NewRegistry()
	Register(reg)
	sink, err := reg.NewSink("iceberg_native", connector.JobContext{JobID: intent.JobID, MetaKey: intent.StateKey, SubmissionID: "current", Metadata: map[string]string{config.IcebergRebuildScopedKey: "current"}, MetaStore: store, Retry: config.RetryPolicy{MaxAttempts: 1}}, cfg)
	if err != nil {
		t.Fatalf("actual factory failed startup reconciliation: %v", err)
	}
	if sink.(*Sink).scopedRebuildToken != "current" || store.intents[intent.OperationID].Status != meta.IcebergCommitCommitted {
		t.Fatal("factory did not recover accepted query with bound submission")
	}
}
