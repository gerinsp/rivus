package iceberg

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

type unfencedTrinoStore struct{ *guardTestStore }

func (*unfencedTrinoStore) RecordIcebergTrinoQuery(context.Context, string, string, string) error {
	return nil
}

func TestScopedRebuildProtocolRequiresSubmissionAndFencedQueryStore(t *testing.T) {
	for _, scenario := range []string{"valid", "missing-marker", "stale-marker", "missing-submission", "mixed-reset", "unfenced", "missing-query-store", "missing-protocol"} {
		t.Run(scenario, func(t *testing.T) {
			store := &trinoGuardStore{automaticStore: &automaticStore{guardTestStore: newGuardTestStore()}}
			jctx := connector.JobContext{SubmissionID: "current", Metadata: map[string]string{config.IcebergRebuildScopedKey: "current"}, MetaStore: store}
			cfg := config.IcebergConfig{SnapshotWriteMode: config.IcebergRebuildScopedWriteMode}
			switch scenario {
			case "missing-marker":
				delete(jctx.Metadata, config.IcebergRebuildScopedKey)
			case "stale-marker":
				jctx.Metadata[config.IcebergRebuildScopedKey] = "old"
			case "missing-submission":
				jctx.SubmissionID = ""
			case "mixed-reset":
				jctx.Metadata[config.IcebergRebuildFullResetKey] = "current"
			case "unfenced":
				jctx.MetaStore = &unfencedTrinoStore{newGuardTestStore()}
			case "missing-query-store":
				jctx.MetaStore = store.automaticStore
			case "missing-protocol":
				cfg.SnapshotWriteMode = "auto"
			}
			got, err := rebuildSnapshotConfig(jctx, cfg)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("protocol admission: %v", err)
			}
			if err == nil && (got.SnapshotWriteMode != "auto" || got.SnapshotRollingEnabled == nil || *got.SnapshotRollingEnabled) {
				t.Fatal("wrong scoped snapshot execution mode")
			}
		})
	}
}

func TestScopedRebuildQueryIdentityDoesNotChangeNativeIntentFingerprint(t *testing.T) {
	intent := meta.IcebergCommitIntent{OperationID: "legacy", JobID: "job", StateKey: "state", Kind: "cdc", Status: meta.IcebergCommitPrepared}
	encoded, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "TargetUUID") || strings.Contains(string(encoded), "TrinoQueryID") {
		t.Fatal("new empty fields changed legacy fingerprint input")
	}
	before := icebergIntentFingerprint(intent)
	intent.TrinoQueryID = "durable-query"
	if got := icebergIntentFingerprint(intent); got != before {
		t.Fatal("mutable query identity changed commit fingerprint")
	}
	intent.TargetUUID = "target-generation"
	if got := icebergIntentFingerprint(intent); got == before {
		t.Fatal("target generation not protected by intent fingerprint")
	}
}
