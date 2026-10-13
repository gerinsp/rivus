#!/usr/bin/env bash
set -euo pipefail

# Build the deployed revision from Git, without copying the current worktree.
# Only test probes are added; production code remains the pinned baseline.
revision=ff300c01937e5bbd408aa268a4b0923b53e211c6
output=${1:?Usage: build-compatibility-baseline.sh /absolute/path/baseline.test}
case "$output" in /*) ;; *) printf 'Output must be an absolute path\n' >&2; exit 1 ;; esac
task_dir=$(mktemp -d "${TMPDIR:-/tmp}/rivus-compatibility-baseline.XXXXXX")
trap 'rm -rf "$task_dir"' EXIT
git archive "$revision" | tar -x -C "$task_dir"
cat > "$task_dir/pkg/connectors/iceberg/compatibility_probe_test.go" <<'GO'
//go:build integration

package iceberg

import (
    "context"
    "encoding/json"
    "errors"
    "os"
    "strings"
    "testing"

    "github.com/apache/iceberg-go/catalog/hadoop"
    "github.com/gerinsp/rivus/pkg/config"
    "github.com/gerinsp/rivus/pkg/meta"
)

func compatibilityInput(t *testing.T) recoveryWorkerInput {
    t.Helper()
    data, err := os.ReadFile(os.Getenv("RIVUS_RECOVERY_CHILD_INPUT"))
    if err != nil { t.Fatal(err) }
    var input recoveryWorkerInput
    if err := json.Unmarshal(data, &input); err != nil { t.Fatal(err) }
    return input
}

func TestCompatibilityOldRebuildModeRejected(t *testing.T) {
    for _, mode := range []string{"rebuild-truncate-append-v1", "rebuild-scoped-trino-v1"} {
        t.Run(mode, func(t *testing.T) {
            _, err := NewSink("compatibility", "", "", config.IcebergConfig{SnapshotWriteMode: mode}, config.RetryPolicy{}, nil, nil)
            if err == nil || !strings.Contains(err.Error(), "invalid iceberg snapshot_write_mode") { t.Fatalf("old worker did not reject rebuild before catalog discovery: %v", err) }
        })
    }
}

func TestCompatibilityOldVersion2ReceiptBlocked(t *testing.T) {
    input := compatibilityInput(t)
    store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
    if err != nil { t.Fatal(err) }; defer store.Close()
    cat, err := hadoop.NewCatalog("compatibility", input.Warehouse, nil)
    if err != nil { t.Fatal(err) }
    sink := integrationSink(t, cat, store, input)
    if err := sink.checkIcebergRecovery(context.Background()); err == nil || !strings.Contains(err.Error(), "CDC intent lacks stable source event identities") { t.Fatalf("old worker replayed an unsupported manifest: %v", err) }
}

func TestCompatibilityOldFenceRejectsNewOwner(t *testing.T) {
    data, err := os.ReadFile(os.Getenv("RIVUS_COMPATIBILITY_OWNERSHIP_INPUT"))
    if err != nil { t.Fatal(err) }
    var token meta.WorkerOwnership
    if err := json.Unmarshal(data, &token); err != nil { t.Fatal(err) }
    store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
    if err != nil { t.Fatal(err) }; defer store.Close()
    registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
    if err != nil { t.Fatal(err) }; defer registry.Close()
    fenced, err := registry.FenceWorkerOffsets(store, token)
    if err != nil { t.Fatal(err) }
    if err := fenced.SaveOffset(context.Background(), token.StateKey, meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}); !errors.Is(err, meta.ErrWorkerOwnershipLost) { t.Fatalf("old worker updated checkpoint after takeover: %v", err) }
}
GO
(
  cd "$task_dir"
  go test -c -tags=integration -o "$output" ./pkg/connectors/iceberg
)
printf '%s\n' "$revision" > "$output.revision"
