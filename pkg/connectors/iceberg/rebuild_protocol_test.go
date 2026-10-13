package iceberg

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

func TestRebuildProtocolRequiresCurrentSubmissionAndDurableMetadata(t *testing.T) {
	for _, fault := range []string{"valid", "stale marker", "missing submission", "missing marker", "missing metadata", "ordinary mode with marker"} {
		t.Run(fault, func(t *testing.T) {
			ctx := connector.JobContext{SubmissionID: "current", Metadata: map[string]string{config.IcebergRebuildFullResetKey: "current"}, MetaStore: newGuardTestStore()}
			cfg := config.IcebergConfig{SnapshotWriteMode: config.IcebergRebuildSnapshotWriteMode}
			switch fault {
			case "stale marker":
				ctx.Metadata[config.IcebergRebuildFullResetKey] = "previous"
			case "missing submission":
				ctx.SubmissionID = ""
			case "missing marker":
				ctx.Metadata = nil
			case "missing metadata":
				ctx.MetaStore = nil
			case "ordinary mode with marker":
				cfg.SnapshotWriteMode = "truncate-append"
			}
			got, err := rebuildSnapshotConfig(ctx, cfg)
			if (err == nil) != (fault == "valid") {
				t.Fatalf("config=%+v error=%v", got, err)
			}
			if err == nil && got.SnapshotWriteMode != snapshotWriteModeTruncateAppend {
				t.Fatal("valid protocol was not translated")
			}
		})
	}
}
