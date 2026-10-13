package core

import (
	"context"
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

type scopedRebuildSource struct{ *countResumeSource }

func (s *scopedRebuildSource) SnapshotFilterExpression(schema, table string) string {
	return "`created_at` >= '2026-09-01'"
}

type scopedRebuildSink struct {
	*countResumeSink
	scopedResets []string
}

func (s *scopedRebuildSink) ValidateSnapshotSourceTable(context.Context, string, string, string) error {
	return nil
}
func (s *scopedRebuildSink) ResetSnapshotSourceTable(_ context.Context, schema, table, expression string) error {
	s.scopedResets = append(s.scopedResets, schema+"."+table+":"+expression)
	return nil
}

func TestScopedRebuildPreflightResetsEmptySourcePreservesFilterAndRejectsSharedTarget(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "matching-filter", true: "shared-target"}[shared], func(t *testing.T) {
			cfg := newTestJobConfig("scoped-rebuild")
			cfg.Source = &config.ConnectorSpec{Type: "mysql", Config: map[string]any{"tables": []string{"app.empty", "app.orders"}, "table_configs": map[string]any{"app.*": map[string]any{"filter": "`created_at` >= '2026-09-01'"}}}}
			cfg.Sink = &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{"snapshot_write_mode": config.IcebergRebuildScopedWriteMode, "snapshot_replace_delete_executor": "trino", "snapshot_replace_filters": map[string]any{"app.*": map[string]any{"column": "created_at", "op": "gte", "value": "2026-09-01"}}, "trino_delete": map[string]any{"uri": "http://127.0.0.1:18080"}}}
			job := NewJob(cfg, connector.NewRegistry())
			cfg.Metadata = map[string]string{config.IcebergRebuildScopedKey: job.SubmissionID()}
			source := &scopedRebuildSource{&countResumeSource{tables: []connector.TableRef{{Schema: "app", Table: "empty"}, {Schema: "app", Table: "orders"}}, filtered: map[string]bool{"app.empty": true, "app.orders": true}, sourceCounts: map[string]int64{"app.empty": 0, "app.orders": 2}}}
			sink := &scopedRebuildSink{countResumeSink: &countResumeSink{}}
			if shared {
				sink.targetFor = map[string]string{"app.empty": "shared", "app.orders": "shared"}
			}
			err := job.preflight(context.Background(), source, sink, config.JobModeSnapshotHandoff)
			if shared {
				if err == nil || len(sink.scopedResets) != 0 || len(sink.resetTargets) != 0 {
					t.Fatalf("shared target was reset: err=%v scoped=%v full=%v", err, sink.scopedResets, sink.resetTargets)
				}
				return
			}
			if err != nil || len(sink.scopedResets) != 2 || len(sink.resetTargets) != 0 {
				t.Fatalf("scoped reset missing or full reset used: err=%v scoped=%v full=%v", err, sink.scopedResets, sink.resetTargets)
			}
			if err := job.preflight(context.Background(), source, sink, config.JobModeSnapshotHandoffResume); err != nil || len(sink.scopedResets) != 2 {
				t.Fatalf("resume repeated destructive reset: %v", err)
			}
		})
	}
}
