package core

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

func TestSinkRuntimeProgressKeepsSourcePhase(t *testing.T) {
	job := NewJob(&config.JobConfig{ID: "job-1"}, nil)
	job.updateProgress(connector.ProgressInfo{
		Phase:             "snapshot",
		Summary:           "Loading snapshot table 1/2",
		CurrentTable:      "app.orders",
		CurrentTableIndex: 1,
		TotalTables:       2,
		CurrentTableRows:  40000,
	})

	job.updateProgress(connector.ProgressInfo{
		Phase:            "sink_commit",
		Summary:          "Committing Iceberg overwrite",
		Detail:           "source=app.orders | target=orders_bronze.orders",
		CurrentTable:     "app.orders",
		CurrentTableRows: 10000,
	})

	progress := job.Progress()
	if progress == nil {
		t.Fatal("progress is nil")
	}
	if got, want := progress.Phase, "snapshot"; got != want {
		t.Fatalf("phase = %q, want %q", got, want)
	}
	if got, want := progress.Summary, "Loading snapshot table 1/2"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got, want := progress.SinkSummary, "Committing Iceberg overwrite"; got != want {
		t.Fatalf("sink summary = %q, want %q", got, want)
	}
	if got, want := progress.CurrentTableIndex, 1; got != want {
		t.Fatalf("current table index = %d, want %d", got, want)
	}
	if got, want := progress.TotalTables, 2; got != want {
		t.Fatalf("total tables = %d, want %d", got, want)
	}
	if got, want := progress.CurrentTableRows, int64(40000); got != want {
		t.Fatalf("current table rows = %d, want %d", got, want)
	}
	if got, want := progress.SinkRows, int64(10000); got != want {
		t.Fatalf("sink rows = %d, want %d", got, want)
	}
}
