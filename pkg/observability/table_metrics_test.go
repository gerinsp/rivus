package observability

import (
	"strings"
	"testing"
	"time"
)

func TestTableMetricsRecordsActivityAndPrometheusOutput(t *testing.T) {
	resetTableMetricsForTest()

	RegisterSourceTable("job-1", "App.Reservasi")
	SetSinkType("job-1", "app.reservasi", "iceberg_native")
	SetTargetTable("job-1", "app.reservasi", "bronze.reservasi")
	RecordMySQLCDC("job-1", "app", "reservasi", "insert", 3)
	RecordSinkFlush("job-1", "app.reservasi", "bronze.reservasi", "stream", "delete-append", 2, 2, 2, 25*time.Millisecond)
	RecordIcebergWrite("job-1", "app.reservasi", "bronze.reservasi", "delete-append", "success", 2, 2, 1500*time.Millisecond)
	RecordIcebergTableSnapshot("job-1", "app.reservasi", "bronze.reservasi", 42, 1200, 7340032)
	RegisterSourceTable("job-2", "App.Invoice")
	SetSinkType("job-2", "app.invoice", "doris")

	activities := TableActivities()
	if len(activities) != 2 {
		t.Fatalf("activities length = %d, want 2", len(activities))
	}
	icebergActivities := TableActivitiesBySink("iceberg")
	if len(icebergActivities) != 1 {
		t.Fatalf("iceberg activities length = %d, want 1", len(icebergActivities))
	}
	dorisActivities := TableActivitiesBySink("doris")
	if len(dorisActivities) != 1 {
		t.Fatalf("doris activities length = %d, want 1", len(dorisActivities))
	}
	got := icebergActivities[0]
	if got.SinkType != "iceberg" {
		t.Fatalf("sink type = %q, want iceberg", got.SinkType)
	}
	if got.SourceTable != "app.reservasi" {
		t.Fatalf("source table = %q, want app.reservasi", got.SourceTable)
	}
	if got.TargetTable != "bronze.reservasi" {
		t.Fatalf("target table = %q, want bronze.reservasi", got.TargetTable)
	}
	if got.CDCRows["insert"] != 3 {
		t.Fatalf("cdc insert rows = %d, want 3", got.CDCRows["insert"])
	}
	if got.SinkRows["stream|delete_append"] != 2 {
		t.Fatalf("sink rows = %d, want 2", got.SinkRows["stream|delete_append"])
	}
	if got.IcebergWriteDurationMS["delete_append|success"] != 1500 {
		t.Fatalf("write duration ms = %d, want 1500", got.IcebergWriteDurationMS["delete_append|success"])
	}
	if got.IcebergTotalRecords != 1200 {
		t.Fatalf("iceberg total records = %d, want 1200", got.IcebergTotalRecords)
	}
	if got.IcebergTotalSizeBytes != 7340032 {
		t.Fatalf("iceberg total size bytes = %d, want 7340032", got.IcebergTotalSizeBytes)
	}

	var out strings.Builder
	WritePrometheus(&out)
	text := out.String()
	for _, want := range []string{
		`rivus_table_cdc_rows_total{action="insert",job_id="job-1",sink_type="iceberg",source_table="app.reservasi",target_table="bronze.reservasi"} 3`,
		`rivus_table_sink_rows_total{job_id="job-1",kind="stream",op="delete_append",sink_type="iceberg",source_table="app.reservasi",target_table="bronze.reservasi"} 2`,
		`rivus_table_iceberg_write_duration_seconds_sum{job_id="job-1",op="delete_append",sink_type="iceberg",source_table="app.reservasi",status="success",target_table="bronze.reservasi"} 1.5`,
		`rivus_table_iceberg_total_records{job_id="job-1",sink_type="iceberg",source_table="app.reservasi",target_table="bronze.reservasi"} 1200`,
		`rivus_table_iceberg_total_files_size_bytes{job_id="job-1",sink_type="iceberg",source_table="app.reservasi",target_table="bronze.reservasi"} 7.340032e+06`,
		`rivus_table_iceberg_snapshot_id{job_id="job-1",sink_type="iceberg",source_table="app.reservasi",target_table="bronze.reservasi"} 42`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, text)
		}
	}
}

func resetTableMetricsForTest() {
	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	tableRegistry.tables = make(map[string]*tableStats)
}
