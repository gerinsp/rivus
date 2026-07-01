package observability

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type TableActivity struct {
	JobID                  string            `json:"job_id"`
	SinkType               string            `json:"sink_type,omitempty"`
	SourceTable            string            `json:"source_table"`
	TargetTable            string            `json:"target_table,omitempty"`
	CDCEvents              map[string]uint64 `json:"cdc_events"`
	CDCRows                map[string]uint64 `json:"cdc_rows"`
	SinkFlushes            map[string]uint64 `json:"sink_flushes"`
	SinkEvents             map[string]uint64 `json:"sink_events"`
	SinkRows               map[string]uint64 `json:"sink_rows"`
	SinkDeletes            map[string]uint64 `json:"sink_deletes"`
	IcebergWrites          map[string]uint64 `json:"iceberg_writes"`
	IcebergWriteRows       map[string]uint64 `json:"iceberg_write_rows"`
	IcebergWriteDeletes    map[string]uint64 `json:"iceberg_write_deletes"`
	IcebergWriteErrors     map[string]uint64 `json:"iceberg_write_errors"`
	IcebergWriteDurationMS map[string]int64  `json:"iceberg_write_duration_ms"`
	IcebergTotalRecords    int64             `json:"iceberg_total_records,omitempty"`
	IcebergTotalSizeBytes  int64             `json:"iceberg_total_size_bytes,omitempty"`
	IcebergSnapshotID      int64             `json:"iceberg_snapshot_id,omitempty"`
	LastWriteDurationMS    int64             `json:"last_write_duration_ms"`
	MaxWriteDurationMS     int64             `json:"max_write_duration_ms"`
	LastCDCUnix            int64             `json:"last_cdc_unix,omitempty"`
	LastSinkUnix           int64             `json:"last_sink_unix,omitempty"`
	LastWriteUnix          int64             `json:"last_write_unix,omitempty"`
	LastActivityUnix       int64             `json:"last_activity_unix,omitempty"`
	Classification         string            `json:"classification"`
	RegisteredUnix         int64             `json:"registered_unix"`
}

type tableStats struct {
	jobID                  string
	sinkType               string
	sourceTable            string
	targetTable            string
	cdcEvents              map[string]uint64
	cdcRows                map[string]uint64
	sinkFlushes            map[string]uint64
	sinkEvents             map[string]uint64
	sinkRows               map[string]uint64
	sinkDeletes            map[string]uint64
	icebergWrites          map[string]uint64
	icebergWriteRows       map[string]uint64
	icebergWriteDeletes    map[string]uint64
	icebergWriteErrors     map[string]uint64
	icebergWriteDurationMS map[string]int64
	icebergTotalRecords    int64
	icebergTotalSizeBytes  int64
	icebergSnapshotID      int64
	lastWriteDurationMS    int64
	maxWriteDurationMS     int64
	lastCDCUnix            int64
	lastSinkUnix           int64
	lastWriteUnix          int64
	lastActivityUnix       int64
	registeredUnix         int64
}

var tableRegistry = struct {
	mu     sync.RWMutex
	tables map[string]*tableStats
}{
	tables: make(map[string]*tableStats),
}

func RegisterSourceTable(jobID, sourceTable string) {
	jobID = normalizeLabelValue(jobID)
	sourceTable = normalizeTableName(sourceTable)
	if jobID == "" || sourceTable == "" {
		return
	}
	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	ensureTableLocked(jobID, sourceTable)
}

func SetSinkType(jobID, sourceTable, sinkType string) {
	jobID = normalizeLabelValue(jobID)
	sourceTable = normalizeTableName(sourceTable)
	sinkType = normalizeSinkType(sinkType)
	if jobID == "" || sourceTable == "" || sinkType == "" {
		return
	}
	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	st := ensureTableLocked(jobID, sourceTable)
	st.sinkType = sinkType
}

func SetTargetTable(jobID, sourceTable, targetTable string) {
	jobID = normalizeLabelValue(jobID)
	sourceTable = normalizeTableName(sourceTable)
	targetTable = normalizeTableName(targetTable)
	if jobID == "" || sourceTable == "" {
		return
	}
	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	st := ensureTableLocked(jobID, sourceTable)
	if targetTable != "" {
		st.targetTable = targetTable
	}
}

func RecordMySQLCDC(jobID, schema, table, action string, rows int) {
	if rows < 0 {
		rows = 0
	}
	sourceTable := normalizeTableName(tableKey(schema, table))
	action = normalizeMetricKey(action)
	if sourceTable == "" || action == "" {
		return
	}
	now := time.Now().Unix()

	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	st := ensureTableLocked(jobID, sourceTable)
	st.cdcEvents[action]++
	st.cdcRows[action] += uint64(rows)
	st.lastCDCUnix = now
	st.lastActivityUnix = now
}

func RecordSinkFlush(jobID, sourceTable, targetTable, kind, op string, events, rows, deletes int, duration time.Duration) {
	sourceTable = normalizeTableName(sourceTable)
	targetTable = normalizeTableName(targetTable)
	opKey := operationKey(kind, op)
	if sourceTable == "" || opKey == "" {
		return
	}
	now := time.Now().Unix()

	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	st := ensureTableLocked(jobID, sourceTable)
	if targetTable != "" {
		st.targetTable = targetTable
	}
	st.sinkFlushes[opKey]++
	st.sinkEvents[opKey] += uint64(nonNegative(events))
	st.sinkRows[opKey] += uint64(nonNegative(rows))
	st.sinkDeletes[opKey] += uint64(nonNegative(deletes))
	st.lastSinkUnix = now
	st.lastActivityUnix = now
	_ = duration
}

func RecordIcebergWrite(jobID, sourceTable, targetTable, op, status string, rows, deletes int, duration time.Duration) {
	sourceTable = normalizeTableName(sourceTable)
	targetTable = normalizeTableName(targetTable)
	op = normalizeMetricKey(op)
	status = normalizeMetricKey(status)
	if sourceTable == "" || op == "" || status == "" {
		return
	}
	key := op + "|" + status
	durationMS := duration.Milliseconds()
	now := time.Now().Unix()

	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	st := ensureTableLocked(jobID, sourceTable)
	st.sinkType = "iceberg"
	if targetTable != "" {
		st.targetTable = targetTable
	}
	st.icebergWrites[key]++
	st.icebergWriteRows[key] += uint64(nonNegative(rows))
	st.icebergWriteDeletes[key] += uint64(nonNegative(deletes))
	st.icebergWriteDurationMS[key] += durationMS
	if status != "success" {
		st.icebergWriteErrors[key]++
	}
	st.lastWriteDurationMS = durationMS
	if durationMS > st.maxWriteDurationMS {
		st.maxWriteDurationMS = durationMS
	}
	st.lastWriteUnix = now
	st.lastActivityUnix = now
}

func RecordIcebergTableSnapshot(jobID, sourceTable, targetTable string, snapshotID, totalRecords, totalSizeBytes int64) {
	sourceTable = normalizeTableName(sourceTable)
	targetTable = normalizeTableName(targetTable)
	if sourceTable == "" {
		return
	}
	now := time.Now().Unix()

	tableRegistry.mu.Lock()
	defer tableRegistry.mu.Unlock()
	st := ensureTableLocked(jobID, sourceTable)
	st.sinkType = "iceberg"
	if targetTable != "" {
		st.targetTable = targetTable
	}
	st.icebergSnapshotID = snapshotID
	st.icebergTotalRecords = totalRecords
	st.icebergTotalSizeBytes = totalSizeBytes
	st.lastWriteUnix = now
	st.lastActivityUnix = now
}

func TableActivities() []TableActivity {
	return TableActivitiesBySink("")
}

func TableActivitiesBySink(sinkType string) []TableActivity {
	sinkType = normalizeSinkType(sinkType)
	tableRegistry.mu.RLock()
	defer tableRegistry.mu.RUnlock()

	out := make([]TableActivity, 0, len(tableRegistry.tables))
	for _, st := range tableRegistry.tables {
		activity := st.activityLocked(time.Now())
		if sinkType != "" && activity.SinkType != sinkType {
			continue
		}
		out = append(out, activity)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastActivityUnix != out[j].LastActivityUnix {
			return out[i].LastActivityUnix > out[j].LastActivityUnix
		}
		if out[i].JobID != out[j].JobID {
			return out[i].JobID < out[j].JobID
		}
		return out[i].SourceTable < out[j].SourceTable
	})
	return out
}

func WritePrometheus(w io.Writer) {
	activities := TableActivities()

	writeHelp(w, "rivus_table_registered", "Registered configured source table.")
	writeType(w, "rivus_table_registered", "gauge")
	writeHelp(w, "rivus_table_last_activity_timestamp_seconds", "Unix timestamp of the latest observed table activity.")
	writeType(w, "rivus_table_last_activity_timestamp_seconds", "gauge")
	writeHelp(w, "rivus_table_last_cdc_timestamp_seconds", "Unix timestamp of the latest observed MySQL CDC event.")
	writeType(w, "rivus_table_last_cdc_timestamp_seconds", "gauge")
	writeHelp(w, "rivus_table_activity_class", "Current table activity classification. Label class is one of unknown, hot, warm, cold, stale.")
	writeType(w, "rivus_table_activity_class", "gauge")
	writeHelp(w, "rivus_table_cdc_rows_total", "Observed MySQL CDC rows by table and action.")
	writeType(w, "rivus_table_cdc_rows_total", "counter")
	writeHelp(w, "rivus_table_cdc_events_total", "Observed MySQL CDC events by table and action.")
	writeType(w, "rivus_table_cdc_events_total", "counter")
	writeHelp(w, "rivus_table_sink_flushes_total", "Observed sink flushes by table and operation.")
	writeType(w, "rivus_table_sink_flushes_total", "counter")
	writeHelp(w, "rivus_table_sink_rows_total", "Observed sink rows by table and operation.")
	writeType(w, "rivus_table_sink_rows_total", "counter")
	writeHelp(w, "rivus_table_sink_deletes_total", "Observed sink delete keys/rows by table and operation.")
	writeType(w, "rivus_table_sink_deletes_total", "counter")
	writeHelp(w, "rivus_table_iceberg_write_duration_seconds_sum", "Cumulative Iceberg write duration by table, operation, and status.")
	writeType(w, "rivus_table_iceberg_write_duration_seconds_sum", "counter")
	writeHelp(w, "rivus_table_iceberg_write_duration_seconds_count", "Iceberg write duration sample count by table, operation, and status.")
	writeType(w, "rivus_table_iceberg_write_duration_seconds_count", "counter")
	writeHelp(w, "rivus_table_iceberg_write_duration_seconds_max", "Maximum observed Iceberg write duration by table.")
	writeType(w, "rivus_table_iceberg_write_duration_seconds_max", "gauge")
	writeHelp(w, "rivus_table_iceberg_write_rows_total", "Observed Iceberg write rows by table, operation, and status.")
	writeType(w, "rivus_table_iceberg_write_rows_total", "counter")
	writeHelp(w, "rivus_table_iceberg_write_deletes_total", "Observed Iceberg write deletes by table, operation, and status.")
	writeType(w, "rivus_table_iceberg_write_deletes_total", "counter")
	writeHelp(w, "rivus_table_iceberg_write_errors_total", "Observed Iceberg write errors by table, operation, and status.")
	writeType(w, "rivus_table_iceberg_write_errors_total", "counter")
	writeHelp(w, "rivus_table_iceberg_total_records", "Latest Iceberg current snapshot total-records by table.")
	writeType(w, "rivus_table_iceberg_total_records", "gauge")
	writeHelp(w, "rivus_table_iceberg_total_files_size_bytes", "Latest Iceberg current snapshot total-files-size by table.")
	writeType(w, "rivus_table_iceberg_total_files_size_bytes", "gauge")
	writeHelp(w, "rivus_table_iceberg_snapshot_id", "Latest Iceberg current snapshot ID by table.")
	writeType(w, "rivus_table_iceberg_snapshot_id", "gauge")

	for _, a := range activities {
		baseLabels := map[string]string{"job_id": a.JobID, "source_table": a.SourceTable}
		if a.SinkType != "" {
			baseLabels["sink_type"] = a.SinkType
		}
		if a.TargetTable != "" {
			baseLabels["target_table"] = a.TargetTable
		}
		writeMetric(w, "rivus_table_registered", baseLabels, 1)
		writeMetric(w, "rivus_table_last_activity_timestamp_seconds", baseLabels, float64(a.LastActivityUnix))
		writeMetric(w, "rivus_table_last_cdc_timestamp_seconds", baseLabels, float64(a.LastCDCUnix))
		classLabels := cloneLabels(baseLabels)
		classLabels["class"] = a.Classification
		writeMetric(w, "rivus_table_activity_class", classLabels, 1)

		for action, value := range a.CDCRows {
			labels := cloneLabels(baseLabels)
			labels["action"] = action
			writeMetric(w, "rivus_table_cdc_rows_total", labels, float64(value))
		}
		for action, value := range a.CDCEvents {
			labels := cloneLabels(baseLabels)
			labels["action"] = action
			writeMetric(w, "rivus_table_cdc_events_total", labels, float64(value))
		}
		for op, value := range a.SinkFlushes {
			kind, opName := splitOperationKey(op)
			labels := cloneLabels(baseLabels)
			labels["kind"] = kind
			labels["op"] = opName
			writeMetric(w, "rivus_table_sink_flushes_total", labels, float64(value))
		}
		for op, value := range a.SinkRows {
			kind, opName := splitOperationKey(op)
			labels := cloneLabels(baseLabels)
			labels["kind"] = kind
			labels["op"] = opName
			writeMetric(w, "rivus_table_sink_rows_total", labels, float64(value))
		}
		for op, value := range a.SinkDeletes {
			kind, opName := splitOperationKey(op)
			labels := cloneLabels(baseLabels)
			labels["kind"] = kind
			labels["op"] = opName
			writeMetric(w, "rivus_table_sink_deletes_total", labels, float64(value))
		}
		for key, value := range a.IcebergWriteDurationMS {
			op, status := splitWriteKey(key)
			labels := cloneLabels(baseLabels)
			labels["op"] = op
			labels["status"] = status
			writeMetric(w, "rivus_table_iceberg_write_duration_seconds_sum", labels, float64(value)/1000)
		}
		for key, value := range a.IcebergWrites {
			op, status := splitWriteKey(key)
			labels := cloneLabels(baseLabels)
			labels["op"] = op
			labels["status"] = status
			writeMetric(w, "rivus_table_iceberg_write_duration_seconds_count", labels, float64(value))
			writeMetric(w, "rivus_table_iceberg_write_rows_total", labels, float64(a.IcebergWriteRows[key]))
			writeMetric(w, "rivus_table_iceberg_write_deletes_total", labels, float64(a.IcebergWriteDeletes[key]))
			writeMetric(w, "rivus_table_iceberg_write_errors_total", labels, float64(a.IcebergWriteErrors[key]))
		}
		writeMetric(w, "rivus_table_iceberg_write_duration_seconds_max", baseLabels, float64(a.MaxWriteDurationMS)/1000)
		if a.IcebergSnapshotID != 0 {
			writeMetric(w, "rivus_table_iceberg_total_records", baseLabels, float64(a.IcebergTotalRecords))
			writeMetric(w, "rivus_table_iceberg_total_files_size_bytes", baseLabels, float64(a.IcebergTotalSizeBytes))
			writeMetric(w, "rivus_table_iceberg_snapshot_id", baseLabels, float64(a.IcebergSnapshotID))
		}
	}
}

func ensureTableLocked(jobID, sourceTable string) *tableStats {
	jobID = normalizeLabelValue(jobID)
	sourceTable = normalizeTableName(sourceTable)
	key := jobID + "|" + sourceTable
	if st, ok := tableRegistry.tables[key]; ok {
		return st
	}
	st := &tableStats{
		jobID:                  jobID,
		sourceTable:            sourceTable,
		cdcEvents:              make(map[string]uint64),
		cdcRows:                make(map[string]uint64),
		sinkFlushes:            make(map[string]uint64),
		sinkEvents:             make(map[string]uint64),
		sinkRows:               make(map[string]uint64),
		sinkDeletes:            make(map[string]uint64),
		icebergWrites:          make(map[string]uint64),
		icebergWriteRows:       make(map[string]uint64),
		icebergWriteDeletes:    make(map[string]uint64),
		icebergWriteErrors:     make(map[string]uint64),
		icebergWriteDurationMS: make(map[string]int64),
		registeredUnix:         time.Now().Unix(),
	}
	tableRegistry.tables[key] = st
	return st
}

func (s *tableStats) activityLocked(now time.Time) TableActivity {
	return TableActivity{
		JobID:                  s.jobID,
		SinkType:               s.sinkType,
		SourceTable:            s.sourceTable,
		TargetTable:            s.targetTable,
		CDCEvents:              cloneUintMap(s.cdcEvents),
		CDCRows:                cloneUintMap(s.cdcRows),
		SinkFlushes:            cloneUintMap(s.sinkFlushes),
		SinkEvents:             cloneUintMap(s.sinkEvents),
		SinkRows:               cloneUintMap(s.sinkRows),
		SinkDeletes:            cloneUintMap(s.sinkDeletes),
		IcebergWrites:          cloneUintMap(s.icebergWrites),
		IcebergWriteRows:       cloneUintMap(s.icebergWriteRows),
		IcebergWriteDeletes:    cloneUintMap(s.icebergWriteDeletes),
		IcebergWriteErrors:     cloneUintMap(s.icebergWriteErrors),
		IcebergWriteDurationMS: cloneInt64Map(s.icebergWriteDurationMS),
		IcebergTotalRecords:    s.icebergTotalRecords,
		IcebergTotalSizeBytes:  s.icebergTotalSizeBytes,
		IcebergSnapshotID:      s.icebergSnapshotID,
		LastWriteDurationMS:    s.lastWriteDurationMS,
		MaxWriteDurationMS:     s.maxWriteDurationMS,
		LastCDCUnix:            s.lastCDCUnix,
		LastSinkUnix:           s.lastSinkUnix,
		LastWriteUnix:          s.lastWriteUnix,
		LastActivityUnix:       s.lastActivityUnix,
		Classification:         classifyTableActivity(now, s.lastActivityUnix),
		RegisteredUnix:         s.registeredUnix,
	}
}

func classifyTableActivity(now time.Time, lastActivityUnix int64) string {
	if lastActivityUnix <= 0 {
		return "unknown"
	}
	age := now.Sub(time.Unix(lastActivityUnix, 0))
	switch {
	case age <= time.Hour:
		return "hot"
	case age <= 24*time.Hour:
		return "warm"
	case age <= 7*24*time.Hour:
		return "cold"
	default:
		return "stale"
	}
}

func operationKey(kind, op string) string {
	kind = normalizeMetricKey(kind)
	op = normalizeMetricKey(op)
	if kind == "" {
		return op
	}
	if op == "" {
		return kind
	}
	return kind + "|" + op
}

func splitWriteKey(key string) (string, string) {
	parts := strings.SplitN(key, "|", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func splitOperationKey(key string) (string, string) {
	parts := strings.SplitN(key, "|", 2)
	if len(parts) == 1 {
		return "", parts[0]
	}
	return parts[0], parts[1]
}

func tableKey(schema, table string) string {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if schema == "" {
		return table
	}
	if table == "" {
		return schema
	}
	return schema + "." + table
}

func normalizeTableName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func normalizeMetricKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "_")
	value = strings.ReplaceAll(value, "-", "_")
	if value == "" {
		return "unknown"
	}
	return value
}

func normalizeSinkType(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	value = normalizeMetricKey(value)
	if strings.HasPrefix(value, "iceberg") {
		return "iceberg"
	}
	return value
}

func normalizeLabelValue(value string) string {
	return strings.TrimSpace(value)
}

func nonNegative(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

func cloneUintMap(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func writeHelp(w io.Writer, name, help string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
}

func writeType(w io.Writer, name, typ string) {
	fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
}

func writeMetric(w io.Writer, name string, labels map[string]string, value float64) {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	fmt.Fprint(w, name)
	if len(keys) > 0 {
		fmt.Fprint(w, "{")
		for idx, key := range keys {
			if idx > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, `%s="%s"`, key, escapePrometheusLabel(labels[key]))
		}
		fmt.Fprint(w, "}")
	}
	fmt.Fprintf(w, " %g\n", value)
}

func escapePrometheusLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
