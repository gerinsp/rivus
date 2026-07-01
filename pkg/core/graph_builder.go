package core

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

const graphBackpressureGuard = 10 * time.Second

func sourceTypeFromConfig(cfg *config.JobConfig) string {
	if cfg == nil {
		return ""
	}
	if cfg.Source != nil && strings.TrimSpace(cfg.Source.Type) != "" {
		return strings.ToLower(strings.TrimSpace(cfg.Source.Type))
	}
	return "mysql"
}

func (j *Job) buildGraphLocked() *JobGraph {
	if j == nil || j.Config == nil {
		return nil
	}

	var progressCopy *JobProgress
	if j.progress != nil {
		cp := *j.progress
		progressCopy = &cp
	}

	sourceType := sourceTypeFromConfig(j.Config)
	sinkType := sinkTypeFromConfig(j.Config)
	lastErrComponent, lastErrMessage := j.lastGraphErrorLocked()

	sourceID := "source:" + sourceType
	bufferID := "buffer:events"
	sinkID := "sink:" + sinkType

	return &JobGraph{
		JobID:    j.Config.ID,
		Status:   j.status,
		Progress: progressCopy,
		Nodes: []GraphNode{
			buildSourceGraphNode(j.Config, sourceID, sourceType, j.status, progressCopy, lastErrComponent, lastErrMessage),
			buildBufferGraphNode(j.Config, bufferID, j.status, progressCopy, lastErrComponent, lastErrMessage),
			buildSinkGraphNode(j.Config, sinkID, sinkType, j.status, progressCopy, lastErrComponent, lastErrMessage),
		},
		Edges: []GraphEdge{
			buildSourceBufferEdge(j.Config, sourceID, bufferID, j.status, progressCopy),
			buildBufferSinkEdge(j.Config, bufferID, sinkID, j.status, progressCopy),
		},
	}
}

func (j *Job) lastGraphErrorLocked() (string, string) {
	if len(j.errors) == 0 {
		return "", ""
	}
	last := j.errors[len(j.errors)-1]
	return strings.ToLower(strings.TrimSpace(last.Component)), strings.TrimSpace(last.Message)
}

func buildSourceGraphNode(cfg *config.JobConfig, id, sourceType string, status JobStatus, progress *JobProgress, lastErrComponent, lastErrMessage string) GraphNode {
	totalTables := sourceTableCount(cfg, progress)
	currentTableIndex := 0
	currentTableRows := int64(0)
	currentTable := ""
	if progress != nil {
		currentTableIndex = progress.CurrentTableIndex
		currentTableRows = progress.CurrentTableRows
		currentTable = strings.TrimSpace(progress.CurrentTable)
	}

	metrics := []GraphMetric{
		{Label: "Mode", Value: graphModeLabel(cfg), Tone: "blue"},
		{Label: "Tables", Value: graphTableCountLabel(totalTables), Tone: "slate"},
	}
	if chunkSize := sourceChunkSize(cfg, sourceType); chunkSize > 0 {
		metrics = append(metrics, GraphMetric{Label: "Chunk", Value: fmt.Sprintf("%s rows", graphFormatInt(int64(chunkSize))), Tone: "slate"})
	}
	if currentTableIndex > 0 && totalTables > 0 {
		metrics = append(metrics, GraphMetric{Label: "Position", Value: fmt.Sprintf("%s / %s", graphFormatInt(int64(currentTableIndex)), graphFormatInt(int64(totalTables))), Tone: graphFlowTone(progress, status)})
	}
	if currentTableRows > 0 {
		metrics = append(metrics, GraphMetric{Label: "Rows emitted", Value: graphFormatInt(currentTableRows), Tone: graphFlowTone(progress, status)})
	}
	if progress != nil && strings.TrimSpace(progress.CDCCurrentFile) != "" && progress.CDCCurrentPos > 0 {
		metrics = append(metrics, GraphMetric{Label: "CDC current", Value: graphBinlogPosition(progress.CDCCurrentFile, progress.CDCCurrentPos), Tone: graphFlowTone(progress, status)})
	}
	if progress != nil && strings.TrimSpace(progress.CDCStartFile) != "" && progress.CDCStartPos > 0 {
		metrics = append(metrics, GraphMetric{Label: "CDC start", Value: graphBinlogPosition(progress.CDCStartFile, progress.CDCStartPos), Tone: "slate"})
	}
	if countResumeEnabled(cfg) {
		metrics = append(metrics, GraphMetric{Label: "Resume strategy", Value: "COUNT(*) skip/reset", Tone: "blue"})
	}
	if filterCount := sourceFilterCount(cfg, sourceType); filterCount > 0 {
		metrics = append(metrics, GraphMetric{Label: "Filters", Value: graphFormatInt(int64(filterCount)), Tone: "slate"})
	}

	subtitle := firstNonEmpty(sourceEndpoint(cfg), "source:"+sourceType)
	detail := sourceGraphDetail(progress, status, currentTable, lastErrComponent, lastErrMessage)

	return GraphNode{
		ID:       id,
		Type:     NodeSource,
		Label:    fmt.Sprintf("Source (%s)", firstNonEmpty(sourceType, "unknown")),
		Subtitle: subtitle,
		Detail:   detail,
		State:    sourceGraphState(progress, status),
		Status:   sourceGraphStatus(progress, status),
		Metrics:  metrics,
	}
}

func buildBufferGraphNode(cfg *config.JobConfig, id string, status JobStatus, progress *JobProgress, lastErrComponent, lastErrMessage string) GraphNode {
	metrics := []GraphMetric{
		{Label: "Capacity", Value: fmt.Sprintf("%s events", graphFormatInt(int64(bufferCapacity(cfg)))), Tone: graphFlowTone(progress, status)},
		{Label: "Guard", Value: graphBackpressureGuard.String(), Tone: "slate"},
	}
	if progress != nil {
		metrics = append(metrics, GraphMetric{Label: "Phase", Value: graphPhaseLabel(progress.Phase), Tone: graphFlowTone(progress, status)})
	}

	detail := "In-memory channel between source and sink."
	if isBackpressureProgress(progress) {
		detail = "Source is waiting because the sink has not drained buffered events fast enough."
	} else if status == JobStatusPending {
		detail = "Queue is idle while preflight validates source and target tables."
	} else if status == JobStatusDone {
		detail = "Buffered events have been drained."
	} else if status == JobStatusStopped {
		detail = "Stop/delete now waits for buffered events to drain before shutdown."
	} else if status == JobStatusFailed && lastErrComponent == "system" && lastErrMessage != "" {
		detail = lastErrMessage
	}

	return GraphNode{
		ID:       id,
		Type:     NodeBuffer,
		Label:    "Event Buffer",
		Subtitle: "In-memory event handoff",
		Detail:   detail,
		State:    bufferGraphState(progress, status),
		Status:   bufferGraphStatus(progress, status),
		Metrics:  metrics,
	}
}

func buildSinkGraphNode(cfg *config.JobConfig, id, sinkType string, status JobStatus, progress *JobProgress, lastErrComponent, lastErrMessage string) GraphNode {
	metrics := []GraphMetric{
		{Label: "Type", Value: strings.ToUpper(firstNonEmpty(sinkType, "unknown")), Tone: "blue"},
	}
	if batchSize := sinkBatchSize(cfg, sinkType); batchSize > 0 {
		metrics = append(metrics, GraphMetric{Label: "Batch", Value: fmt.Sprintf("%s events", graphFormatInt(int64(batchSize))), Tone: "slate"})
	}
	if flushSeconds := sinkFlushSeconds(cfg, sinkType); flushSeconds > 0 {
		metrics = append(metrics, GraphMetric{Label: "Flush", Value: fmt.Sprintf("%ds", flushSeconds), Tone: "slate"})
	}
	if overrideCount := sinkOverrideCount(cfg, sinkType); overrideCount > 0 {
		metrics = append(metrics, GraphMetric{Label: "Routes", Value: graphFormatInt(int64(overrideCount)), Tone: "slate"})
	}
	if targetSummary := sinkTargetSummary(cfg, sinkType); targetSummary != "" {
		metrics = append(metrics, GraphMetric{Label: "Target", Value: targetSummary, Tone: "slate"})
	}
	if progress != nil && strings.TrimSpace(progress.SinkSummary) != "" {
		metrics = append(metrics, GraphMetric{Label: "Runtime", Value: strings.TrimSpace(progress.SinkSummary), Tone: graphFlowTone(progress, status)})
	}

	subtitle := firstNonEmpty(sinkEndpoint(cfg, sinkType), "sink:"+sinkType)
	detail := sinkGraphDetail(progress, status, lastErrComponent, lastErrMessage)

	return GraphNode{
		ID:       id,
		Type:     NodeSink,
		Label:    fmt.Sprintf("Sink (%s)", firstNonEmpty(sinkType, "unknown")),
		Subtitle: subtitle,
		Detail:   detail,
		State:    sinkGraphState(progress, status),
		Status:   sinkGraphStatus(progress, status),
		Metrics:  metrics,
	}
}

func buildSourceBufferEdge(cfg *config.JobConfig, from, to string, status JobStatus, progress *JobProgress) GraphEdge {
	metrics := []GraphMetric{}
	if chunkSize := sourceChunkSize(cfg, sourceTypeFromConfig(cfg)); chunkSize > 0 {
		metrics = append(metrics, GraphMetric{Label: "Snapshot chunk", Value: fmt.Sprintf("%s rows", graphFormatInt(int64(chunkSize))), Tone: "slate"})
	}
	if totalTables := sourceTableCount(cfg, progress); totalTables > 0 {
		metrics = append(metrics, GraphMetric{Label: "Table scope", Value: graphTableCountLabel(totalTables), Tone: "slate"})
	}

	return GraphEdge{
		From:    from,
		To:      to,
		Label:   "Read and emit events",
		Detail:  sourceEdgeDetail(progress, status),
		State:   sourceEdgeState(progress, status),
		Metrics: metrics,
	}
}

func buildBufferSinkEdge(cfg *config.JobConfig, from, to string, status JobStatus, progress *JobProgress) GraphEdge {
	sinkType := sinkTypeFromConfig(cfg)
	metrics := []GraphMetric{
		{Label: "Buffer cap", Value: fmt.Sprintf("%s events", graphFormatInt(int64(bufferCapacity(cfg)))), Tone: graphFlowTone(progress, status)},
	}
	if batchSize := sinkBatchSize(cfg, sinkType); batchSize > 0 {
		metrics = append(metrics, GraphMetric{Label: "Flush batch", Value: fmt.Sprintf("%s events", graphFormatInt(int64(batchSize))), Tone: "slate"})
	}
	if flushSeconds := sinkFlushSeconds(cfg, sinkType); flushSeconds > 0 {
		metrics = append(metrics, GraphMetric{Label: "Flush cadence", Value: fmt.Sprintf("%ds", flushSeconds), Tone: "slate"})
	}

	return GraphEdge{
		From:    from,
		To:      to,
		Label:   "Flush buffered events to target",
		Detail:  sinkEdgeDetail(progress, status),
		State:   sinkEdgeState(progress, status),
		Metrics: metrics,
	}
}

func sourceGraphState(progress *JobProgress, status JobStatus) string {
	switch status {
	case JobStatusFailed:
		return "BLOCKED"
	case JobStatusStopped:
		return "STOPPED"
	case JobStatusPausing:
		return "STOPPING SOURCE"
	case JobStatusPaused:
		return "PAUSED"
	case JobStatusDone:
		return "COMPLETED"
	}
	if isBackpressureProgress(progress) {
		return "PAUSED ON BUFFER"
	}
	switch graphPhase(progress) {
	case "preflight":
		return "DISCOVERING TABLES"
	case "snapshot", "snapshot_complete":
		return "READING SNAPSHOT"
	case "streaming":
		return "READING CDC"
	default:
		if status == JobStatusRunning {
			return "RUNNING"
		}
		return string(status)
	}
}

func bufferGraphState(progress *JobProgress, status JobStatus) string {
	switch status {
	case JobStatusFailed:
		return "BLOCKED"
	case JobStatusStopped:
		return "DRAINED"
	case JobStatusPausing:
		return "DRAINING"
	case JobStatusPaused:
		return "EMPTY"
	case JobStatusDone:
		return "EMPTY"
	}
	if isBackpressureProgress(progress) {
		return "BACKPRESSURE"
	}
	switch graphPhase(progress) {
	case "preflight":
		return "IDLE"
	case "snapshot", "snapshot_complete", "streaming":
		return "FLOWING"
	default:
		if status == JobStatusRunning {
			return "READY"
		}
		return string(status)
	}
}

func sinkGraphState(progress *JobProgress, status JobStatus) string {
	switch status {
	case JobStatusFailed:
		return "BLOCKED"
	case JobStatusStopped:
		return "STOPPED"
	case JobStatusPausing:
		return "FLUSHING"
	case JobStatusPaused:
		return "PAUSED"
	case JobStatusDone:
		return "COMPLETED"
	}
	if isBackpressureProgress(progress) {
		return "FLUSHING SLOWLY"
	}
	switch graphPhase(progress) {
	case "preflight":
		return "CREATING TARGETS"
	case "snapshot", "snapshot_complete":
		return "WRITING BATCHES"
	case "streaming":
		return "APPLYING EVENTS"
	default:
		if status == JobStatusRunning {
			return "READY"
		}
		return string(status)
	}
}

func sourceGraphStatus(progress *JobProgress, status JobStatus) JobStatus {
	if graphPhase(progress) == "preflight" && status == JobStatusRunning {
		return JobStatusPending
	}
	return status
}

func bufferGraphStatus(progress *JobProgress, status JobStatus) JobStatus {
	if isBackpressureProgress(progress) && status == JobStatusRunning {
		return JobStatusPending
	}
	return status
}

func sinkGraphStatus(progress *JobProgress, status JobStatus) JobStatus {
	if graphPhase(progress) == "preflight" && (status == JobStatusPending || status == JobStatusRunning) {
		return JobStatusPending
	}
	if isBackpressureProgress(progress) && status == JobStatusRunning {
		return JobStatusPending
	}
	return status
}

func sourceGraphDetail(progress *JobProgress, status JobStatus, currentTable, lastErrComponent, lastErrMessage string) string {
	if status == JobStatusFailed && (lastErrComponent == "source" || lastErrComponent == "system") && lastErrMessage != "" {
		return lastErrMessage
	}
	if progress == nil {
		return "Waiting for source runtime updates."
	}
	if detail := strings.TrimSpace(progress.Detail); detail != "" {
		return detail
	}
	if summary := strings.TrimSpace(progress.Summary); summary != "" {
		return summary
	}
	if currentTable != "" {
		return currentTable
	}
	return "Waiting for source runtime updates."
}

func sinkGraphDetail(progress *JobProgress, status JobStatus, lastErrComponent, lastErrMessage string) string {
	if status == JobStatusFailed && (lastErrComponent == "sink" || lastErrComponent == "system") && lastErrMessage != "" {
		return lastErrMessage
	}
	if progress != nil && strings.TrimSpace(progress.SinkSummary) != "" {
		return strings.TrimSpace(strings.Join(nonEmptyStrings(progress.SinkSummary, progress.SinkDetail), " | "))
	}
	if isBackpressureProgress(progress) {
		return "Sink is still draining and flushing events, so source emission is temporarily paused."
	}
	switch graphPhase(progress) {
	case "preflight":
		return "Preparing sink tables and target mappings before event flow starts."
	case "snapshot", "snapshot_complete":
		return "Applying snapshot batches into the target."
	case "streaming":
		return "Applying live CDC events into the target."
	default:
		if status == JobStatusRunning {
			return "Waiting for events from source."
		}
		return "Sink is idle."
	}
}

func sourceEdgeState(progress *JobProgress, status JobStatus) string {
	if isBackpressureProgress(progress) {
		return "PAUSED"
	}
	switch status {
	case JobStatusFailed:
		return "BLOCKED"
	case JobStatusStopped:
		return "STOPPED"
	case JobStatusPausing:
		return "STOPPING"
	case JobStatusPaused:
		return "PAUSED"
	case JobStatusDone:
		return "COMPLETED"
	default:
		if graphPhase(progress) == "preflight" {
			return "IDLE"
		}
		return "ACTIVE"
	}
}

func sinkEdgeState(progress *JobProgress, status JobStatus) string {
	if isBackpressureProgress(progress) {
		return "WAITING"
	}
	switch status {
	case JobStatusFailed:
		return "BLOCKED"
	case JobStatusStopped:
		return "STOPPED"
	case JobStatusPausing:
		return "DRAINING"
	case JobStatusPaused:
		return "PAUSED"
	case JobStatusDone:
		return "COMPLETED"
	default:
		if graphPhase(progress) == "preflight" {
			return "IDLE"
		}
		return "ACTIVE"
	}
}

func sourceEdgeDetail(progress *JobProgress, status JobStatus) string {
	if isBackpressureProgress(progress) {
		return "Source has rows ready, but it is waiting for downstream buffer capacity."
	}
	switch graphPhase(progress) {
	case "preflight":
		return "No events emitted yet while schemas and target tables are validated."
	case "snapshot", "snapshot_complete":
		return "Snapshot rows are being read from MySQL and emitted into the pipeline."
	case "streaming":
		return "CDC events are being emitted from the source into the pipeline."
	default:
		if status == JobStatusDone {
			return "Source emission is complete."
		}
		return "Source emission is idle."
	}
}

func sinkEdgeDetail(progress *JobProgress, status JobStatus) string {
	if progress != nil && strings.TrimSpace(progress.SinkSummary) != "" {
		return strings.TrimSpace(strings.Join(nonEmptyStrings(progress.SinkSummary, progress.SinkDetail), " | "))
	}
	if isBackpressureProgress(progress) {
		return "Buffered events are waiting because sink flush throughput is lower than source read throughput."
	}
	switch graphPhase(progress) {
	case "preflight":
		return "Sink is not consuming events until target validation is done."
	case "snapshot", "snapshot_complete":
		return "Buffered snapshot rows are being flushed into the target sink."
	case "streaming":
		return "Buffered CDC events are being applied into the target sink."
	default:
		if status == JobStatusDone {
			return "All buffered events have been flushed."
		}
		return "Buffer is ready to flush events to sink."
	}
}

func graphPhase(progress *JobProgress) string {
	if progress == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(progress.Phase))
}

func graphPhaseLabel(phase string) string {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "preflight":
		return "Preflight"
	case "snapshot":
		return "Snapshot"
	case "snapshot_complete":
		return "Snapshot complete"
	case "streaming":
		return "CDC"
	case "done":
		return "Done"
	case "failed":
		return "Failed"
	case "stopped":
		return "Stopped"
	default:
		return firstNonEmpty(strings.TrimSpace(phase), "Unknown")
	}
}

func graphModeLabel(cfg *config.JobConfig) string {
	if cfg == nil {
		return "unknown"
	}
	mode := strings.TrimSpace(string(cfg.Mode))
	if mode == "" {
		return "initial"
	}
	return mode
}

func graphTableCountLabel(totalTables int) string {
	if totalTables <= 0 {
		return "-"
	}
	return graphFormatInt(int64(totalTables))
}

func graphFlowTone(progress *JobProgress, status JobStatus) string {
	switch {
	case status == JobStatusFailed:
		return "rose"
	case isBackpressureProgress(progress):
		return "amber"
	case status == JobStatusDone:
		return "blue"
	default:
		return "blue"
	}
}

func isBackpressureProgress(progress *JobProgress) bool {
	if progress == nil {
		return false
	}
	summary := strings.ToLower(strings.TrimSpace(progress.Summary))
	detail := strings.ToLower(strings.TrimSpace(progress.Detail))
	return strings.Contains(summary, "waiting for sink flush") || strings.Contains(detail, "sink is slower than snapshot reader")
}

func sourceTableCount(cfg *config.JobConfig, progress *JobProgress) int {
	if progress != nil && progress.TotalTables > 0 {
		return progress.TotalTables
	}
	if cfg == nil {
		return 0
	}
	if len(cfg.MySQL.Tables) > 0 {
		return len(cfg.MySQL.Tables)
	}
	srcCfg := sourceConfigMap(cfg)
	if srcCfg == nil {
		return 0
	}

	seen := make(map[string]struct{})
	add := func(raw string) {
		entry := strings.ToLower(strings.TrimSpace(raw))
		if entry == "" {
			return
		}
		seen[entry] = struct{}{}
	}

	for _, table := range stringSliceFromAny(srcCfg["tables"]) {
		add(table)
	}

	databases := stringSliceFromAny(srcCfg["databases"])
	tableNames := stringSliceFromAny(srcCfg["table_names"])
	for _, dbName := range databases {
		dbName = strings.TrimSpace(dbName)
		if dbName == "" {
			continue
		}
		for _, tableName := range tableNames {
			tableName = strings.TrimSpace(tableName)
			if tableName == "" {
				continue
			}
			add(dbName + "." + tableName)
		}
	}

	return len(seen)
}

func sourceChunkSize(cfg *config.JobConfig, sourceType string) int {
	if cfg == nil {
		return 0
	}
	if sourceType == "mysql" && cfg.MySQL.ChunkSize > 0 {
		return cfg.MySQL.ChunkSize
	}
	return intFromAny(sourceConfigMap(cfg)["chunk_size"])
}

func sourceFilterCount(cfg *config.JobConfig, sourceType string) int {
	if cfg == nil {
		return 0
	}
	if sourceType == "mysql" && len(cfg.MySQL.TableConfigs) > 0 {
		return len(cfg.MySQL.TableConfigs)
	}
	return len(mapFromAny(sourceConfigMap(cfg)["table_configs"]))
}

func countResumeEnabled(cfg *config.JobConfig) bool {
	if cfg == nil {
		return false
	}
	return metadataBool(cfg.Metadata, "snapshot_only_count_resume")
}

func bufferCapacity(cfg *config.JobConfig) int {
	if cfg == nil || cfg.BufferSize <= 0 {
		return 1000
	}
	return cfg.BufferSize
}

func sinkBatchSize(cfg *config.JobConfig, sinkType string) int {
	if cfg == nil {
		return 0
	}
	switch sinkType {
	case "doris":
		if cfg.Doris.BatchSize > 0 {
			return cfg.Doris.BatchSize
		}
	case "iceberg_native":
		return intFromAny(sinkConfigMap(cfg)["batch_size"])
	}
	return intFromAny(sinkConfigMap(cfg)["batch_size"])
}

func sinkFlushSeconds(cfg *config.JobConfig, sinkType string) int {
	if cfg == nil {
		return 0
	}
	switch sinkType {
	case "doris":
		if cfg.Doris.FlushSeconds > 0 {
			return cfg.Doris.FlushSeconds
		}
	case "iceberg_native":
		return intFromAny(sinkConfigMap(cfg)["flush_seconds"])
	}
	return intFromAny(sinkConfigMap(cfg)["flush_seconds"])
}

func sinkOverrideCount(cfg *config.JobConfig, sinkType string) int {
	if cfg == nil {
		return 0
	}
	switch sinkType {
	case "doris":
		if len(cfg.Doris.Overrides) > 0 {
			return len(cfg.Doris.Overrides)
		}
	case "iceberg_native":
		return len(mapFromAny(sinkConfigMap(cfg)["overrides"]))
	}
	return len(mapFromAny(sinkConfigMap(cfg)["overrides"]))
}

func sinkTargetSummary(cfg *config.JobConfig, sinkType string) string {
	if cfg == nil {
		return ""
	}
	switch sinkType {
	case "doris":
		if strings.TrimSpace(cfg.Doris.DefaultDatabase) != "" {
			return cfg.Doris.DefaultDatabase
		}
	case "iceberg_native":
		if ns := stringFromAny(sinkConfigMap(cfg)["default_namespace"]); ns != "" {
			return ns
		}
	}
	if sinkType == "doris" {
		return stringFromAny(sinkConfigMap(cfg)["default_database"])
	}
	return ""
}

func sourceEndpoint(cfg *config.JobConfig) string {
	if cfg == nil {
		return ""
	}
	if strings.TrimSpace(cfg.MySQL.Addr) != "" {
		return cfg.MySQL.Addr
	}
	return stringFromAny(sourceConfigMap(cfg)["addr"])
}

func sinkEndpoint(cfg *config.JobConfig, sinkType string) string {
	if cfg == nil {
		return ""
	}
	switch sinkType {
	case "doris":
		if strings.TrimSpace(cfg.Doris.HTTPHost) != "" {
			return cfg.Doris.HTTPHost
		}
		return stringFromAny(sinkConfigMap(cfg)["http_host"])
	case "iceberg_native":
		sinkCfg := sinkConfigMap(cfg)
		if restURI := stringFromAny(sinkCfg["rest_uri"]); restURI != "" {
			return restURI
		}
		return stringFromAny(sinkCfg["catalog_uri"])
	default:
		return ""
	}
}

func sourceConfigMap(cfg *config.JobConfig) map[string]any {
	if cfg == nil || cfg.Source == nil {
		return nil
	}
	return cfg.Source.Config
}

func sinkConfigMap(cfg *config.JobConfig) map[string]any {
	if cfg == nil || cfg.Sink == nil {
		return nil
	}
	return cfg.Sink.Config
}

func mapFromAny(v any) map[string]any {
	switch t := v.(type) {
	case map[string]any:
		return t
	default:
		return nil
	}
}

func stringSliceFromAny(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s := stringFromAny(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func intFromAny(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int8:
		return int(t)
	case int16:
		return int(t)
	case int32:
		return int(t)
	case int64:
		return int(t)
	case uint:
		return int(t)
	case uint8:
		return int(t)
	case uint16:
		return int(t)
	case uint32:
		return int(t)
	case uint64:
		return int(t)
	case float32:
		return int(t)
	case float64:
		return int(t)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err == nil {
			return n
		}
	}
	return 0
}

func stringFromAny(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		return ""
	}
}

func graphFormatInt(n int64) string {
	raw := strconv.FormatInt(n, 10)
	if len(raw) <= 3 {
		return raw
	}

	sign := ""
	if strings.HasPrefix(raw, "-") {
		sign = "-"
		raw = raw[1:]
	}

	out := make([]byte, 0, len(raw)+len(raw)/3)
	head := len(raw) % 3
	if head == 0 {
		head = 3
	}
	out = append(out, raw[:head]...)
	for i := head; i < len(raw); i += 3 {
		out = append(out, ',')
		out = append(out, raw[i:i+3]...)
	}
	return sign + string(out)
}

func graphBinlogPosition(file string, pos uint32) string {
	file = strings.TrimSpace(file)
	if file == "" || pos == 0 {
		return "-"
	}
	return fmt.Sprintf("%s:%d", file, pos)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func nonEmptyStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
