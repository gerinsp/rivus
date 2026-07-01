package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
)

type JobError struct {
	Component string    `json:"component"`
	Message   string    `json:"message"`
	Time      time.Time `json:"time"`
}

type JobProgress struct {
	Phase                   string `json:"phase"`
	Summary                 string `json:"summary"`
	Detail                  string `json:"detail,omitempty"`
	CurrentTable            string `json:"current_table,omitempty"`
	CurrentTableIndex       int    `json:"current_table_index,omitempty"`
	CompletedTables         int    `json:"completed_tables,omitempty"`
	TotalTables             int    `json:"total_tables,omitempty"`
	CurrentTableRows        int64  `json:"current_table_rows,omitempty"`
	CDCStartFile            string `json:"cdc_start_file,omitempty"`
	CDCStartPos             uint32 `json:"cdc_start_pos,omitempty"`
	CDCCurrentFile          string `json:"cdc_current_file,omitempty"`
	CDCCurrentPos           uint32 `json:"cdc_current_pos,omitempty"`
	CheckpointPending       bool   `json:"checkpoint_pending,omitempty"`
	CheckpointReason        string `json:"checkpoint_reason,omitempty"`
	CheckpointPosition      string `json:"checkpoint_position,omitempty"`
	CheckpointPendingTables string `json:"checkpoint_pending_tables,omitempty"`
	SinkPhase               string `json:"sink_phase,omitempty"`
	SinkSummary             string `json:"sink_summary,omitempty"`
	SinkDetail              string `json:"sink_detail,omitempty"`
	SinkTable               string `json:"sink_table,omitempty"`
	SinkRows                int64  `json:"sink_rows,omitempty"`
}

type Checkpoint struct {
	MetaKey           string                       `json:"meta_key,omitempty"`
	CDCOffset         *CheckpointOffset            `json:"cdc_offset,omitempty"`
	SnapshotState     *CheckpointSnapshotState     `json:"snapshot_state,omitempty"`
	SnapshotProgress  *CheckpointSnapshotProgress  `json:"snapshot_progress,omitempty"`
	BinlogDiagnostics *CheckpointBinlogDiagnostics `json:"binlog_diagnostics,omitempty"`
	Error             string                       `json:"error,omitempty"`
}

type CheckpointOffset struct {
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  uint32 `json:"binlog_pos,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type CheckpointSnapshotState struct {
	StartFile string `json:"start_file,omitempty"`
	StartPos  uint32 `json:"start_pos,omitempty"`
	Done      bool   `json:"done"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type CheckpointSnapshotProgress struct {
	TableName  string `json:"table_name,omitempty"`
	NextOffset int64  `json:"next_offset,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type CheckpointBinlogDiagnostics struct {
	CheckpointFile                    string `json:"checkpoint_file,omitempty"`
	CheckpointPos                     uint32 `json:"checkpoint_pos,omitempty"`
	CheckpointUpdatedAt               string `json:"checkpoint_updated_at,omitempty"`
	CheckpointAgeSec                  int64  `json:"checkpoint_age_sec,omitempty"`
	EarliestFile                      string `json:"earliest_file,omitempty"`
	EarliestFirstEventAt              string `json:"earliest_first_event_at,omitempty"`
	EarliestFirstEvent                string `json:"earliest_first_event,omitempty"`
	EarliestFirstEventError           string `json:"earliest_first_event_error,omitempty"`
	CheckpointToEarliestFirstEventSec int64  `json:"checkpoint_to_earliest_first_event_sec,omitempty"`
	LatestFile                        string `json:"latest_file,omitempty"`
	LatestCreatedAt                   string `json:"latest_created_at,omitempty"`
	LatestFirstEventAt                string `json:"latest_first_event_at,omitempty"`
	LatestFirstEvent                  string `json:"latest_first_event,omitempty"`
	LatestFirstEventError             string `json:"latest_first_event_error,omitempty"`
	CheckpointToLatestFirstEventSec   int64  `json:"checkpoint_to_latest_first_event_sec,omitempty"`
	AvailableCount                    int    `json:"available_count,omitempty"`
	ObservedAt                        string `json:"observed_at,omitempty"`
	SourceServerTime                  string `json:"source_server_time,omitempty"`
	CheckpointAvailable               bool   `json:"checkpoint_available"`
	Status                            string `json:"status"`
	Error                             string `json:"error,omitempty"`
}

type Job struct {
	Config  *config.JobConfig
	Created time.Time
	Updated time.Time

	mu               sync.RWMutex
	status           JobStatus
	errors           []JobError
	progress         *JobProgress
	cancelFunc       context.CancelFunc
	sourceCancelFunc context.CancelFunc
	runDone          chan struct{}
	pauseRequested   bool

	statusListener   func(JobStatus)
	progressListener func(*JobProgress)

	// runtime deps
	registry *connector.Registry

	// meta checkpoint
	metaStore meta.OffsetStore
	metaKey   string

	// graph snapshot
	graph *JobGraph
}

// NOTE: constructor now needs registry
func NewJob(cfg *config.JobConfig, reg *connector.Registry) *Job {
	now := time.Now()
	j := &Job{
		Config:   cfg,
		Created:  now,
		Updated:  now,
		status:   JobStatusCreated,
		registry: reg,
	}
	// metaKey will be built at Start(), because it depends on chosen source/sink config
	return j
}

func (j *Job) MetaKey() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.metaKey
}

func (j *Job) GetStatus() JobStatus {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.status
}

func (j *Job) StatusValue() JobStatus { // helper for API
	return j.GetStatus()
}

func (j *Job) GetErrors() []JobError {
	j.mu.RLock()
	defer j.mu.RUnlock()
	out := make([]JobError, len(j.errors))
	copy(out, j.errors)
	return out
}

func (j *Job) GetLastError() *JobError {
	j.mu.RLock()
	defer j.mu.RUnlock()

	if len(j.errors) == 0 {
		return nil
	}
	le := j.errors[len(j.errors)-1] // copy value
	return &le
}

func (j *Job) Progress() *JobProgress {
	j.mu.RLock()
	defer j.mu.RUnlock()

	if j.progress == nil {
		return nil
	}

	cp := *j.progress
	return &cp
}

func (j *Job) Checkpoint() *Checkpoint {
	store, key, readerErr := j.checkpointReader()
	if store == nil || strings.TrimSpace(key) == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cp := &Checkpoint{MetaKey: key}
	if readerErr != nil {
		cp.Error = readerErr.Error()
		return cp
	}

	off, err := store.GetOffset(ctx, key)
	if err != nil {
		cp.Error = err.Error()
		return cp
	}
	if off != nil {
		cp.CDCOffset = &CheckpointOffset{
			BinlogFile: off.BinlogFile,
			BinlogPos:  off.BinlogPos,
			UpdatedAt:  formatCheckpointTime(off.UpdatedAt),
		}
		cp.BinlogDiagnostics = j.checkpointBinlogDiagnostics(ctx, off)
	}

	st, err := store.GetSnapshotState(ctx, key)
	if err != nil {
		cp.Error = err.Error()
		return cp
	}
	if st != nil {
		cp.SnapshotState = &CheckpointSnapshotState{
			StartFile: st.StartFile,
			StartPos:  st.StartPos,
			Done:      st.Done,
			UpdatedAt: formatCheckpointTime(st.UpdatedAt),
		}
	}

	progress, err := store.GetSnapshotProgress(ctx, key)
	if err != nil {
		cp.Error = err.Error()
		return cp
	}
	if progress != nil {
		cp.SnapshotProgress = &CheckpointSnapshotProgress{
			TableName:  progress.TableName,
			NextOffset: progress.NextOffset,
			UpdatedAt:  formatCheckpointTime(progress.UpdatedAt),
		}
	}

	if cp.CDCOffset == nil && cp.SnapshotState == nil && cp.SnapshotProgress == nil {
		return nil
	}
	return cp
}

func (j *Job) checkpointReader() (meta.OffsetStore, string, error) {
	j.mu.RLock()
	store := j.metaStore
	key := strings.TrimSpace(j.metaKey)
	cfg := j.Config
	j.mu.RUnlock()

	if store != nil && key != "" {
		return store, key, nil
	}
	if cfg == nil || strings.TrimSpace(cfg.Meta.MySQLDSN) == "" {
		return store, key, nil
	}

	srcType, srcCfg := j.pickSource()
	sinkType, sinkCfg := j.pickSink()
	computedKey := buildMetaKey(
		cfg.ID,
		string(normalizeMode(cfg.Mode)),
		srcType, srcCfg,
		sinkType, sinkCfg,
	)
	if key == "" {
		key = computedKey
	}

	if store == nil {
		metaStore, err := meta.NewMySQLOffsetStore(cfg.Meta.MySQLDSN)
		if err != nil {
			return nil, key, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := metaStore.Init(ctx); err != nil {
			return nil, key, err
		}
		store = metaStore
	}

	j.mu.Lock()
	if j.metaStore == nil {
		j.metaStore = store
	}
	if strings.TrimSpace(j.metaKey) == "" {
		j.metaKey = key
	}
	j.mu.Unlock()

	return store, key, nil
}

func (j *Job) checkpointBinlogDiagnostics(ctx context.Context, off *meta.Offset) *CheckpointBinlogDiagnostics {
	if off == nil {
		return nil
	}
	diag := &CheckpointBinlogDiagnostics{
		CheckpointFile:      off.BinlogFile,
		CheckpointPos:       off.BinlogPos,
		CheckpointUpdatedAt: formatCheckpointTime(off.UpdatedAt),
		ObservedAt:          time.Now().Format(time.RFC3339),
		Status:              "unknown",
	}
	if !off.UpdatedAt.IsZero() {
		diag.CheckpointAgeSec = int64(time.Since(off.UpdatedAt).Seconds())
	}

	cfg, ok := j.mysqlSourceConfig()
	if !ok {
		diag.Error = "source is not mysql or mysql config is unavailable"
		return diag
	}

	dbName := strings.TrimSpace(cfg.Database)
	if dbName == "" {
		dbName = "information_schema"
	}
	dsn := drivermysql.NewConfig()
	dsn.User = cfg.User
	dsn.Passwd = cfg.Password
	dsn.Net = "tcp"
	dsn.Addr = cfg.Addr
	dsn.DBName = dbName
	dsn.ParseTime = true
	dsn.InterpolateParams = true
	dsn.Timeout = 10 * time.Second
	dsn.ReadTimeout = 10 * time.Second
	dsn.WriteTimeout = 10 * time.Second
	dsn.Params = map[string]string{"charset": "utf8mb4"}

	db, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		diag.Error = err.Error()
		return diag
	}
	defer db.Close()

	if serverTime, err := queryMySQLServerTime(ctx, db); err == nil {
		diag.SourceServerTime = formatCheckpointTime(serverTime)
		if !off.UpdatedAt.IsZero() {
			diag.CheckpointAgeSec = int64(serverTime.Sub(off.UpdatedAt).Seconds())
		}
	}

	first, last, count, err := showBinaryLogs(ctx, db)
	if err != nil {
		diag.Error = err.Error()
		return diag
	}

	diag.EarliestFile = first
	diag.LatestFile = last
	diag.AvailableCount = count
	if strings.TrimSpace(first) != "" {
		eventCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		eventTime, eventType, eventErr := firstBinlogEventTime(eventCtx, cfg, first)
		cancel()
		if eventErr != nil {
			diag.EarliestFirstEventError = eventErr.Error()
		} else {
			diag.EarliestFirstEventAt = formatCheckpointTime(eventTime)
			diag.EarliestFirstEvent = eventType
			if !off.UpdatedAt.IsZero() {
				diag.CheckpointToEarliestFirstEventSec = int64(eventTime.Sub(off.UpdatedAt).Seconds())
			}
		}
	}
	if strings.TrimSpace(last) != "" {
		if strings.TrimSpace(first) == strings.TrimSpace(last) {
			diag.LatestFirstEventAt = diag.EarliestFirstEventAt
			diag.LatestFirstEvent = diag.EarliestFirstEvent
			diag.LatestFirstEventError = diag.EarliestFirstEventError
			diag.CheckpointToLatestFirstEventSec = diag.CheckpointToEarliestFirstEventSec
		} else {
			eventCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			eventTime, eventType, eventErr := firstBinlogEventTime(eventCtx, cfg, last)
			cancel()
			if eventErr != nil {
				diag.LatestFirstEventError = eventErr.Error()
			} else {
				diag.LatestFirstEventAt = formatCheckpointTime(eventTime)
				diag.LatestFirstEvent = eventType
				if !off.UpdatedAt.IsZero() {
					diag.CheckpointToLatestFirstEventSec = int64(eventTime.Sub(off.UpdatedAt).Seconds())
				}
			}
		}
	}
	diag.CheckpointAvailable = binlogFileInRange(off.BinlogFile, first, last)
	switch {
	case first == "" || last == "" || count == 0:
		diag.Status = "no_binlogs"
	case diag.CheckpointAvailable:
		diag.Status = "available"
	case strings.TrimSpace(off.BinlogFile) != "" && strings.TrimSpace(first) != "" && strings.Compare(off.BinlogFile, first) < 0:
		diag.Status = "purged"
	default:
		diag.Status = "missing"
	}
	return diag
}

func firstBinlogEventTime(ctx context.Context, cfg config.MySQLConfig, file string) (time.Time, string, error) {
	file = strings.TrimSpace(file)
	if file == "" {
		return time.Time{}, "", fmt.Errorf("binlog file is empty")
	}

	host, port, err := parseMySQLHostPort(cfg.Addr)
	if err != nil {
		return time.Time{}, "", err
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:        diagnosticServerID(),
		Flavor:          "mysql",
		Host:            host,
		Port:            port,
		User:            cfg.User,
		Password:        cfg.Password,
		Charset:         "utf8mb4",
		HeartbeatPeriod: time.Second,
		ReadTimeout:     2 * time.Second,
	})
	defer syncer.Close()

	streamer, err := syncer.StartSync(gomysql.Position{Name: file, Pos: 4})
	if err != nil {
		return time.Time{}, "", err
	}

	for i := 0; i < 8; i++ {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return time.Time{}, "", err
		}
		if ev == nil || ev.Header == nil || ev.Header.Timestamp == 0 {
			continue
		}
		return time.Unix(int64(ev.Header.Timestamp), 0), ev.Header.EventType.String(), nil
	}
	return time.Time{}, "", fmt.Errorf("no timestamped event found in %s", file)
}

func parseMySQLHostPort(addr string) (string, uint16, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "127.0.0.1", 3306, nil
	}

	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.Count(addr, ":") == 0 || strings.Contains(err.Error(), "missing port in address") {
			return addr, 3306, nil
		}
		return "", 0, err
	}
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}

	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	if port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("mysql port out of range: %d", port)
	}
	return host, uint16(port), nil
}

func diagnosticServerID() uint32 {
	return uint32(2000000000 + time.Now().UnixNano()%100000000)
}

func (j *Job) mysqlSourceConfig() (config.MySQLConfig, bool) {
	if j == nil || j.Config == nil {
		return config.MySQLConfig{}, false
	}
	sourceType, sourceCfg := j.pickSource()
	if strings.TrimSpace(sourceType) == "" {
		sourceType = "mysql"
	}
	if !strings.EqualFold(sourceType, "mysql") {
		return config.MySQLConfig{}, false
	}

	switch cfg := sourceCfg.(type) {
	case config.MySQLConfig:
		return config.NormalizeMySQLConfig(cfg), true
	case *config.MySQLConfig:
		if cfg == nil {
			return config.MySQLConfig{}, false
		}
		return config.NormalizeMySQLConfig(*cfg), true
	default:
		b, err := json.Marshal(cfg)
		if err != nil {
			return config.MySQLConfig{}, false
		}
		var out config.MySQLConfig
		if err := json.Unmarshal(b, &out); err != nil {
			return config.MySQLConfig{}, false
		}
		return config.NormalizeMySQLConfig(out), true
	}
}

func showBinaryLogs(ctx context.Context, db *sql.DB) (string, string, int, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return "", "", 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", "", 0, err
	}
	if len(cols) == 0 {
		return "", "", 0, fmt.Errorf("SHOW BINARY LOGS returned no columns")
	}

	values := make([]sql.RawBytes, len(cols))
	scan := make([]any, len(cols))
	for i := range values {
		scan[i] = &values[i]
	}

	first := ""
	last := ""
	count := 0
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return "", "", 0, err
		}
		file := string(values[0])
		if count == 0 {
			first = file
		}
		last = file
		count++
	}
	if err := rows.Err(); err != nil {
		return "", "", 0, err
	}
	return first, last, count, nil
}

func queryMySQLServerTime(ctx context.Context, db *sql.DB) (time.Time, error) {
	var serverTime time.Time
	if err := db.QueryRowContext(ctx, "SELECT NOW(6)").Scan(&serverTime); err != nil {
		return time.Time{}, err
	}
	return serverTime, nil
}

func binlogFileInRange(file, first, last string) bool {
	file = strings.TrimSpace(file)
	first = strings.TrimSpace(first)
	last = strings.TrimSpace(last)
	if file == "" || first == "" || last == "" {
		return false
	}
	return strings.Compare(file, first) >= 0 && strings.Compare(file, last) <= 0
}

func formatCheckpointTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func (j *Job) setStatus(s JobStatus) {
	j.mu.Lock()
	previous, listener, jobID, changed := j.setStatusLocked(s)
	j.mu.Unlock()

	if !changed {
		return
	}
	j.notifyStatusTransition(jobID, previous, s, listener)
}

func (j *Job) setStatusLocked(s JobStatus) (JobStatus, func(JobStatus), string, bool) {
	if j.status == s {
		return j.status, nil, "", false
	}

	previous := j.status
	j.status = s
	j.Updated = time.Now()
	j.syncProgressForStatusLocked(s)

	// keep graph status in-sync (best effort)
	if j.graph != nil {
		j.graph.Status = s
		for i := range j.graph.Nodes {
			j.graph.Nodes[i].Status = s
		}
	}

	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	return previous, j.statusListener, jobID, true
}

func (j *Job) notifyStatusTransition(jobID string, previous, current JobStatus, listener func(JobStatus)) {
	log.Printf("[job %s] status=%s previous_status=%s", jobID, current, previous)
	if listener != nil {
		listener(current)
	}
}

func (j *Job) syncProgressForStatusLocked(status JobStatus) {
	switch status {
	case JobStatusPending:
		if j.progress == nil {
			j.progress = &JobProgress{
				Phase:   "preflight",
				Summary: "Preparing job",
			}
		}
	case JobStatusQueued:
		j.progress = &JobProgress{
			Phase:   "queued",
			Summary: "Waiting for snapshot slot",
		}
	case JobStatusRunning:
		if j.progress == nil {
			j.progress = &JobProgress{
				Phase:   "running",
				Summary: "Job is running",
			}
		}
	case JobStatusPausing:
		j.progress = finalizeJobProgress(j.progress, "pausing", "Draining pending events before pause")
	case JobStatusPaused:
		j.progress = finalizeJobProgress(j.progress, "paused", "Job paused at a committed checkpoint")
	case JobStatusStopped:
		j.progress = finalizeJobProgress(j.progress, "stopped", "Job stopped")
	case JobStatusFailed:
		j.progress = finalizeJobProgress(j.progress, "failed", "Job failed")
	case JobStatusDone:
		j.progress = finalizeJobProgress(j.progress, "done", "Job completed")
	}
}

func finalizeJobProgress(prev *JobProgress, phase, summary string) *JobProgress {
	next := &JobProgress{
		Phase:   phase,
		Summary: summary,
	}
	if prev == nil {
		return next
	}

	*next = *prev
	prevSummary := strings.TrimSpace(prev.Summary)
	next.Phase = phase
	next.Summary = summary
	if strings.TrimSpace(next.Detail) == "" && prevSummary != "" && prevSummary != summary {
		next.Detail = prevSummary
	}
	return next
}

func (j *Job) updateProgress(info connector.ProgressInfo) {
	progress := &JobProgress{
		Phase:                   strings.TrimSpace(info.Phase),
		Summary:                 strings.TrimSpace(info.Summary),
		Detail:                  strings.TrimSpace(info.Detail),
		CurrentTable:            strings.TrimSpace(info.CurrentTable),
		CurrentTableIndex:       info.CurrentTableIndex,
		CompletedTables:         info.CompletedTables,
		TotalTables:             info.TotalTables,
		CurrentTableRows:        info.CurrentTableRows,
		CDCStartFile:            strings.TrimSpace(info.CDCStartFile),
		CDCStartPos:             info.CDCStartPos,
		CDCCurrentFile:          strings.TrimSpace(info.CDCCurrentFile),
		CDCCurrentPos:           info.CDCCurrentPos,
		CheckpointPending:       info.CheckpointPending,
		CheckpointReason:        strings.TrimSpace(info.CheckpointReason),
		CheckpointPosition:      strings.TrimSpace(info.CheckpointPosition),
		CheckpointPendingTables: strings.TrimSpace(info.CheckpointPendingTables),
	}

	j.mu.Lock()
	previousPhase := ""
	if j.progress != nil {
		previousPhase = j.progress.Phase
	}
	if isSinkRuntimeProgress(progress.Phase) && j.progress != nil {
		progress = mergeSinkRuntimeProgress(*j.progress, *progress)
	} else if j.progress != nil {
		progress = mergeSourceRuntimeProgress(*j.progress, *progress)
	}
	j.progress = progress
	j.Updated = time.Now()
	listener := j.progressListener
	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	j.mu.Unlock()

	if progress.Phase != "" && !strings.EqualFold(strings.TrimSpace(previousPhase), progress.Phase) {
		log.Printf("[job %s] progress phase=%s summary=%s detail=%s", jobID, progress.Phase, progress.Summary, progress.Detail)
	}
	if listener != nil {
		listener(progress)
	}
}

func isSinkRuntimeProgress(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "sink_commit", "sink_commit_wait", "sink_checkpoint", "sink_checkpoint_wait":
		return true
	default:
		return false
	}
}

func mergeSinkRuntimeProgress(previous, incoming JobProgress) *JobProgress {
	merged := previous
	if strings.TrimSpace(merged.Phase) == "" || isSinkRuntimeProgress(merged.Phase) {
		merged.Phase = "running"
		if strings.TrimSpace(merged.Summary) == "" || strings.EqualFold(strings.TrimSpace(merged.Summary), strings.TrimSpace(previous.SinkSummary)) {
			merged.Summary = "Job is running"
		}
	}
	merged.SinkPhase = strings.TrimSpace(incoming.Phase)
	merged.SinkSummary = strings.TrimSpace(incoming.Summary)
	merged.SinkDetail = strings.TrimSpace(incoming.Detail)
	if strings.TrimSpace(incoming.CurrentTable) != "" {
		merged.SinkTable = strings.TrimSpace(incoming.CurrentTable)
	}
	merged.SinkRows = incoming.CurrentTableRows
	merged.CheckpointPending = incoming.CheckpointPending
	merged.CheckpointReason = incoming.CheckpointReason
	merged.CheckpointPosition = incoming.CheckpointPosition
	merged.CheckpointPendingTables = incoming.CheckpointPendingTables
	return &merged
}

func mergeSourceRuntimeProgress(previous, incoming JobProgress) *JobProgress {
	merged := incoming
	merged.SinkPhase = previous.SinkPhase
	merged.SinkSummary = previous.SinkSummary
	merged.SinkDetail = previous.SinkDetail
	merged.SinkTable = previous.SinkTable
	merged.SinkRows = previous.SinkRows
	merged.CheckpointPending = previous.CheckpointPending
	merged.CheckpointReason = previous.CheckpointReason
	merged.CheckpointPosition = previous.CheckpointPosition
	merged.CheckpointPendingTables = previous.CheckpointPendingTables
	return &merged
}

func (j *Job) addError(component string, err error) {
	if err == nil {
		return
	}
	j.mu.Lock()
	j.errors = append(j.errors, JobError{
		Component: component,
		Message:   err.Error(),
		Time:      time.Now(),
	})
	j.Updated = time.Now()
	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	j.mu.Unlock()
	log.Printf("[job %s] %s error: %v", jobID, component, err)
}

func (j *Job) setCancel(cancel context.CancelFunc) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.cancelFunc = cancel
}

func (j *Job) setSourceCancel(cancel context.CancelFunc) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.sourceCancelFunc = cancel
}

func (j *Job) takeSourceCancel() context.CancelFunc {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.sourceCancelFunc
	j.sourceCancelFunc = nil
	return c
}

func (j *Job) setPauseRequested(requested bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.pauseRequested = requested
}

func (j *Job) isPauseRequested() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.pauseRequested
}

func (j *Job) setRunDone(done chan struct{}) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.runDone = done
}

func (j *Job) takeCancel() context.CancelFunc {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.cancelFunc
	j.cancelFunc = nil
	return c
}

func (j *Job) currentRunDone() chan struct{} {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.runDone
}

func (j *Job) waitRunDone(timeout time.Duration) bool {
	done := j.currentRunDone()
	if done == nil {
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (j *Job) runActive() bool {
	done := j.currentRunDone()
	if done == nil {
		return false
	}

	select {
	case <-done:
		return false
	default:
		return true
	}
}

func (j *Job) setStatusListener(listener func(JobStatus)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.statusListener = listener
}

func (j *Job) setProgressListener(listener func(*JobProgress)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.progressListener = listener
}

func (j *Job) failStart(component string, err error, cancel context.CancelFunc) error {
	if err == nil {
		return nil
	}
	j.addError(component, err)
	j.setStatus(JobStatusFailed)
	if cancel != nil {
		cancel()
	}
	return err
}

func (j *Job) recoverPipelinePanic(component string, cancel context.CancelFunc) {
	recovered := recover()
	if recovered == nil {
		return
	}

	err := fmt.Errorf("%s panic: %v", component, recovered)
	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	log.Printf("[job %s] %s panic: %v\n%s", jobID, component, recovered, debug.Stack())
	j.addError(component, err)
	j.setStatus(JobStatusFailed)
	if cancel != nil {
		cancel()
	}
}

// requestStop makes cancellation visible immediately while the pipeline drains.
func (j *Job) requestStop() bool {
	cancel := j.takeCancel()
	if cancel != nil {
		cancel()
	}
	if sourceCancel := j.takeSourceCancel(); sourceCancel != nil {
		sourceCancel()
	}

	st := j.GetStatus()
	if st == JobStatusDone || st == JobStatusFailed {
		return false
	}
	j.setStatus(JobStatusStopped)
	return true
}

// RequestPause stops only the source. The source goroutine then closes the
// event channel, allowing the sink to drain, flush, and commit its checkpoint.
func (j *Job) RequestPause() bool {
	j.mu.Lock()
	if j.status != JobStatusRunning || j.sourceCancelFunc == nil {
		j.mu.Unlock()
		return false
	}

	sourceCancel := j.sourceCancelFunc
	j.sourceCancelFunc = nil
	j.pauseRequested = true
	previous, listener, jobID, changed := j.setStatusLocked(JobStatusPausing)
	j.mu.Unlock()

	if changed {
		j.notifyStatusTransition(jobID, previous, JobStatusPausing, listener)
	}
	sourceCancel()
	return true
}

// Stop waits for full pipeline shutdown after exposing STOPPED immediately.
func (j *Job) Stop() {
	if !j.requestStop() {
		return
	}
	if !j.waitRunDone(30 * time.Second) {
		log.Printf("[job %s] stop timeout waiting for pipeline shutdown", j.Config.ID)
	}
}

func (j *Job) StopAsync() {
	if !j.requestStop() {
		return
	}
	go func() {
		if !j.waitRunDone(30 * time.Second) {
			log.Printf("[job %s] stop timeout waiting for pipeline shutdown", j.Config.ID)
		}
	}()
}

// IMPORTANT: delete meta state when job is deleted from manager/UI
func (j *Job) CleanupMeta() {
	j.mu.RLock()
	store := j.metaStore
	key := j.metaKey
	j.mu.RUnlock()

	if store == nil || key == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = store.DeleteJobState(ctx, key)
}

// ---- NEW: Graph accessor ----
func (j *Job) Graph() *JobGraph {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.buildGraphLocked()
}

// ---- connector selection ----

func (j *Job) pickSource() (typ string, cfg any) {
	// v2 preferred
	if j.Config.Source != nil && strings.TrimSpace(j.Config.Source.Type) != "" {
		return strings.ToLower(strings.TrimSpace(j.Config.Source.Type)), j.Config.Source.Config
	}
	// legacy fallback
	return "mysql", j.Config.MySQL
}

func (j *Job) pickSink() (typ string, cfg any) {
	if j.Config.Sink != nil && strings.TrimSpace(j.Config.Sink.Type) != "" {
		return strings.ToLower(strings.TrimSpace(j.Config.Sink.Type)), j.Config.Sink.Config
	}
	return "doris", j.Config.Doris
}

// ---- preflight generic (capability-based) ----

func (j *Job) preflight(ctx context.Context, src connector.Source, sink connector.Sink, mode config.JobMode) error {
	lister, hasLister := src.(connector.TableLister)
	sp, hasSP := src.(connector.SchemaProvider)
	tm, hasTM := sink.(connector.TableManager)
	sc, hasSC := sink.(connector.SchemaConsumer)
	countResume := snapshotCountResumeSupported(mode) && snapshotOnlyCountResumeEnabled(j.Config.Metadata)
	pkSkipper, skipMissingPK := sink.(connector.SnapshotPrimaryKeySkipper)
	skipMissingPK = mode == config.JobModeSnapshotOnly && skipMissingPK

	if countResume && !hasTM {
		return fmt.Errorf("snapshot count resume requires sink connector table manager support")
	}
	if !hasTM && !hasSC {
		return nil
	}
	if !hasLister || !hasSP {
		return fmt.Errorf("source connector does not support schema discovery required by sink")
	}

	var sourceCounter connector.TableRowCounter
	var targetCounter connector.TargetTableRowCounter
	var targetResetter connector.TargetTableResetter
	var snapshotSkipper connector.SnapshotTableSkipper
	if countResume {
		var ok bool
		snapshotSkipper, ok = src.(connector.SnapshotTableSkipper)
		if !ok {
			return fmt.Errorf("snapshot count resume requires source connector snapshot skip support")
		}
	}
	if countResume {
		var ok bool
		sourceCounter, ok = src.(connector.TableRowCounter)
		if !ok {
			return fmt.Errorf("snapshot count resume requires source connector row count support")
		}
		targetCounter, ok = sink.(connector.TargetTableRowCounter)
		if !ok {
			return fmt.Errorf("snapshot count resume requires sink connector row count support")
		}
		targetResetter, ok = sink.(connector.TargetTableResetter)
		if !ok {
			return fmt.Errorf("snapshot count resume requires sink connector table reset support")
		}
	}

	var resolver connector.TargetResolver
	if tr, ok := sink.(connector.TargetResolver); ok {
		resolver = tr
	}

	type countResumeTarget struct {
		schema     string
		table      string
		sourceRows int64
		sources    []connector.TableRef
	}

	countTargets := make(map[string]*countResumeTarget)
	skipTables := make([]connector.TableRef, 0)
	for _, t := range lister.Tables() {
		schema, err := sp.FetchSchema(ctx, t.Schema, t.Table)
		if err != nil {
			return err
		}
		if skipMissingPK && pkSkipper.SkipSnapshotTableWithoutPrimaryKey(t.Schema, t.Table, schema) {
			if snapshotSkipper == nil {
				var ok bool
				snapshotSkipper, ok = src.(connector.SnapshotTableSkipper)
				if !ok {
					return fmt.Errorf("snapshot-only missing primary key skip requires source connector snapshot skip support")
				}
			}
			log.Printf("[job %s] snapshot-only skip %s.%s because source table has no primary key", j.Config.ID, t.Schema, t.Table)
			skipTables = append(skipTables, t)
			continue
		}

		if hasSC {
			if err := sc.RegisterSourceSchema(t.Schema, t.Table, schema); err != nil {
				return err
			}
		}

		if hasTM {
			targetDB, targetTbl := t.Schema, t.Table
			if resolver != nil {
				targetDB, targetTbl = resolver.ResolveTarget(t.Schema, t.Table)
			}

			if err := tm.EnsureTable(ctx, targetDB, targetTbl, schema); err != nil {
				return err
			}

			if countResume {
				sourceRows, err := sourceCounter.CountRows(ctx, t.Schema, t.Table)
				if err != nil {
					return fmt.Errorf("count source rows failed %s.%s: %w", t.Schema, t.Table, err)
				}
				targetName := strings.ToLower(strings.TrimSpace(targetDB + "." + targetTbl))
				group := countTargets[targetName]
				if group == nil {
					group = &countResumeTarget{
						schema: targetDB,
						table:  targetTbl,
					}
					countTargets[targetName] = group
				}
				group.sourceRows += sourceRows
				group.sources = append(group.sources, t)
			}
		}
	}
	if countResume {
		for targetName, group := range countTargets {
			targetRows, err := targetCounter.CountTargetRows(ctx, group.schema, group.table)
			if err != nil {
				return fmt.Errorf("count target rows failed %s.%s: %w", group.schema, group.table, err)
			}

			if group.sourceRows == targetRows {
				log.Printf("[job %s] snapshot count resume skip target=%s sources=%d rows=%d", j.Config.ID, targetName, len(group.sources), group.sourceRows)
				skipTables = append(skipTables, group.sources...)
				continue
			}
			if targetRows > 0 {
				log.Printf("[job %s] snapshot count resume reset target=%s source_rows=%d target_rows=%d sources=%d", j.Config.ID, targetName, group.sourceRows, targetRows, len(group.sources))
				if err := targetResetter.ResetTargetTable(ctx, group.schema, group.table); err != nil {
					return fmt.Errorf("reset target table failed %s.%s: %w", group.schema, group.table, err)
				}
			}
		}
	}
	if snapshotSkipper != nil {
		snapshotSkipper.SkipSnapshotTables(skipTables)
	}
	return nil
}

func (j *Job) Start() error {
	return j.startWithMode(j.Config.Mode)
}

func (j *Job) Resume() error {
	return j.startWithMode(config.JobModeResume)
}

func (j *Job) startWithMode(mode config.JobMode) (err error) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}

		err = fmt.Errorf("job start panic: %v", recovered)
		jobID := ""
		if j.Config != nil {
			jobID = j.Config.ID
		}
		log.Printf("[job %s] start panic: %v\n%s", jobID, recovered, debug.Stack())
		j.addError("system", err)
		j.setStatus(JobStatusFailed)
		if cancel := j.takeCancel(); cancel != nil {
			cancel()
		}
	}()

	st := j.GetStatus()
	if st == JobStatusRunning || st == JobStatusPending || st == JobStatusPausing {
		return nil
	}
	if st == JobStatusDone {
		return fmt.Errorf("job already DONE")
	}

	if j.registry == nil {
		err := fmt.Errorf("job registry is nil (JobManager must pass registry to NewJob)")
		j.addError("system", err)
		j.setStatus(JobStatusFailed)
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	j.setCancel(cancel)
	j.setPauseRequested(false)
	sourceCtx, sourceCancel := context.WithCancel(ctx)
	j.setSourceCancel(sourceCancel)

	// PENDING = fase setup/preflight
	j.setStatus(JobStatusPending)
	j.updateProgress(connector.ProgressInfo{
		Phase:   "preflight",
		Summary: "Preparing job",
		Detail:  "Validating source schema and target tables",
	})

	events := make(chan model.Event, j.Config.BufferSize)

	// init meta store
	if j.Config.Meta.MySQLDSN != "" {
		store, err := meta.NewMySQLOffsetStore(j.Config.Meta.MySQLDSN)
		if err != nil {
			return j.failStart("system", err, cancel)
		}
		if err := store.Init(ctx); err != nil {
			return j.failStart("system", err, cancel)
		}
		j.metaStore = store
	}

	// pick connectors
	srcType, srcCfg := j.pickSource()
	sinkType, sinkCfg := j.pickSink()

	// build new metaKey (allowed to change)
	effectiveMode := normalizeMode(mode)
	storedMode := normalizeMode(j.Config.Mode)
	log.Printf("[job %s] start mode=%s source=%s sink=%s", j.Config.ID, effectiveMode, srcType, sinkType)
	metaKey := buildMetaKey(
		j.Config.ID,
		string(storedMode),
		srcType, srcCfg,
		sinkType, sinkCfg,
	)

	// store metakey
	j.mu.Lock()
	j.metaKey = metaKey
	j.mu.Unlock()

	jctx := connector.JobContext{
		JobID:      j.Config.ID,
		JobName:    j.Config.Name,
		MetaKey:    metaKey,
		Mode:       effectiveMode,
		StoredMode: storedMode,
		SinkType:   sinkType,
		Retry:      j.Config.Retry,
		MetaStore:  j.metaStore,
		Metadata:   j.Config.Metadata,
		ReportProgress: func(info connector.ProgressInfo) {
			j.updateProgress(info)
		},
	}

	// instantiate source/sink from registry
	src, err := j.registry.NewSource(srcType, jctx, srcCfg)
	if err != nil {
		return j.failStart("source", err, cancel)
	}

	sink, err := j.registry.NewSink(sinkType, jctx, sinkCfg)
	if err != nil {
		return j.failStart("sink", err, cancel)
	}

	// build minimal graph snapshot (refreshed on-demand by Graph())
	j.mu.Lock()
	j.graph = &JobGraph{
		JobID:  j.Config.ID,
		Status: j.status,
		Nodes: []GraphNode{
			{ID: "source:" + srcType, Type: NodeSource, Label: "Source (" + srcType + ")", Status: j.status},
			{ID: "buffer:events", Type: NodeBuffer, Label: "Event Buffer", Status: j.status},
			{ID: "sink:" + sinkType, Type: NodeSink, Label: "Sink (" + sinkType + ")", Status: j.status},
		},
		Edges: []GraphEdge{
			{From: "source:" + srcType, To: "buffer:events"},
			{From: "buffer:events", To: "sink:" + sinkType},
		},
	}
	j.mu.Unlock()

	runDone := make(chan struct{})
	var runWG sync.WaitGroup
	runWG.Add(2)
	j.setRunDone(runDone)
	go func() {
		runWG.Wait()
		close(runDone)
	}()

	// source goroutine
	go func() {
		defer runWG.Done()
		defer close(events)
		defer j.recoverPipelinePanic("source", cancel)

		// Preflight ensure table (generic)
		if err := j.preflight(ctx, src, sink, effectiveMode); err != nil {
			j.addError("system", err)
			j.setStatus(JobStatusFailed)
			cancel()
			return
		}

		// Preflight sukses => RUNNING (kalau belum di-stop)
		if ctx.Err() != nil {
			return
		}
		j.setStatus(JobStatusRunning)

		if err := src.Run(sourceCtx, events); err != nil {
			// Pause cancels only the source so the sink can drain normally.
			if sourceCtx.Err() != nil && j.isPauseRequested() {
				return
			}
			// Kalau error karena cancel (Stop), jangan FAILED
			if ctx.Err() != nil {
				return
			}
			j.addError("source", err)
			j.setStatus(JobStatusFailed)
			cancel()
			return
		}
	}()

	// sink goroutine
	go func() {
		defer runWG.Done()
		defer j.recoverPipelinePanic("sink", cancel)
		if err := sink.Run(ctx, events); err != nil {
			// kalau stop/cancel, jangan FAILED
			if ctx.Err() != nil {
				return
			}
			j.addError("sink", err)
			j.setStatus(JobStatusFailed)
			cancel()
			return
		}

		// sink selesai normal
		if ctx.Err() == nil {
			if j.isPauseRequested() {
				j.setStatus(JobStatusPaused)
			} else {
				j.setStatus(JobStatusDone)
			}
		}
	}()

	return nil
}

func normalizeMode(m config.JobMode) config.JobMode {
	switch m {
	case config.JobModeInitial, config.JobModeSnapshotOnly, config.JobModeResume, config.JobModeLatestOffset, config.JobModeLatest:
		return m
	default:
		return config.JobModeInitial
	}
}

func snapshotOnlyCountResumeEnabled(metadata map[string]string) bool {
	return metadataBool(metadata, "snapshot_only_count_resume")
}

func snapshotCountResumeSupported(mode config.JobMode) bool {
	return mode == config.JobModeInitial || mode == config.JobModeSnapshotOnly
}

func metadataBool(metadata map[string]string, key string) bool {
	if len(metadata) == 0 {
		return false
	}
	raw, ok := metadata[key]
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on", "enabled":
		return true
	default:
		return false
	}
}
