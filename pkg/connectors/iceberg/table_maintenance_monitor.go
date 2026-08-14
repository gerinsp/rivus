package iceberg

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

type tableMaintenanceMonitor struct {
	jobID   string
	jobName string
	cfg     config.IcebergConfig
	sink    *Sink

	mu             sync.Mutex
	states         map[string]*tableMaintenanceWatchState
	wake           chan struct{}
	paused         bool
	statusReporter connector.TableMaintenanceStatusReporter
}

type tableMaintenanceWatchState struct {
	target              config.IcebergTarget
	initialized         bool
	lastSequenceNumber  int64
	newDataFiles        int
	newEqualityDeletes  int
	activeSmallFiles    int
	activeSmallBytes    int64
	activeDataFiles     int
	activeEqDeletes     int
	eligibilityReady    bool
	eligibilityDirty    bool
	lastCheckedAt       time.Time
	lastInventoryError  string
	lastExpireSnapshots time.Time
	lastOrphanCleanup   time.Time
	active              *activeTableMaintenance
}

type activeTableMaintenance struct {
	submissionID       string
	operations         []string
	submittedDataFiles int
	submittedEqDeletes int
	previousExpireAt   time.Time
	previousOrphanAt   time.Time
}

func newTableMaintenanceMonitor(jobID, jobName string, cfg config.IcebergConfig, sink *Sink) *tableMaintenanceMonitor {
	if !cfg.TableMaintenance.Enabled {
		return nil
	}
	return &tableMaintenanceMonitor{
		jobID:   jobID,
		jobName: jobName,
		cfg:     cfg,
		sink:    sink,
		states:  make(map[string]*tableMaintenanceWatchState),
		wake:    make(chan struct{}, 1),
	}
}

func (m *tableMaintenanceMonitor) pauseSubmissions() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.paused = true
	m.mu.Unlock()
	m.publishStatus()
}

func (m *tableMaintenanceMonitor) resumeSubmissions() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.paused = false
	m.mu.Unlock()
	m.publishStatus()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *tableMaintenanceMonitor) setStatusReporter(reporter connector.TableMaintenanceStatusReporter) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.statusReporter = reporter
	m.mu.Unlock()
	m.publishStatus()
}

func validateAutomaticTableMaintenanceConfig(cfg config.IcebergConfig) error {
	maintenanceCfg := cfg.TableMaintenance
	if !maintenanceCfg.Enabled {
		return nil
	}
	if err := validateMaintenanceBackend(maintenanceCfg); err != nil {
		return err
	}
	if catalogName := maintenanceCatalogName(cfg); !sparkCatalogNamePattern.MatchString(catalogName) {
		return fmt.Errorf("invalid Spark Iceberg catalog name %q", catalogName)
	}
	if maintenanceCfg.OrphanCleanupIntervalSeconds > 0 && maintenanceCfg.OrphanCleanupOlderThanHours < defaultOrphanCleanupOlderThan.Hours() {
		return fmt.Errorf("iceberg table_maintenance.orphan_cleanup_older_than_hours must be at least 72 while streaming")
	}
	if maintenanceCfg.SmallFileSizeBytes <= 0 {
		return fmt.Errorf("iceberg table_maintenance.small_file_size_bytes must be greater than zero")
	}
	if maintenanceCfg.SmallFilesMinCount <= 0 {
		return fmt.Errorf("iceberg table_maintenance.small_files_min_count must be greater than zero")
	}
	if maintenanceCfg.SmallFilesMinTotalBytes <= 0 {
		return fmt.Errorf("iceberg table_maintenance.small_files_min_total_bytes must be greater than zero")
	}
	return nil
}

func (m *tableMaintenanceMonitor) registerTable(target config.IcebergTarget, tbl *icetable.Table, now time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	state := m.stateLocked(target, now)
	if !state.initialized {
		state.initialized = true
		state.eligibilityDirty = true
		if tbl != nil && tbl.CurrentSnapshot() != nil {
			state.lastSequenceNumber = tbl.CurrentSnapshot().SequenceNumber
		}
	}
	m.mu.Unlock()
	m.publishStatus()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *tableMaintenanceMonitor) observeTable(target config.IcebergTarget, tbl *icetable.Table, now time.Time) {
	if m == nil || tbl == nil {
		return
	}
	key := tableKey(target.Namespace, target.Table)
	m.mu.Lock()
	state := m.states[key]
	if state == nil {
		state = m.stateLocked(target, now)
		state.initialized = true
	}
	snapshots := append([]icetable.Snapshot(nil), tbl.Metadata().Snapshots()...)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].SequenceNumber < snapshots[j].SequenceNumber
	})
	for _, snapshot := range snapshots {
		if snapshot.SequenceNumber <= state.lastSequenceNumber {
			continue
		}
		state.lastSequenceNumber = snapshot.SequenceNumber
		if snapshot.Summary == nil || snapshot.Summary.Properties == nil || snapshot.Summary.Properties["rivus.job_id"] != m.jobID {
			continue
		}
		addedDataFiles := snapshotSummaryCount(snapshot.Summary.Properties, "added-data-files")
		addedEqualityDeletes := snapshotSummaryCount(snapshot.Summary.Properties, "added-equality-delete-files")
		state.newDataFiles += addedDataFiles
		state.newEqualityDeletes += addedEqualityDeletes
		if addedDataFiles > 0 || addedEqualityDeletes > 0 {
			state.eligibilityDirty = true
		}
	}
	m.mu.Unlock()
	m.publishStatus()

	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *tableMaintenanceMonitor) stateLocked(target config.IcebergTarget, now time.Time) *tableMaintenanceWatchState {
	key := tableKey(target.Namespace, target.Table)
	state := m.states[key]
	if state == nil {
		state = &tableMaintenanceWatchState{
			target:              target,
			lastExpireSnapshots: now,
			lastOrphanCleanup:   now,
		}
		m.states[key] = state
	}
	return state
}

func snapshotSummaryCount(properties map[string]string, key string) int {
	value, err := strconv.Atoi(strings.TrimSpace(properties[key]))
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func (m *tableMaintenanceMonitor) run(ctx context.Context) {
	if m == nil {
		return
	}
	interval := time.Duration(m.cfg.TableMaintenance.PollIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[iceberg][job %s] automatic table maintenance enabled poll=%s data_files=%d equality_delete_files=%d small_file_below_bytes=%d small_files=%d small_bytes=%d expire=%s orphan=%s",
		m.jobID,
		interval,
		m.cfg.TableMaintenance.DataFilesThreshold,
		m.cfg.TableMaintenance.EqualityDeleteFilesThreshold,
		m.cfg.TableMaintenance.SmallFileSizeBytes,
		m.cfg.TableMaintenance.SmallFilesMinCount,
		m.cfg.TableMaintenance.SmallFilesMinTotalBytes,
		durationOrDisabled(m.cfg.TableMaintenance.ExpireSnapshotsIntervalSeconds),
		durationOrDisabled(m.cfg.TableMaintenance.OrphanCleanupIntervalSeconds),
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			m.refreshDirtyEligibility(ctx)
			m.submitDue(ctx, time.Now())
		case now := <-ticker.C:
			m.refreshDirtyEligibility(ctx)
			m.pollActive(ctx)
			m.submitDue(ctx, now)
		}
	}
}

func durationOrDisabled(seconds int) string {
	if seconds < 0 {
		return "disabled"
	}
	return (time.Duration(seconds) * time.Second).String()
}

func (m *tableMaintenanceMonitor) refreshDirtyEligibility(ctx context.Context) {
	m.mu.Lock()
	if m.paused {
		m.mu.Unlock()
		return
	}
	targets := make([]config.IcebergTarget, 0, len(m.states))
	for _, state := range m.states {
		if state != nil && state.eligibilityDirty && state.active == nil {
			state.eligibilityDirty = false
			targets = append(targets, state.target)
		}
	}
	m.mu.Unlock()
	sort.Slice(targets, func(i, j int) bool {
		return tableKey(targets[i].Namespace, targets[i].Table) < tableKey(targets[j].Namespace, targets[j].Table)
	})

	for _, target := range targets {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		var dataFiles, smallFiles, equalityDeletes int
		var smallBytes int64
		tbl, err := m.sink.catalog.LoadTable(requestCtx, namespaceIdentifier(target.Namespace, target.Table))
		if err == nil {
			dataFiles, smallFiles, smallBytes, equalityDeletes, err = activeFileEligibility(requestCtx, tbl, m.cfg.TableMaintenance.SmallFileSizeBytes)
			if err == nil {
				m.mu.Lock()
				state := m.states[tableKey(target.Namespace, target.Table)]
				if state != nil {
					state.activeDataFiles = dataFiles
					state.activeSmallFiles = smallFiles
					state.activeSmallBytes = smallBytes
					state.activeEqDeletes = equalityDeletes
					state.eligibilityReady = true
					state.lastCheckedAt = time.Now().UTC()
					state.lastInventoryError = ""
				}
				m.mu.Unlock()
			}
		}
		cancel()
		if err != nil {
			m.mu.Lock()
			if state := m.states[tableKey(target.Namespace, target.Table)]; state != nil {
				state.eligibilityDirty = true
				state.lastInventoryError = err.Error()
			}
			m.mu.Unlock()
			m.publishStatus()
			log.Printf("[iceberg][job %s] maintenance eligibility target=%s error=%v", m.jobID, tableKey(target.Namespace, target.Table), err)
			continue
		}
		m.publishStatus()
		log.Printf("[iceberg][job %s] maintenance eligibility target=%s data_files=%d small_files=%d small_bytes=%d equality_deletes=%d",
			m.jobID, tableKey(target.Namespace, target.Table), dataFiles, smallFiles, smallBytes, equalityDeletes)
	}
}

func activeFileEligibility(ctx context.Context, tbl *icetable.Table, smallFileSizeBytes int64) (int, int, int64, int, error) {
	if tbl == nil || tbl.CurrentSnapshot() == nil {
		return 0, 0, 0, 0, nil
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	dataFiles := 0
	smallFiles := 0
	var smallBytes int64
	equalityDeletePaths := make(map[string]struct{})
	for _, task := range tasks {
		if task.File != nil {
			dataFiles++
			if task.File.FileSizeBytes() < smallFileSizeBytes {
				smallFiles++
				smallBytes += task.File.FileSizeBytes()
			}
		}
		for _, deleteFile := range task.EqualityDeleteFiles {
			if deleteFile != nil {
				equalityDeletePaths[deleteFile.FilePath()] = struct{}{}
			}
		}
	}
	return dataFiles, smallFiles, smallBytes, len(equalityDeletePaths), nil
}

func (m *tableMaintenanceMonitor) submitDue(ctx context.Context, now time.Time) {
	for {
		m.mu.Lock()
		if m.activeCountLocked() >= m.cfg.TableMaintenance.MaxConcurrentJobs {
			m.mu.Unlock()
			return
		}
		key, target, operations := m.nextDueLocked(now)
		m.mu.Unlock()
		if key == "" {
			return
		}

		statements := make([]maintenanceStatement, 0, len(operations))
		operationNames := make([]string, 0, len(operations))
		for _, operation := range operations {
			sql, err := buildMaintenanceSQL(maintenanceCatalogName(m.cfg), target, operation.Type, operation.Options)
			if err != nil {
				log.Printf("[iceberg][job %s] build automatic maintenance target=%s operation=%s error=%v", m.jobID, key, operation.Type, err)
				return
			}
			operationNames = append(operationNames, operation.Type)
			statements = append(statements, maintenanceStatement{Operation: operation.Type, Table: key, SQL: sql})
		}

		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		submission, err := submitPreparedTableMaintenance(requestCtx, m.jobID, m.jobName, m.cfg, []config.IcebergTarget{target}, operationNames, statements)
		cancel()
		if err != nil {
			log.Printf("[iceberg][job %s] automatic maintenance submit target=%s operations=%s error=%v", m.jobID, key, strings.Join(operationNames, ","), err)
			return
		}

		m.mu.Lock()
		state := m.states[key]
		if state != nil && state.active == nil {
			active := &activeTableMaintenance{
				submissionID:     submission.SubmissionID,
				operations:       operationNames,
				previousExpireAt: state.lastExpireSnapshots,
				previousOrphanAt: state.lastOrphanCleanup,
			}
			if containsString(operationNames, "rewrite_data_files") {
				active.submittedDataFiles = state.newDataFiles
				active.submittedEqDeletes = state.newEqualityDeletes
				state.newDataFiles = 0
				state.newEqualityDeletes = 0
			}
			if containsString(operationNames, "expire_snapshots") {
				state.lastExpireSnapshots = now
			}
			if containsString(operationNames, "remove_orphan_files") {
				state.lastOrphanCleanup = now
			}
			state.active = active
		}
		m.mu.Unlock()
		m.publishStatus()
		log.Printf("[iceberg][job %s] automatic maintenance submitted target=%s operations=%s spark_submission_id=%s",
			m.jobID, key, strings.Join(operationNames, ","), submission.SubmissionID)
	}
}

func (m *tableMaintenanceMonitor) nextDueLocked(now time.Time) (string, config.IcebergTarget, []TableMaintenanceOperation) {
	if m.paused {
		return "", config.IcebergTarget{}, nil
	}
	keys := make([]string, 0, len(m.states))
	for key := range m.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		state := m.states[key]
		if state == nil || !state.initialized || state.active != nil {
			continue
		}
		operations := automaticOperationsDue(state, m.cfg.TableMaintenance, now)
		if len(operations) > 0 {
			return key, state.target, operations
		}
	}
	return "", config.IcebergTarget{}, nil
}

func automaticOperationsDue(state *tableMaintenanceWatchState, cfg config.IcebergTableMaintenanceConfig, now time.Time) []TableMaintenanceOperation {
	if state == nil {
		return nil
	}
	dataDue := cfg.DataFilesThreshold > 0 &&
		state.newDataFiles >= cfg.DataFilesThreshold &&
		state.eligibilityReady &&
		state.activeSmallFiles >= cfg.SmallFilesMinCount &&
		state.activeSmallBytes >= cfg.SmallFilesMinTotalBytes
	equalityDue := cfg.EqualityDeleteFilesThreshold > 0 &&
		state.newEqualityDeletes >= cfg.EqualityDeleteFilesThreshold &&
		state.eligibilityReady &&
		state.activeEqDeletes >= cfg.EqualityDeleteFilesThreshold
	operations := make([]TableMaintenanceOperation, 0, 3)
	if dataDue || equalityDue {
		options := copyAnyMap(cfg.CompactOptions)
		if equalityDue {
			rewriteOptions := stringAnyMap(options["options"])
			if rewriteOptions == nil {
				rewriteOptions = make(map[string]any)
			}
			if _, exists := rewriteOptions["delete-file-threshold"]; !exists {
				rewriteOptions["delete-file-threshold"] = "1"
			}
			if _, exists := rewriteOptions["remove-dangling-deletes"]; !exists {
				rewriteOptions["remove-dangling-deletes"] = "true"
			}
			options["options"] = rewriteOptions
		}
		operations = append(operations, TableMaintenanceOperation{Type: "rewrite_data_files", Options: options})
	}
	if cfg.ExpireSnapshotsIntervalSeconds > 0 && now.Sub(state.lastExpireSnapshots) >= time.Duration(cfg.ExpireSnapshotsIntervalSeconds)*time.Second {
		options := map[string]any{
			"older_than":     now.Add(-time.Duration(cfg.ExpireSnapshotsOlderThanHours * float64(time.Hour))).UTC().Format(time.RFC3339),
			"retain_last":    cfg.ExpireSnapshotsRetainLast,
			"stream_results": true,
		}
		operations = append(operations, TableMaintenanceOperation{Type: "expire_snapshots", Options: options})
	}
	if cfg.OrphanCleanupIntervalSeconds > 0 && now.Sub(state.lastOrphanCleanup) >= time.Duration(cfg.OrphanCleanupIntervalSeconds)*time.Second {
		options := map[string]any{
			"older_than":     now.Add(-time.Duration(cfg.OrphanCleanupOlderThanHours * float64(time.Hour))).UTC().Format(time.RFC3339),
			"dry_run":        false,
			"stream_results": true,
		}
		operations = append(operations, TableMaintenanceOperation{Type: "remove_orphan_files", Options: options})
	}
	return operations
}

func copyAnyMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func stringAnyMap(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return copyAnyMap(typed)
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	default:
		return nil
	}
}

func (m *tableMaintenanceMonitor) activeCountLocked() int {
	count := 0
	for _, state := range m.states {
		if state != nil && state.active != nil {
			count++
		}
	}
	return count
}

func (m *tableMaintenanceMonitor) pollActive(ctx context.Context) {
	type activePoll struct {
		key          string
		submissionID string
	}
	m.mu.Lock()
	active := make([]activePoll, 0)
	for key, state := range m.states {
		if state != nil && state.active != nil {
			active = append(active, activePoll{key: key, submissionID: state.active.submissionID})
		}
	}
	m.mu.Unlock()

	for _, item := range active {
		requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		status, err := getTableMaintenanceStatus(requestCtx, m.cfg, item.submissionID)
		cancel()
		if err != nil {
			log.Printf("[iceberg][job %s] automatic maintenance status target=%s submission=%s error=%v", m.jobID, item.key, item.submissionID, err)
			continue
		}
		if !status.Success {
			m.finishActive(item.key, item.submissionID, false)
			log.Printf("[iceberg][job %s] automatic maintenance status rejected target=%s submission=%s message=%q",
				m.jobID, item.key, item.submissionID, status.Message)
			continue
		}
		if !sparkDriverTerminal(status.DriverState) {
			continue
		}
		succeeded := strings.EqualFold(status.DriverState, "FINISHED") && status.Success
		m.finishActive(item.key, item.submissionID, succeeded)
		log.Printf("[iceberg][job %s] automatic maintenance finished target=%s submission=%s state=%s success=%t",
			m.jobID, item.key, item.submissionID, status.DriverState, succeeded)
	}
}

func sparkDriverTerminal(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "FINISHED", "FAILED", "ERROR", "KILLED":
		return true
	default:
		return false
	}
}

func (m *tableMaintenanceMonitor) finishActive(key, submissionID string, succeeded bool) {
	m.mu.Lock()
	state := m.states[key]
	if state == nil || state.active == nil || state.active.submissionID != submissionID {
		m.mu.Unlock()
		return
	}
	refreshEligibility := succeeded && containsString(state.active.operations, "rewrite_data_files")
	if !succeeded {
		state.newDataFiles += state.active.submittedDataFiles
		state.newEqualityDeletes += state.active.submittedEqDeletes
		if containsString(state.active.operations, "expire_snapshots") {
			state.lastExpireSnapshots = state.active.previousExpireAt
		}
		if containsString(state.active.operations, "remove_orphan_files") {
			state.lastOrphanCleanup = state.active.previousOrphanAt
		}
	}
	if refreshEligibility {
		state.eligibilityReady = false
		state.eligibilityDirty = true
	}
	state.active = nil
	m.mu.Unlock()
	m.publishStatus()
	if refreshEligibility {
		select {
		case m.wake <- struct{}{}:
		default:
		}
	}
}

func (m *tableMaintenanceMonitor) publishStatus() {
	if m == nil {
		return
	}
	m.mu.Lock()
	reporter := m.statusReporter
	status := m.statusLocked(time.Now().UTC())
	m.mu.Unlock()
	if reporter != nil {
		reporter(status)
	}
}

func (m *tableMaintenanceMonitor) statusLocked(now time.Time) *connector.TableMaintenanceStatus {
	cfg := m.cfg.TableMaintenance
	status := &connector.TableMaintenanceStatus{
		Enabled:                      cfg.Enabled,
		State:                        "watching",
		CatalogName:                  maintenanceCatalogName(m.cfg),
		RunnerResourceProfile:        cfg.RunnerResourceProfile,
		MaxConcurrentJobs:            cfg.MaxConcurrentJobs,
		DataFilesThreshold:           cfg.DataFilesThreshold,
		EqualityDeleteFilesThreshold: cfg.EqualityDeleteFilesThreshold,
		SmallFileSizeBytes:           cfg.SmallFileSizeBytes,
		SmallFilesMinCount:           cfg.SmallFilesMinCount,
		SmallFilesMinTotalBytes:      cfg.SmallFilesMinTotalBytes,
		Paused:                       m.paused,
		Tables:                       make([]connector.TableMaintenanceTableStatus, 0, len(m.states)),
	}

	keys := make([]string, 0, len(m.states))
	for key := range m.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var latestChecked time.Time
	for _, key := range keys {
		state := m.states[key]
		if state == nil {
			continue
		}
		tableStatus := connector.TableMaintenanceTableStatus{
			Namespace:                 state.target.Namespace,
			Table:                     state.target.Table,
			Identifier:                key,
			State:                     "healthy",
			ActiveDataFiles:           state.activeDataFiles,
			ActiveEqualityDeleteFiles: state.activeEqDeletes,
			EligibleSmallFiles:        state.activeSmallFiles,
			EligibleSmallBytes:        state.activeSmallBytes,
			NewDataFiles:              state.newDataFiles,
			NewEqualityDeleteFiles:    state.newEqualityDeletes,
			Error:                     state.lastInventoryError,
		}
		if !state.lastCheckedAt.IsZero() {
			tableStatus.CheckedAt = state.lastCheckedAt.UTC().Format(time.RFC3339)
			if state.lastCheckedAt.After(latestChecked) {
				latestChecked = state.lastCheckedAt
			}
		}
		if m.paused {
			tableStatus.State = "waiting_for_snapshot"
		} else if state.active != nil {
			tableStatus.State = "running"
			tableStatus.SubmissionID = state.active.submissionID
			tableStatus.Operations = append([]string(nil), state.active.operations...)
		} else if state.lastInventoryError != "" {
			tableStatus.State = "error"
		} else if !state.eligibilityReady {
			tableStatus.State = "scanning"
		} else if containsOperation(automaticOperationsDue(state, cfg, now), "rewrite_data_files") {
			tableStatus.State = "ready"
			status.TablesReady++
		} else if state.newDataFiles > 0 || state.newEqualityDeletes > 0 {
			tableStatus.State = "accumulating"
		}

		status.ActiveDataFiles += state.activeDataFiles
		status.ActiveEqualityDeleteFiles += state.activeEqDeletes
		status.EligibleSmallFiles += state.activeSmallFiles
		status.EligibleSmallBytes += state.activeSmallBytes
		if state.eligibilityReady {
			status.TablesScanned++
		}
		if state.active != nil {
			status.ActiveRuns++
		}
		if state.lastInventoryError != "" {
			status.InventoryErrors++
		}
		status.Tables = append(status.Tables, tableStatus)
	}
	status.TablesTotal = len(status.Tables)
	if !latestChecked.IsZero() {
		status.CheckedAt = latestChecked.UTC().Format(time.RFC3339)
	}

	switch {
	case m.paused:
		status.State = "waiting_for_snapshot"
	case status.ActiveRuns > 0:
		status.State = "running"
	case status.InventoryErrors > 0:
		status.State = "error"
	case status.TablesScanned < status.TablesTotal:
		status.State = "scanning"
	case status.TablesReady > 0:
		status.State = "ready"
	}
	return status
}

func containsOperation(operations []TableMaintenanceOperation, wanted string) bool {
	for _, operation := range operations {
		if operation.Type == wanted {
			return true
		}
	}
	return false
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
