package iceberg

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdfs "io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	defaultNativeMaxInputBytes          = int64(512 * 1024 * 1024)
	defaultNativeMaxInputFiles          = 250
	defaultNativeMaxEqualityDeleteFiles = 100
	defaultNativeTargetBytes            = int64(128 * 1024 * 1024)
	defaultNativeSmallBytes             = int64(64 * 1024 * 1024)
	defaultNativeMinSmallFiles          = 10
	defaultNativeMinSmallBytes          = int64(256 * 1024 * 1024)
	defaultDataFilesThreshold           = 200
	defaultEqualityDeleteFilesThreshold = 50
	defaultPositionDeleteFilesThreshold = 25
	defaultNativeTimeout                = 10 * time.Minute
	defaultNativeOrphanAge              = 7 * 24 * time.Hour
	defaultNativeOrphanInactiveInterval = 90 * 24 * time.Hour
	sparkMediumInputBytes               = int64(512 * 1024 * 1024)
	sparkLargeInputBytes                = int64(2 * 1024 * 1024 * 1024)
	sparkXLargeInputBytes               = int64(8 * 1024 * 1024 * 1024)
	sparkMediumInputFiles               = 250
	sparkLargeInputFiles                = 1000
	sparkXLargeInputFiles               = 2000
	sparkMediumDeleteFiles              = 100
	sparkLargeDeleteFiles               = 400
	sparkXLargeDeleteFiles              = 800
	orphanHashBuckets                   = 256
)

type nativeMaintenanceSettings struct {
	Enabled                 bool
	Executor                string
	DataFilesThreshold      int
	EqualityDeleteThreshold int
	PositionDeleteThreshold int
	MaxSelectedInputBytes   int64
	MaxSelectedFiles        int
	MaxEqualityDeleteFiles  int
	TargetFileSizeBytes     int64
	SmallFileSizeBytes      int64
	MinSmallFiles           int
	MinSmallBytes           int64
	ScanConcurrency         int
	Timeout                 time.Duration
	TempDirectory           string
	ExpireInterval          time.Duration
	SnapshotMaxAge          time.Duration
	SnapshotRetainLast      int
	OrphanInterval          time.Duration
	OrphanInactiveInterval  time.Duration
	OrphanMinAge            time.Duration
	OrphanDryRun            bool
	SparkPollInterval       time.Duration
	SparkTimeout            time.Duration
	IdleCompactionInterval  time.Duration
}

type nativeTaskOutcome struct {
	Result    meta.IcebergMaintenanceResult
	Retryable bool
}

type activeFileInventory struct {
	SnapshotID      int64
	DataFiles       int
	SmallFiles      int
	SmallBytes      int64
	EqualityDeletes int
	PositionDeletes int
}

type compactionWorkload struct {
	Groups              []icetable.CompactionTaskGroup
	SelectedDataFiles   int
	SelectedDeleteFiles int
	SelectedFiles       int
	SelectedBytes       int64
	EqualityDeletes     int
	PositionDeletes     int
	GroupCount          int
}

// compactionTriggers describes why a table is eligible for a rewrite. The
// counters come from the active files in its current Iceberg snapshot, not
// from historical commit counts. That distinction prevents an initial
// snapshot of healthy 128 MiB files from looking like a small-file problem.
type compactionTriggers struct {
	SmallFileCount bool
	SmallFileBytes bool
	EqualityDelete bool
	PositionDelete bool
}

func (t compactionTriggers) Any() bool {
	return t.SmallFileCount || t.SmallFileBytes || t.EqualityDelete || t.PositionDelete
}

func defaultNativeMaintenanceSettings() nativeMaintenanceSettings {
	return nativeMaintenanceSettings{
		Executor:                maintenanceExecutorHybrid,
		DataFilesThreshold:      intEnv("RIVUS_MAINTENANCE_DATA_FILES_THRESHOLD", defaultDataFilesThreshold),
		EqualityDeleteThreshold: intEnv("RIVUS_MAINTENANCE_EQUALITY_DELETE_FILES_THRESHOLD", defaultEqualityDeleteFilesThreshold),
		PositionDeleteThreshold: intEnv("RIVUS_MAINTENANCE_POSITION_DELETE_FILES_THRESHOLD", defaultPositionDeleteFilesThreshold),
		MaxSelectedInputBytes:   int64Env("RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_INPUT_BYTES", defaultNativeMaxInputBytes),
		MaxSelectedFiles:        intEnv("RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_FILES", defaultNativeMaxInputFiles),
		MaxEqualityDeleteFiles:  intEnv("RIVUS_MAINTENANCE_NATIVE_MAX_EQUALITY_DELETE_FILES", defaultNativeMaxEqualityDeleteFiles),
		TargetFileSizeBytes:     int64Env("RIVUS_MAINTENANCE_NATIVE_TARGET_FILE_SIZE_BYTES", defaultNativeTargetBytes),
		SmallFileSizeBytes:      int64Env("RIVUS_MAINTENANCE_SMALL_FILE_SIZE_BYTES", defaultNativeSmallBytes),
		MinSmallFiles:           intEnv("RIVUS_MAINTENANCE_SMALL_FILES_MIN_COUNT", defaultNativeMinSmallFiles),
		MinSmallBytes:           int64Env("RIVUS_MAINTENANCE_SMALL_FILES_MIN_TOTAL_BYTES", defaultNativeMinSmallBytes),
		ScanConcurrency:         intEnv("RIVUS_MAINTENANCE_NATIVE_SCAN_CONCURRENCY", 1),
		Timeout:                 durationEnv("RIVUS_MAINTENANCE_NATIVE_TIMEOUT_SECONDS", defaultNativeTimeout),
		TempDirectory:           firstNonEmpty(strings.TrimSpace(os.Getenv("RIVUS_MAINTENANCE_WORKER_TEMP_DIRECTORY")), "/tmp/rivus-maintenance"),
		ExpireInterval:          durationEnv("RIVUS_MAINTENANCE_NATIVE_EXPIRE_INTERVAL_SECONDS", 24*time.Hour),
		SnapshotMaxAge:          time.Duration(float64Env("RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_MAX_AGE_HOURS", 168.0) * float64(time.Hour)),
		SnapshotRetainLast:      intEnv("RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_RETAIN_LAST", 10),
		OrphanInterval:          durationEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INTERVAL_SECONDS", 30*24*time.Hour),
		OrphanInactiveInterval:  durationEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INACTIVE_INTERVAL_SECONDS", defaultNativeOrphanInactiveInterval),
		OrphanMinAge:            time.Duration(float64Env("RIVUS_MAINTENANCE_NATIVE_ORPHAN_MIN_AGE_HOURS", defaultNativeOrphanAge.Hours()) * float64(time.Hour)),
		OrphanDryRun:            boolEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_DRY_RUN", false),
		SparkPollInterval:       durationEnv("RIVUS_MAINTENANCE_SPARK_POLL_INTERVAL_SECONDS", 5*time.Second),
		SparkTimeout:            durationEnv("RIVUS_MAINTENANCE_SPARK_TIMEOUT_SECONDS", 2*time.Hour),
		IdleCompactionInterval:  durationEnv("RIVUS_MAINTENANCE_NATIVE_IDLE_CHECK_INTERVAL_SECONDS", 7*24*time.Hour),
	}
}

func executeNativeMaintenanceTask(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobID string,
	jobCfg *config.JobConfig,
	state meta.IcebergMaintenanceState,
	task meta.IcebergMaintenanceTask,
	settings nativeMaintenanceSettings,
) nativeTaskOutcome {
	started := time.Now()
	result := meta.IcebergMaintenanceResult{
		TaskID:    task.ID,
		TableKey:  state.TableKey,
		Operation: task.Operation,
		Engine:    "native",
		Status:    "failed",
		Attempt:   task.AttemptCount,
		CreatedAt: started.UTC(),
	}
	finish := func(out nativeTaskOutcome) nativeTaskOutcome {
		out.Result.DurationMillis = time.Since(started).Milliseconds()
		return out
	}

	if jobCfg == nil {
		result.Error = "owner job configuration is unavailable"
		return finish(nativeTaskOutcome{Result: result})
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, settings.Timeout)
	iceCfg, err := icebergConfigForNativeWorker(jobCfg)
	if err != nil {
		setupCancel()
		result.Error = err.Error()
		return finish(nativeTaskOutcome{Result: result})
	}
	cat, err := newCatalog(setupCtx, iceCfg)
	if err != nil {
		setupCancel()
		result.Error = err.Error()
		return finish(nativeTaskOutcome{Result: result, Retryable: true})
	}
	tbl, err := cat.LoadTable(setupCtx, namespaceIdentifier(state.Namespace, state.Table))
	if err != nil {
		if errorsIsNoSuchIcebergTable(err) {
			if storeErr := store.MarkInventoryMissing(setupCtx, state.TableKey); storeErr != nil {
				setupCancel()
				result.Error = fmt.Sprintf("mark missing table inventory: %v", storeErr)
				return finish(nativeTaskOutcome{Result: result, Retryable: true})
			}
			setupCancel()
			result.Status = "skipped"
			result.RoutingReason = "table does not exist yet"
			return finish(nativeTaskOutcome{Result: result})
		}
		setupCancel()
		result.Error = fmt.Sprintf("load table: %v", err)
		return finish(nativeTaskOutcome{Result: result, Retryable: true})
	}
	if task.Operation == "compact" {
		inventory, inventoryErr := scanActiveFileInventory(setupCtx, tbl, settings.SmallFileSizeBytes)
		if inventoryErr != nil {
			setupCancel()
			result.Error = fmt.Sprintf("scan active file inventory: %v", inventoryErr)
			return finish(nativeTaskOutcome{Result: result, Retryable: true})
		}
		if err := saveActiveFileInventory(setupCtx, store, state.TableKey, inventory); err != nil {
			setupCancel()
			result.Error = fmt.Sprintf("save active file inventory: %v", err)
			return finish(nativeTaskOutcome{Result: result, Retryable: true})
		}
		// The planning decision below must use the inventory just read, rather
		// than the state value that was loaded before this task was leased.
		state.InventorySnapshotID = inventory.SnapshotID
		state.ActiveDataFiles = inventory.DataFiles
		state.ActiveSmallFiles = inventory.SmallFiles
		state.ActiveSmallBytes = inventory.SmallBytes
		state.ActiveEqualityDeleteFiles = inventory.EqualityDeletes
		state.ActivePositionDeleteFiles = inventory.PositionDeletes
	}
	setupCancel()

	var outcome nativeTaskOutcome
	switch task.Operation {
	case "compact":
		outcome = executeHybridCompaction(ctx, jobID, jobCfg, iceCfg, tbl, state, task, settings)
	case "expire_snapshots":
		taskCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
		outcome = executeNativeSnapshotExpiration(taskCtx, tbl, result, settings)
		cancel()
	case "remove_orphan_files":
		taskCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
		outcome = executeBoundedOrphanCleanup(taskCtx, tbl, result, settings)
		cancel()
	default:
		result.Error = fmt.Sprintf("unsupported maintenance operation %q", task.Operation)
		return finish(nativeTaskOutcome{Result: result})
	}

	if task.Operation == "compact" && outcome.Result.Status == "succeeded" {
		refreshCtx, refreshCancel := context.WithTimeout(ctx, settings.Timeout)
		if err := tbl.Refresh(refreshCtx); err == nil {
			if inventory, inventoryErr := scanActiveFileInventory(refreshCtx, tbl, settings.SmallFileSizeBytes); inventoryErr == nil {
				if saveErr := saveActiveFileInventory(refreshCtx, store, state.TableKey, inventory); saveErr != nil {
					addInventoryWarning(&outcome.Result, saveErr)
				}
			} else {
				addInventoryWarning(&outcome.Result, inventoryErr)
			}
		} else {
			addInventoryWarning(&outcome.Result, err)
		}
		refreshCancel()
	}
	outcome.Result.DurationMillis = time.Since(started).Milliseconds()
	return outcome
}

func refreshPendingInventory(ctx context.Context, store *meta.IcebergMaintenanceStore, jobCfg *config.JobConfig, state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings) error {
	iceCfg, err := icebergConfigForNativeWorker(jobCfg)
	if err != nil {
		return err
	}
	cat, err := newCatalog(ctx, iceCfg)
	if err != nil {
		return err
	}
	tbl, err := cat.LoadTable(ctx, namespaceIdentifier(state.Namespace, state.Table))
	if err != nil {
		if errorsIsNoSuchIcebergTable(err) {
			return store.MarkInventoryMissing(ctx, state.TableKey)
		}
		return fmt.Errorf("load table: %w", err)
	}
	inventory, err := scanActiveFileInventory(ctx, tbl, settings.SmallFileSizeBytes)
	if err != nil {
		return err
	}
	if err := saveActiveFileInventory(ctx, store, state.TableKey, inventory); err != nil {
		return err
	}
	if !state.SnapshotComplete || !inventoryTriggersCompaction(inventory, settings) {
		return nil
	}
	return store.ScheduleCompactionCheck(ctx, state.TableKey, time.Now().UTC())
}

func scanActiveFileInventory(ctx context.Context, tbl *icetable.Table, smallFileSizeBytes int64) (activeFileInventory, error) {
	var inventory activeFileInventory
	if tbl == nil || tbl.CurrentSnapshot() == nil {
		return inventory, nil
	}
	inventory.SnapshotID = tbl.CurrentSnapshot().SnapshotID
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return inventory, fmt.Errorf("open table filesystem: %w", err)
	}
	manifests, err := tbl.CurrentSnapshot().Manifests(fsys)
	if err != nil {
		return inventory, fmt.Errorf("read current manifest list: %w", err)
	}
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return inventory, err
		}
		for entry, entryErr := range manifest.Entries(fsys, true) {
			if entryErr != nil {
				return inventory, fmt.Errorf("read manifest %s: %w", manifest.FilePath(), entryErr)
			}
			if entry == nil || entry.DataFile() == nil {
				continue
			}
			accumulateActiveFile(&inventory, entry.DataFile(), smallFileSizeBytes)
		}
	}
	return inventory, nil
}

func accumulateActiveFile(inventory *activeFileInventory, file iceberglib.DataFile, smallFileSizeBytes int64) {
	if inventory == nil || file == nil {
		return
	}
	switch file.ContentType() {
	case iceberglib.EntryContentData:
		inventory.DataFiles++
		if smallFileSizeBytes > 0 && file.FileSizeBytes() < smallFileSizeBytes {
			inventory.SmallFiles++
			inventory.SmallBytes += file.FileSizeBytes()
		}
	case iceberglib.EntryContentEqDeletes:
		inventory.EqualityDeletes++
	case iceberglib.EntryContentPosDeletes:
		inventory.PositionDeletes++
	}
}

func saveActiveFileInventory(ctx context.Context, store *meta.IcebergMaintenanceStore, tableKey string, inventory activeFileInventory) error {
	return store.UpdateInventory(ctx, tableKey, inventory.SnapshotID, inventory.DataFiles, inventory.SmallFiles,
		inventory.SmallBytes, inventory.EqualityDeletes, inventory.PositionDeletes)
}

func inventoryTriggersCompaction(inventory activeFileInventory, settings nativeMaintenanceSettings) bool {
	return compactionTriggersFor(meta.IcebergMaintenanceState{
		ActiveSmallFiles:          inventory.SmallFiles,
		ActiveSmallBytes:          inventory.SmallBytes,
		ActiveEqualityDeleteFiles: inventory.EqualityDeletes,
		ActivePositionDeleteFiles: inventory.PositionDeletes,
	}, settings).Any()
}

func compactionTriggersFor(state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings) compactionTriggers {
	return compactionTriggers{
		SmallFileCount: settings.DataFilesThreshold > 0 && state.ActiveSmallFiles >= settings.DataFilesThreshold,
		SmallFileBytes: settings.MinSmallFiles > 0 && settings.MinSmallBytes > 0 &&
			state.ActiveSmallFiles >= settings.MinSmallFiles && state.ActiveSmallBytes >= settings.MinSmallBytes,
		EqualityDelete: settings.EqualityDeleteThreshold > 0 && state.ActiveEqualityDeleteFiles >= settings.EqualityDeleteThreshold,
		// Trino snapshot replacement intentionally leaves a small number of
		// position-delete files while it replaces the recent history window.
		// Make the table eligible only after the per-table threshold is reached;
		// engine routing is decided separately from the selected workload size.
		PositionDelete: settings.PositionDeleteThreshold > 0 && state.ActivePositionDeleteFiles >= settings.PositionDeleteThreshold,
	}
}

func addInventoryWarning(result *meta.IcebergMaintenanceResult, err error) {
	if result == nil || err == nil {
		return
	}
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	result.Details["inventory_refresh_warning"] = err.Error()
}

func icebergConfigForNativeWorker(jobCfg *config.JobConfig) (config.IcebergConfig, error) {
	if jobCfg == nil {
		return config.IcebergConfig{}, fmt.Errorf("job config is nil")
	}
	sinkType, sinkCfg := jobSinkSpec(jobCfg)
	if !strings.EqualFold(sinkType, "iceberg_native") {
		return config.IcebergConfig{}, fmt.Errorf("job sink is %q, not iceberg_native", sinkType)
	}
	return decodeIcebergConfig(sinkCfg)
}

func executeHybridCompaction(
	ctx context.Context,
	jobID string,
	jobCfg *config.JobConfig,
	iceCfg config.IcebergConfig,
	tbl *icetable.Table,
	state meta.IcebergMaintenanceState,
	task meta.IcebergMaintenanceTask,
	settings nativeMaintenanceSettings,
) nativeTaskOutcome {
	nativeCtx, nativeCancel := context.WithTimeout(ctx, settings.Timeout)
	defer nativeCancel()
	result := meta.IcebergMaintenanceResult{
		TaskID:    task.ID,
		TableKey:  state.TableKey,
		Operation: task.Operation,
		Engine:    "native",
		Status:    "failed",
		Attempt:   task.AttemptCount,
		CreatedAt: time.Now().UTC(),
	}
	if tbl.CurrentSnapshot() == nil {
		result.Status = "skipped"
		result.RoutingReason = "table has no current snapshot"
		return nativeTaskOutcome{Result: result}
	}
	startSnapshotID := tbl.CurrentSnapshot().SnapshotID

	triggers := compactionTriggersFor(state, settings)
	cfg := compaction.DefaultConfig()
	cfg.TargetFileSizeBytes = settings.TargetFileSizeBytes
	cfg.MinFileSizeBytes = settings.SmallFileSizeBytes
	cfg.MaxFileSizeBytes = maxInt64(settings.TargetFileSizeBytes*9/5, settings.SmallFileSizeBytes+1)
	cfg.MinInputFiles = uint(settings.MinSmallFiles)
	// Delete-driven maintenance must be able to rewrite a single affected data
	// file. Ordinary small-file compaction keeps the higher minimum so one
	// small file is never rewritten in a loop.
	if triggers.EqualityDelete || triggers.PositionDelete {
		cfg.MinInputFiles = 1
	}
	cfg.DeleteFileThreshold = 1
	cfg.PreserveDeadEqualityDeletes = false

	plan, err := compaction.Analyze(nativeCtx, tbl, cfg)
	if err != nil {
		result.Error = fmt.Sprintf("analyze compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	work := buildCompactionWorkload(plan)
	result.InputFiles = work.SelectedFiles
	result.InputBytes = work.SelectedBytes
	result.DeleteFiles = work.EqualityDeletes + work.PositionDeletes
	result.Details = map[string]any{
		"executor":                         settings.Executor,
		"groups":                           work.GroupCount,
		"selected_data_files":              work.SelectedDataFiles,
		"selected_delete_files":            work.SelectedDeleteFiles,
		"equality_delete_files":            work.EqualityDeletes,
		"position_delete_files":            work.PositionDeletes,
		"active_equality_delete_files":     state.ActiveEqualityDeleteFiles,
		"active_position_delete_files":     state.ActivePositionDeleteFiles,
		"trigger_small_file_count":         triggers.SmallFileCount,
		"trigger_small_file_bytes":         triggers.SmallFileBytes,
		"trigger_equality_delete_files":    triggers.EqualityDelete,
		"trigger_position_delete_files":    triggers.PositionDelete,
		"starting_snapshot_id":             startSnapshotID,
		"estimated_output_files":           plan.EstOutputFiles,
		"estimated_output_bytes":           plan.EstOutputBytes,
		"max_native_input_bytes":           settings.MaxSelectedInputBytes,
		"max_native_input_files":           settings.MaxSelectedFiles,
		"max_native_equality_delete_files": settings.MaxEqualityDeleteFiles,
	}

	// The safety route is deliberately evaluated before the normal byte-based
	// eligibility check. A table with 5,000 delete files must go to Spark even
	// when those files are small in total.
	routeSpark, reason := shouldRouteCompactionToSpark(work, state, settings)
	eligible := triggers.Any()
	if work.SelectedDataFiles == 0 {
		// The native planner can have no usable group even when a table has a
		// dangerous delete-file count. Let Spark make its own plan in hybrid
		// mode for that case; native mode remains bounded and reports a skip.
		if eligible && routeSpark {
			if settings.Executor == maintenanceExecutorNative {
				result.Status = "skipped"
				result.RoutingReason = "native safety limit reached; enable hybrid executor for Spark: " + reason
				return nativeTaskOutcome{Result: result}
			}
			nativeCancel()
			result.Engine = "spark"
			if settings.Executor == maintenanceExecutorSpark {
				result.RoutingReason = "executor=spark selected Spark compaction"
			} else {
				result.RoutingReason = reason
			}
			return executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
		}
		result.Status = "skipped"
		result.RoutingReason = "compaction planner found no eligible data-file group"
		return nativeTaskOutcome{Result: result}
	}
	if !eligible {
		result.Status = "skipped"
		result.RoutingReason = "active inventory has not reached a compaction trigger"
		return nativeTaskOutcome{Result: result}
	}

	if settings.Executor == maintenanceExecutorSpark {
		nativeCancel()
		result.Engine = "spark"
		result.RoutingReason = "executor=spark selected Spark compaction"
		return executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
	}
	if routeSpark {
		if settings.Executor == maintenanceExecutorNative {
			result.Status = "skipped"
			result.RoutingReason = "native safety limit reached; enable hybrid executor for Spark: " + reason
			return nativeTaskOutcome{Result: result}
		}
		nativeCancel()
		result.Engine = "spark"
		result.RoutingReason = reason
		return executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
	}

	if err := tbl.Refresh(nativeCtx); err != nil {
		result.Error = fmt.Sprintf("refresh before compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if tbl.CurrentSnapshot() == nil || tbl.CurrentSnapshot().SnapshotID != startSnapshotID {
		result.Error = "current snapshot changed after compaction analysis; CDC wins and maintenance will retry"
		return nativeTaskOutcome{Result: result, Retryable: true}
	}

	rewrittenPaths := make(map[string]struct{}, work.SelectedDataFiles)
	for _, group := range work.Groups {
		for _, scanTask := range group.Tasks {
			if scanTask.File != nil {
				rewrittenPaths[scanTask.File.FilePath()] = struct{}{}
			}
		}
	}
	fs, err := tbl.FS(nativeCtx)
	if err != nil {
		result.Error = fmt.Sprintf("get table filesystem: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	deadEqDeletes, err := compaction.CollectDeadEqualityDeletes(nativeCtx, fs, tbl.CurrentSnapshot(), rewrittenPaths)
	if err != nil {
		result.Error = fmt.Sprintf("collect dead equality deletes: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}

	tx := tbl.NewTransaction()
	rewriteResult, err := tx.RewriteDataFiles(nativeCtx, work.Groups, icetable.RewriteDataFilesOptions{
		PartialProgress:          false,
		ExtraDeleteFilesToRemove: deadEqDeletes,
		SnapshotProps: iceberglib.Properties{
			"rivus.maintenance":         "native-compaction",
			"rivus.maintenance.job_id":  jobID,
			"rivus.maintenance.task_id": fmt.Sprintf("%d", task.ID),
			"rivus.maintenance.worker":  "maintenance-worker",
		},
		GroupOptions: []icetable.CompactionGroupOption{
			icetable.WithCompactionTargetFileSize(settings.TargetFileSizeBytes),
			icetable.WithCompactionScanConcurrency(settings.ScanConcurrency),
		},
	})
	if err != nil {
		result.Error = fmt.Sprintf("rewrite data files: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if rewriteResult.RewrittenGroups == 0 {
		result.Status = "skipped"
		result.RoutingReason = "compaction planner produced no rewrite output"
		return nativeTaskOutcome{Result: result}
	}
	committed, err := tx.Commit(nativeCtx)
	if err != nil {
		result.Error = fmt.Sprintf("commit native compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: errors.Is(err, icetable.ErrCommitFailed) || nativeCtx.Err() != nil}
	}
	result.Status = "succeeded"
	if result.RoutingReason == "" {
		if settings.Executor == maintenanceExecutorNative {
			result.RoutingReason = "executor=native selected native compaction"
		} else {
			result.RoutingReason = "hybrid selected native: workload is within native file and byte limits"
		}
	}
	result.OutputFiles = rewriteResult.AddedDataFiles
	result.OutputBytes = rewriteResult.BytesAfter
	result.Details["rewritten_groups"] = rewriteResult.RewrittenGroups
	result.Details["removed_data_files"] = rewriteResult.RemovedDataFiles
	result.Details["removed_equality_delete_files"] = rewriteResult.RemovedEqualityDeleteFiles
	result.Details["removed_position_delete_files"] = rewriteResult.RemovedPositionDeleteFiles
	if committed != nil && committed.CurrentSnapshot() != nil {
		result.Details["committed_snapshot_id"] = committed.CurrentSnapshot().SnapshotID
	}
	return nativeTaskOutcome{Result: result}
}

func buildCompactionWorkload(plan compaction.Plan) compactionWorkload {
	work := compactionWorkload{GroupCount: len(plan.Groups)}
	work.Groups = make([]icetable.CompactionTaskGroup, 0, len(plan.Groups))
	seenEq := make(map[string]int64)
	seenPos := make(map[string]int64)
	for _, group := range plan.Groups {
		converted := icetable.CompactionTaskGroup{
			PartitionKey:   group.PartitionKey,
			Tasks:          group.Tasks,
			TotalSizeBytes: group.TotalSizeBytes,
		}
		work.Groups = append(work.Groups, converted)
		work.SelectedDataFiles += len(group.Tasks)
		work.SelectedBytes += group.TotalSizeBytes
		for _, task := range group.Tasks {
			for _, deleteFile := range task.EqualityDeleteFiles {
				if deleteFile != nil {
					if _, seen := seenEq[deleteFile.FilePath()]; !seen {
						seenEq[deleteFile.FilePath()] = deleteFile.FileSizeBytes()
					}
				}
			}
			for _, deleteFile := range task.DeleteFiles {
				if deleteFile != nil {
					if _, seen := seenPos[deleteFile.FilePath()]; !seen {
						seenPos[deleteFile.FilePath()] = deleteFile.FileSizeBytes()
					}
				}
			}
		}
	}
	work.EqualityDeletes = len(seenEq)
	work.PositionDeletes = len(seenPos)
	work.SelectedDeleteFiles = work.EqualityDeletes + work.PositionDeletes
	work.SelectedFiles = work.SelectedDataFiles + work.SelectedDeleteFiles
	for _, size := range seenEq {
		work.SelectedBytes += size
	}
	for _, size := range seenPos {
		work.SelectedBytes += size
	}
	return work
}

func shouldRouteCompactionToSpark(work compactionWorkload, state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings) (bool, string) {
	equalityDeletes := maxInt(work.EqualityDeletes, state.ActiveEqualityDeleteFiles)
	switch {
	case equalityDeletes > settings.MaxEqualityDeleteFiles:
		return true, fmt.Sprintf("equality-delete files %d exceed native limit %d", equalityDeletes, settings.MaxEqualityDeleteFiles)
	case work.SelectedBytes > settings.MaxSelectedInputBytes:
		return true, fmt.Sprintf("selected input %d bytes exceeds native limit %d", work.SelectedBytes, settings.MaxSelectedInputBytes)
	case work.SelectedFiles > settings.MaxSelectedFiles:
		return true, fmt.Sprintf("selected input %d files exceeds native limit %d", work.SelectedFiles, settings.MaxSelectedFiles)
	default:
		return false, ""
	}
}

func hasDeleteWork(work compactionWorkload, state meta.IcebergMaintenanceState) bool {
	return work.EqualityDeletes > 0 || work.PositionDeletes > 0 ||
		state.ActiveEqualityDeleteFiles > 0 || state.ActivePositionDeleteFiles > 0
}

func executeSparkCompactionFallback(
	ctx context.Context,
	jobID string,
	jobCfg *config.JobConfig,
	iceCfg config.IcebergConfig,
	state meta.IcebergMaintenanceState,
	task meta.IcebergMaintenanceTask,
	result meta.IcebergMaintenanceResult,
	settings nativeMaintenanceSettings,
	work compactionWorkload,
	deleteTriggered bool,
) nativeTaskOutcome {
	minInputFiles := settings.MinSmallFiles
	if deleteTriggered {
		minInputFiles = 1
	}
	compactOptions := normalizeAutomaticCompactOptions(iceCfg.TableMaintenance.CompactOptions)
	options := stringAnyMap(compactOptions["options"])
	options["target-file-size-bytes"] = fmt.Sprintf("%d", settings.TargetFileSizeBytes)
	options["min-input-files"] = fmt.Sprintf("%d", minInputFiles)
	if deleteTriggered {
		options["delete-file-threshold"] = "1"
	}
	compactOptions["options"] = options
	resourceProfile, sizingReason := dynamicSparkResourceProfile(iceCfg.TableMaintenance.RunnerResourceProfile, work, task)
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	result.Details["spark_resource_profile"] = resourceProfile
	result.Details["spark_resource_sizing"] = sizingReason
	request := TableMaintenanceRequest{
		Tables: []string{tableKey(state.Namespace, state.Table)},
		// Keep retries idempotent within one durable attempt while allowing a
		// later, right-sized attempt to create a new runner job.
		ExternalRunKey:  fmt.Sprintf("rivus-maintenance:%d:%d", task.ID, task.AttemptCount),
		ResourceProfile: resourceProfile,
		Operations: []TableMaintenanceOperation{{
			Type:    "rewrite_data_files",
			Options: compactOptions,
		}},
	}
	submission, err := SubmitTableMaintenanceForJobConfig(ctx, jobID, jobCfg, request, false)
	if err != nil {
		result.Error = fmt.Sprintf("submit Spark compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	result.SubmissionID = submission.SubmissionID
	pollEvery := settings.SparkPollInterval
	if pollEvery <= 0 {
		pollEvery = 5 * time.Second
	}
	deadline := time.NewTimer(settings.SparkTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			result.Error = ctx.Err().Error()
			return nativeTaskOutcome{Result: result, Retryable: true}
		case <-deadline.C:
			result.Error = "Spark compaction exceeded maintenance timeout"
			return nativeTaskOutcome{Result: result, Retryable: true}
		case <-ticker.C:
			status, err := GetTableMaintenanceStatusForJobConfig(ctx, jobCfg, submission.SubmissionID)
			if err != nil {
				result.Error = fmt.Sprintf("poll Spark compaction: %v", err)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
			switch strings.ToUpper(strings.TrimSpace(status.DriverState)) {
			case "FINISHED", "SUCCESS", "SUCCEEDED":
				result.Status = "succeeded"
				return nativeTaskOutcome{Result: result}
			case "FAILED", "ERROR", "KILLED", "CANCELLED", "CANCELED":
				message := firstNonEmpty(strings.TrimSpace(status.Message), "Spark compaction failed")
				result.Error = fmt.Sprintf("Spark compaction failed with resource profile %s: %s", resourceProfile, message)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
		}
	}
}

func dynamicSparkResourceProfile(configured string, work compactionWorkload, task meta.IcebergMaintenanceTask) (string, string) {
	profile := normalizeSparkResourceProfile(configured)
	reason := "configured minimum"
	deleteFiles := work.EqualityDeletes + work.PositionDeletes

	needed := "tiny"
	workloadReason := ""
	switch {
	case work.SelectedBytes > sparkXLargeInputBytes || work.SelectedFiles > sparkXLargeInputFiles || deleteFiles > sparkXLargeDeleteFiles:
		needed = "xlarge"
		workloadReason = "xlarge selected rewrite workload"
	case work.SelectedBytes > sparkLargeInputBytes || work.SelectedFiles > sparkLargeInputFiles || deleteFiles > sparkLargeDeleteFiles:
		needed = "large"
		workloadReason = "large selected rewrite workload"
	case work.SelectedBytes > sparkMediumInputBytes || work.SelectedFiles > sparkMediumInputFiles || deleteFiles > sparkMediumDeleteFiles:
		needed = "medium"
		workloadReason = "medium selected rewrite workload"
	}
	if sparkResourceProfileRank(needed) > sparkResourceProfileRank(profile) {
		profile = needed
		reason = workloadReason
	}

	if isSparkHeapError(task.LastError) && profile != "xlarge" {
		steps := task.AttemptCount - 1
		if steps < 1 {
			steps = 1
		}
		for i := 0; i < steps && profile != "xlarge"; i++ {
			profile = nextSparkResourceProfile(profile)
		}
		reason = "previous Spark attempt exhausted executor heap"
	}
	return profile, reason
}

func normalizeSparkResourceProfile(profile string) string {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "tiny", "small", "medium", "large", "xlarge":
		return strings.ToLower(strings.TrimSpace(profile))
	default:
		return "small"
	}
}

func sparkResourceProfileRank(profile string) int {
	switch normalizeSparkResourceProfile(profile) {
	case "tiny":
		return 0
	case "medium":
		return 2
	case "large":
		return 3
	case "xlarge":
		return 4
	default:
		return 1
	}
}

func nextSparkResourceProfile(profile string) string {
	switch normalizeSparkResourceProfile(profile) {
	case "tiny":
		return "small"
	case "small":
		return "medium"
	case "medium":
		return "large"
	default:
		return "xlarge"
	}
}

func isSparkHeapError(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "outofmemoryerror") ||
		strings.Contains(message, "java heap space") ||
		strings.Contains(message, "gc overhead limit exceeded")
}

func executeNativeSnapshotExpiration(ctx context.Context, tbl *icetable.Table, result meta.IcebergMaintenanceResult, settings nativeMaintenanceSettings) nativeTaskOutcome {
	before := append([]icetable.Snapshot(nil), tbl.Metadata().Snapshots()...)
	if len(before) == 0 {
		result.Status = "skipped"
		result.RoutingReason = "table has no snapshots"
		return nativeTaskOutcome{Result: result}
	}
	staging := tbl.NewTransaction()
	if err := staging.ExpireSnapshots(
		icetable.WithOlderThan(settings.SnapshotMaxAge),
		icetable.WithRetainLast(settings.SnapshotRetainLast),
		icetable.WithPostCommit(false),
	); err != nil {
		result.Error = fmt.Sprintf("stage snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	staged, err := staging.StagedTable()
	if err != nil {
		result.Error = fmt.Sprintf("inspect staged snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	remaining := make(map[int64]struct{}, len(staged.Metadata().Snapshots()))
	for _, snapshot := range staged.Metadata().Snapshots() {
		remaining[snapshot.SnapshotID] = struct{}{}
	}
	expired := 0
	for _, snapshot := range before {
		if _, ok := remaining[snapshot.SnapshotID]; !ok {
			expired++
		}
	}
	result.ExpiredSnapshots = expired
	if expired == 0 {
		result.Status = "skipped"
		result.RoutingReason = "no snapshots are eligible after retain-last and reference protection"
		return nativeTaskOutcome{Result: result}
	}

	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(
		icetable.WithOlderThan(settings.SnapshotMaxAge),
		icetable.WithRetainLast(settings.SnapshotRetainLast),
	); err != nil {
		result.Error = fmt.Sprintf("prepare snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if _, err := tx.Commit(ctx); err != nil {
		result.Error = fmt.Sprintf("commit snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: errors.Is(err, icetable.ErrCommitFailed) || ctx.Err() != nil}
	}
	result.Status = "succeeded"
	result.RoutingReason = "native snapshot expiration"
	return nativeTaskOutcome{Result: result}
}

type orphanDiskRecord struct {
	Path string `json:"path"`
	Size int64  `json:"size,omitempty"`
}

func executeBoundedOrphanCleanup(ctx context.Context, tbl *icetable.Table, result meta.IcebergMaintenanceResult, settings nativeMaintenanceSettings) nativeTaskOutcome {
	if settings.OrphanMinAge < defaultNativeOrphanAge {
		result.Error = fmt.Sprintf("orphan minimum age %s is below mandatory safety floor %s", settings.OrphanMinAge, defaultNativeOrphanAge)
		return nativeTaskOutcome{Result: result}
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("get table filesystem: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	listable, ok := fsys.(iceio.ListableIO)
	if !ok {
		result.Error = "table filesystem does not implement streaming ListableIO; refusing unbounded orphan scan"
		return nativeTaskOutcome{Result: result}
	}

	baseTemp := settings.TempDirectory
	if strings.TrimSpace(baseTemp) == "" {
		baseTemp = "/tmp/rivus-maintenance"
	}
	if err := os.MkdirAll(baseTemp, 0o700); err != nil {
		result.Error = fmt.Sprintf("create maintenance temp directory: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	tempDir, err := os.MkdirTemp(baseTemp, "orphan-")
	if err != nil {
		result.Error = fmt.Sprintf("create orphan temp directory: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	defer os.RemoveAll(tempDir)

	if err := writeReferencedFileBuckets(ctx, tbl, fsys, tempDir); err != nil {
		result.Error = fmt.Sprintf("index referenced files: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	cutoff := time.Now().Add(-settings.OrphanMinAge)
	var scannedBytes int64
	if err := listable.WalkDir(tbl.Location(), func(path string, entry stdfs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.ModTime().Before(cutoff) {
			return nil
		}
		if err := ensureSameStoragePrefix(tbl.Location(), path); err != nil {
			return err
		}
		scannedBytes += info.Size()
		return appendBucketRecord(tempDir, "candidate", orphanDiskRecord{Path: path, Size: info.Size()})
	}); err != nil {
		result.Error = fmt.Sprintf("stream object listing: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}

	var candidates, deleted int
	var deletedBytes int64
	for bucket := 0; bucket < orphanHashBuckets; bucket++ {
		if err := ctx.Err(); err != nil {
			result.Error = err.Error()
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
		refs, err := loadReferenceBucket(tempDir, bucket)
		if err != nil {
			result.Error = fmt.Sprintf("load reference bucket %d: %v", bucket, err)
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
		candidateFile := bucketFile(tempDir, "candidate", bucket)
		f, err := os.Open(candidateFile)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result.Error = fmt.Sprintf("open candidate bucket %d: %v", bucket, err)
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			var record orphanDiskRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				f.Close()
				result.Error = fmt.Sprintf("decode candidate bucket %d: %v", bucket, err)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
			if _, referenced := refs[record.Path]; referenced {
				continue
			}
			candidates++
			if settings.OrphanDryRun {
				continue
			}
			if err := fsys.Remove(record.Path); err != nil && !errors.Is(err, stdfs.ErrNotExist) {
				f.Close()
				result.Error = fmt.Sprintf("delete orphan %s: %v", record.Path, err)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
			deleted++
			deletedBytes += record.Size
		}
		scanErr := scanner.Err()
		f.Close()
		if scanErr != nil {
			result.Error = fmt.Sprintf("scan candidate bucket %d: %v", bucket, scanErr)
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
	}
	result.OrphanCandidates = candidates
	result.DeletedFiles = deleted
	result.DeletedBytes = deletedBytes
	result.Details = map[string]any{
		"dry_run":           settings.OrphanDryRun,
		"minimum_age_hours": settings.OrphanMinAge.Hours(),
		"scan_bytes":        scannedBytes,
		"disk_buckets":      orphanHashBuckets,
	}
	result.Status = "succeeded"
	result.RoutingReason = "bounded-memory disk-bucket orphan cleanup"
	return nativeTaskOutcome{Result: result}
}

func writeReferencedFileBuckets(ctx context.Context, tbl *icetable.Table, fsys iceio.IO, tempDir string) error {
	add := func(path string) error {
		path = strings.TrimSpace(path)
		if path == "" {
			return nil
		}
		if err := ensureSameStoragePrefix(tbl.Location(), path); err != nil {
			return err
		}
		return appendBucketRecord(tempDir, "reference", orphanDiskRecord{Path: path})
	}
	if err := add(tbl.MetadataLocation()); err != nil {
		return err
	}
	for entry := range tbl.Metadata().PreviousFiles() {
		if err := add(entry.MetadataFile); err != nil {
			return err
		}
	}
	if err := add(strings.TrimRight(tbl.Location(), "/") + "/metadata/version-hint.text"); err != nil {
		return err
	}
	for stat := range tbl.Metadata().Statistics() {
		if err := add(stat.StatisticsPath); err != nil {
			return err
		}
	}
	for stat := range tbl.Metadata().PartitionStatistics() {
		if err := add(stat.StatisticsPath); err != nil {
			return err
		}
	}
	for _, snapshot := range tbl.Metadata().Snapshots() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := add(snapshot.ManifestList); err != nil {
			return err
		}
		manifests, err := snapshot.Manifests(fsys)
		if err != nil {
			return fmt.Errorf("snapshot %d manifests: %w", snapshot.SnapshotID, err)
		}
		for _, manifest := range manifests {
			if err := add(manifest.FilePath()); err != nil {
				return err
			}
			for entry, err := range manifest.Entries(fsys, true) {
				if err != nil {
					return err
				}
				if err := add(entry.DataFile().FilePath()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func appendBucketRecord(tempDir, kind string, record orphanDiskRecord) error {
	bucket := int(sha256.Sum256([]byte(record.Path))[0])
	path := bucketFile(tempDir, kind, bucket)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err == nil {
		_, err = f.Write(append(encoded, '\n'))
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func bucketFile(tempDir, kind string, bucket int) string {
	return filepath.Join(tempDir, fmt.Sprintf("%s-%03d.jsonl", kind, bucket))
}

func loadReferenceBucket(tempDir string, bucket int) (map[string]struct{}, error) {
	refs := make(map[string]struct{})
	f, err := os.Open(bucketFile(tempDir, "reference", bucket))
	if errors.Is(err, os.ErrNotExist) {
		return refs, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var record orphanDiskRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, err
		}
		refs[record.Path] = struct{}{}
	}
	return refs, scanner.Err()
}

func ensureSameStoragePrefix(tableLocation, path string) error {
	base, err := url.Parse(tableLocation)
	if err != nil {
		return fmt.Errorf("invalid table location %q: %w", tableLocation, err)
	}
	candidate, err := url.Parse(path)
	if err != nil {
		return fmt.Errorf("invalid referenced path %q: %w", path, err)
	}
	if !strings.EqualFold(base.Scheme, candidate.Scheme) || !strings.EqualFold(base.Host, candidate.Host) {
		return fmt.Errorf("orphan cleanup prefix mismatch: table=%s candidate=%s", tableLocation, path)
	}
	return nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var _ io.Reader = (*os.File)(nil)
