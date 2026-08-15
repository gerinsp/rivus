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
	defaultNativeMaxInputBytes = int64(512 * 1024 * 1024)
	defaultNativeMaxInputFiles = 100
	defaultNativeTargetBytes   = int64(128 * 1024 * 1024)
	defaultNativeSmallBytes    = int64(64 * 1024 * 1024)
	defaultNativeMinSmallFiles = 10
	defaultNativeMinSmallBytes = int64(256 * 1024 * 1024)
	defaultNativeTimeout       = 10 * time.Minute
	defaultNativeOrphanAge     = 7 * 24 * time.Hour
	orphanHashBuckets          = 256
)

type nativeMaintenanceSettings struct {
	Enabled                bool
	MaxSelectedInputBytes  int64
	MaxSelectedFiles       int
	TargetFileSizeBytes    int64
	SmallFileSizeBytes     int64
	MinSmallFiles          int
	MinSmallBytes          int64
	ScanConcurrency        int
	Timeout                time.Duration
	TempDirectory          string
	ExpireInterval         time.Duration
	SnapshotMaxAge         time.Duration
	SnapshotRetainLast     int
	OrphanInterval         time.Duration
	OrphanMinAge           time.Duration
	OrphanDryRun           bool
	SparkPollInterval      time.Duration
	SparkTimeout           time.Duration
	IdleCompactionInterval time.Duration
}

type nativeTaskOutcome struct {
	Result    meta.IcebergMaintenanceResult
	Retryable bool
}

type compactionWorkload struct {
	Groups          []icetable.CompactionTaskGroup
	SelectedFiles   int
	SelectedBytes   int64
	EqualityDeletes int
	PositionDeletes int
	GroupCount      int
}

func defaultNativeMaintenanceSettings() nativeMaintenanceSettings {
	return nativeMaintenanceSettings{
		MaxSelectedInputBytes:  defaultNativeMaxInputBytes,
		MaxSelectedFiles:       defaultNativeMaxInputFiles,
		TargetFileSizeBytes:    defaultNativeTargetBytes,
		SmallFileSizeBytes:     defaultNativeSmallBytes,
		MinSmallFiles:          defaultNativeMinSmallFiles,
		MinSmallBytes:          defaultNativeMinSmallBytes,
		ScanConcurrency:        1,
		Timeout:                defaultNativeTimeout,
		TempDirectory:          "/tmp/rivus-maintenance",
		ExpireInterval:         24 * time.Hour,
		SnapshotMaxAge:         7 * 24 * time.Hour,
		SnapshotRetainLast:     10,
		OrphanInterval:         30 * 24 * time.Hour,
		OrphanMinAge:           defaultNativeOrphanAge,
		OrphanDryRun:           false,
		SparkPollInterval:      5 * time.Second,
		SparkTimeout:           2 * time.Hour,
		IdleCompactionInterval: 7 * 24 * time.Hour,
	}
}

func executeNativeMaintenanceTask(
	ctx context.Context,
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
	setupCancel()
	if err != nil {
		result.Error = fmt.Sprintf("load table: %v", err)
		return finish(nativeTaskOutcome{Result: result, Retryable: !errorsIsNoSuchIcebergTable(err)})
	}

	switch task.Operation {
	case "compact":
		outcome := executeHybridCompaction(ctx, jobID, jobCfg, iceCfg, tbl, state, task, settings)
		outcome.Result.DurationMillis = time.Since(started).Milliseconds()
		return outcome
	case "expire_snapshots":
		taskCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
		defer cancel()
		outcome := executeNativeSnapshotExpiration(taskCtx, tbl, result, settings)
		outcome.Result.DurationMillis = time.Since(started).Milliseconds()
		return outcome
	case "remove_orphan_files":
		taskCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
		defer cancel()
		outcome := executeBoundedOrphanCleanup(taskCtx, tbl, result, settings)
		outcome.Result.DurationMillis = time.Since(started).Milliseconds()
		return outcome
	default:
		result.Error = fmt.Sprintf("unsupported maintenance operation %q", task.Operation)
		return finish(nativeTaskOutcome{Result: result})
	}
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

	cfg := compaction.DefaultConfig()
	cfg.TargetFileSizeBytes = settings.TargetFileSizeBytes
	cfg.MinFileSizeBytes = settings.SmallFileSizeBytes
	cfg.MaxFileSizeBytes = maxInt64(settings.TargetFileSizeBytes*9/5, settings.SmallFileSizeBytes+1)
	cfg.MinInputFiles = uint(settings.MinSmallFiles)
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
		"groups":                 work.GroupCount,
		"equality_delete_files":  work.EqualityDeletes,
		"position_delete_files":  work.PositionDeletes,
		"starting_snapshot_id":   startSnapshotID,
		"estimated_output_files": plan.EstOutputFiles,
		"estimated_output_bytes": plan.EstOutputBytes,
		"max_native_input_bytes": settings.MaxSelectedInputBytes,
		"max_native_input_files": settings.MaxSelectedFiles,
	}

	if work.SelectedFiles == 0 || work.SelectedBytes < settings.MinSmallBytes {
		result.Status = "skipped"
		result.RoutingReason = "no native compaction group meets minimum input thresholds"
		return nativeTaskOutcome{Result: result}
	}

	routeSpark, reason := shouldRouteCompactionToSpark(work, settings)
	if routeSpark {
		nativeCancel()
		result.Engine = "spark"
		result.RoutingReason = reason
		return executeSparkCompactionFallback(ctx, jobID, jobCfg, state, task, result, settings)
	}

	if err := tbl.Refresh(nativeCtx); err != nil {
		result.Error = fmt.Sprintf("refresh before compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if tbl.CurrentSnapshot() == nil || tbl.CurrentSnapshot().SnapshotID != startSnapshotID {
		result.Error = "current snapshot changed after compaction analysis; CDC wins and maintenance will retry"
		return nativeTaskOutcome{Result: result, Retryable: true}
	}

	rewrittenPaths := make(map[string]struct{}, work.SelectedFiles)
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
	result.RoutingReason = "selected workload is within native file and byte limits"
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
	seenEq := make(map[string]struct{})
	seenPos := make(map[string]struct{})
	for _, group := range plan.Groups {
		converted := icetable.CompactionTaskGroup{
			PartitionKey:   group.PartitionKey,
			Tasks:          group.Tasks,
			TotalSizeBytes: group.TotalSizeBytes,
		}
		work.Groups = append(work.Groups, converted)
		work.SelectedFiles += len(group.Tasks)
		work.SelectedBytes += group.TotalSizeBytes
		for _, task := range group.Tasks {
			for _, deleteFile := range task.EqualityDeleteFiles {
				if deleteFile != nil {
					seenEq[deleteFile.FilePath()] = struct{}{}
				}
			}
			for _, deleteFile := range task.DeleteFiles {
				if deleteFile != nil {
					seenPos[deleteFile.FilePath()] = struct{}{}
				}
			}
		}
	}
	work.EqualityDeletes = len(seenEq)
	work.PositionDeletes = len(seenPos)
	return work
}

func shouldRouteCompactionToSpark(work compactionWorkload, settings nativeMaintenanceSettings) (bool, string) {
	switch {
	case work.PositionDeletes > 0:
		return true, "position-delete files are present during the initial native rollout"
	case work.SelectedBytes > settings.MaxSelectedInputBytes:
		return true, fmt.Sprintf("selected input %d bytes exceeds native limit %d", work.SelectedBytes, settings.MaxSelectedInputBytes)
	case work.SelectedFiles > settings.MaxSelectedFiles:
		return true, fmt.Sprintf("selected input %d files exceeds native limit %d", work.SelectedFiles, settings.MaxSelectedFiles)
	case work.GroupCount > 1 && work.SelectedBytes > settings.MaxSelectedInputBytes/2:
		return true, "multiple substantial compaction groups are better isolated in Spark"
	default:
		return false, ""
	}
}

func executeSparkCompactionFallback(
	ctx context.Context,
	jobID string,
	jobCfg *config.JobConfig,
	state meta.IcebergMaintenanceState,
	task meta.IcebergMaintenanceTask,
	result meta.IcebergMaintenanceResult,
	settings nativeMaintenanceSettings,
) nativeTaskOutcome {
	request := TableMaintenanceRequest{
		Tables:         []string{tableKey(state.Namespace, state.Table)},
		ExternalRunKey: fmt.Sprintf("rivus-native-maintenance:%d", task.ID),
		Operations: []TableMaintenanceOperation{{
			Type: "rewrite_data_files",
			Options: map[string]any{
				"strategy": "binpack",
				"options": map[string]any{
					"target-file-size-bytes": fmt.Sprintf("%d", settings.TargetFileSizeBytes),
					"min-input-files":        fmt.Sprintf("%d", settings.MinSmallFiles),
				},
			},
		}},
	}
	submission, err := SubmitTableMaintenanceForJobConfig(ctx, jobID, jobCfg, request, false)
	if err != nil {
		result.Error = fmt.Sprintf("submit Spark fallback: %v", err)
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
			result.Error = "Spark fallback exceeded maintenance timeout"
			return nativeTaskOutcome{Result: result, Retryable: true}
		case <-ticker.C:
			status, err := GetTableMaintenanceStatusForJobConfig(ctx, jobCfg, submission.SubmissionID)
			if err != nil {
				result.Error = fmt.Sprintf("poll Spark fallback: %v", err)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
			switch strings.ToUpper(strings.TrimSpace(status.DriverState)) {
			case "FINISHED", "SUCCESS", "SUCCEEDED":
				result.Status = "succeeded"
				return nativeTaskOutcome{Result: result}
			case "FAILED", "ERROR", "KILLED", "CANCELLED", "CANCELED":
				result.Error = firstNonEmpty(strings.TrimSpace(status.Message), "Spark fallback failed")
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
		}
	}
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

var _ io.Reader = (*os.File)(nil)
