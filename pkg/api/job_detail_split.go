package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
)

// handleJobDetail is the split-runtime aware job detail endpoint. The master
// owns no execution pipeline, so runtime progress comes from job_registry and
// Iceberg maintenance state comes directly from the durable maintenance store.
func (s *Server) handleJobDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	if err := s.jobManager.RefreshPersistedViews(r.Context()); err != nil {
		fmt.Printf("[api] refresh persisted job view failed job=%s: %v\n", id, err)
	}
	job, err := s.jobManager.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	maintenance := any(job.TableMaintenanceStatus())
	if durable, durableErr := s.durableIcebergMaintenanceView(r.Context(), job); durableErr != nil {
		fmt.Printf("[api] durable maintenance view failed job=%s: %v\n", id, durableErr)
	} else if durable != nil {
		maintenance = durable
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":                  job.Config.ID,
		"name":                job.Config.Name,
		"status":              job.GetStatus(),
		"created":             job.Created,
		"updated":             job.Updated,
		"meta_key":            job.MetaKey(),
		"checkpoint":          job.Checkpoint(),
		"progress":            job.Progress(),
		"iceberg_maintenance": maintenance,
		"last_error":          job.GetLastError(),
		"errors":              job.GetErrors(),
		"config":              job.Config,
	})
}

func (s *Server) durableIcebergMaintenanceView(ctx context.Context, job *core.Job) (map[string]any, error) {
	if job == nil || job.Config == nil || job.Config.Sink == nil || !strings.EqualFold(strings.TrimSpace(job.Config.Sink.Type), "iceberg_native") {
		return nil, nil
	}

	var icebergCfg config.IcebergConfig
	if raw, err := yaml.Marshal(job.Config.Sink.Config); err == nil {
		_ = yaml.Unmarshal(raw, &icebergCfg)
	}
	tm := icebergCfg.TableMaintenance

	store, err := s.maintenanceStoreForView(ctx)
	if err != nil {
		return nil, err
	}
	states, err := store.ListStatesForOwner(ctx, job.Config.ID, 5000)
	if err != nil {
		return nil, err
	}
	summary, err := store.SummaryForOwner(ctx, job.Config.ID)
	if err != nil {
		return nil, err
	}

	enabled := tm.Enabled || tm.NativeEnabled || len(states) > 0 || summary.Tables > 0
	if !enabled {
		return map[string]any{
			"enabled": false,
			"state":   "disabled",
		}, nil
	}

	dataThreshold := tm.DataFilesThreshold
	if dataThreshold <= 0 {
		dataThreshold = 200
	}
	deleteThreshold := tm.EqualityDeleteFilesThreshold
	if deleteThreshold <= 0 {
		deleteThreshold = 50
	}
	positionDeleteThreshold := tm.PositionDeleteFilesThreshold
	if positionDeleteThreshold <= 0 {
		positionDeleteThreshold = 25
	}
	smallFileSize := tm.SmallFileSizeBytes
	if smallFileSize <= 0 {
		smallFileSize = 64 * 1024 * 1024
	}
	smallFilesMinCount := tm.SmallFilesMinCount
	if smallFilesMinCount <= 0 {
		smallFilesMinCount = 10
	}
	smallFilesMinBytes := tm.SmallFilesMinTotalBytes
	if smallFilesMinBytes <= 0 {
		smallFilesMinBytes = 256 * 1024 * 1024
	}
	maxConcurrent := tm.MaxConcurrentJobs
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	resourceProfile := strings.TrimSpace(tm.RunnerResourceProfile)
	if resourceProfile == "" {
		resourceProfile = "small"
	}
	catalogName := strings.TrimSpace(tm.CatalogName)
	if catalogName == "" {
		catalogName = strings.TrimSpace(icebergCfg.CatalogName)
	}

	now := time.Now().UTC()
	tables := make([]map[string]any, 0, len(states))
	activeDataFiles := 0
	activeEqualityDeletes := 0
	activePositionDeletes := 0
	eligibleSmallFiles := 0
	eligibleSmallBytes := int64(0)
	tablesScanned := 0
	tablesReady := 0
	inventoryErrors := 0
	inventoryScanning := false
	inventoryPending := false
	var latest time.Time

	for _, state := range states {
		tableState := durableMaintenanceStateForView(state, tm, now)
		if tableState == "scanning" {
			inventoryScanning = true
		}
		if tableState == "inventory_pending" {
			inventoryPending = true
		}
		if tableState == "ready" || tableState == "running" {
			tablesReady++
		}
		if strings.TrimSpace(state.LastError) != "" {
			inventoryErrors++
		}

		checkedAt := ""
		if state.LastInventoryAt != nil {
			checkedAt = state.LastInventoryAt.UTC().Format(time.RFC3339)
			tablesScanned++
			if state.LastInventoryAt.After(latest) {
				latest = *state.LastInventoryAt
			}
		}

		activeDataFiles += state.ActiveDataFiles
		activeEqualityDeletes += state.ActiveEqualityDeleteFiles
		activePositionDeletes += state.ActivePositionDeleteFiles
		eligibleSmallFiles += state.ActiveSmallFiles
		eligibleSmallBytes += state.ActiveSmallBytes

		tables = append(tables, map[string]any{
			"namespace":                    state.Namespace,
			"table":                        state.Table,
			"identifier":                   state.Namespace + "." + state.Table,
			"state":                        tableState,
			"active_data_files":            state.ActiveDataFiles,
			"active_equality_delete_files": state.ActiveEqualityDeleteFiles,
			"active_position_delete_files": state.ActivePositionDeleteFiles,
			"eligible_small_files":         state.ActiveSmallFiles,
			"eligible_small_bytes":         state.ActiveSmallBytes,
			"new_data_files":               state.NewDataFiles,
			"new_equality_delete_files":    state.NewEqualityDeleteFiles,
			"checked_at":                    checkedAt,
			"error":                         state.LastError,
			"operations":                    []string{},
		})
	}

	sort.Slice(tables, func(i, j int) bool {
		return fmt.Sprint(tables[i]["identifier"]) < fmt.Sprint(tables[j]["identifier"])
	})

	state := "healthy"
	switch {
	case summary.ActiveLeases > 0:
		state = "running"
	case inventoryScanning:
		state = "scanning"
	case inventoryErrors > 0:
		state = "error"
	case inventoryPending:
		state = "inventory_pending"
	case summary.Blocked > 0 && summary.Blocked == summary.Tables:
		state = "waiting_for_snapshot"
	case summary.Tables > 0 && tablesScanned == 0:
		state = "inventory_pending"
	case summary.QueuedTasks > 0 || summary.RetryTasks > 0 || tablesReady > 0:
		state = "ready"
	}

	checkedAt := ""
	if !latest.IsZero() {
		checkedAt = latest.UTC().Format(time.RFC3339)
	}

	return map[string]any{
		"enabled":                         true,
		"state":                           state,
		"catalog_name":                    catalogName,
		"runner_resource_profile":         resourceProfile,
		"max_concurrent_jobs":             maxConcurrent,
		"data_files_threshold":            dataThreshold,
		"equality_delete_files_threshold": deleteThreshold,
		"position_delete_files_threshold": positionDeleteThreshold,
		"small_file_size_bytes":           smallFileSize,
		"small_files_min_count":           smallFilesMinCount,
		"small_files_min_total_bytes":     smallFilesMinBytes,
		"active_data_files":               activeDataFiles,
		"active_equality_delete_files":    activeEqualityDeletes,
		"active_position_delete_files":    activePositionDeletes,
		"eligible_small_files":            eligibleSmallFiles,
		"eligible_small_bytes":            eligibleSmallBytes,
		"tables_total":                    summary.Tables,
		"tables_scanned":                  tablesScanned,
		"tables_ready":                    tablesReady,
		"active_runs":                     summary.ActiveLeases,
		"inventory_errors":                inventoryErrors,
		"paused":                          false,
		"checked_at":                      checkedAt,
		"tables":                          tables,
	}, nil
}

func durableMaintenanceStateForView(state meta.IcebergMaintenanceState, cfg config.IcebergTableMaintenanceConfig, now time.Time) string {
	if !state.SnapshotComplete {
		return "waiting_for_snapshot"
	}
	if state.InventoryLeaseUntil != nil && state.InventoryLeaseUntil.After(now) {
		return "scanning"
	}
	if state.LeaseUntil != nil && state.LeaseUntil.After(now) {
		return "running"
	}
	if strings.TrimSpace(state.LastError) != "" {
		return "error"
	}
	if state.NextInventoryCheckAt != nil || state.LastInventoryAt == nil {
		return "inventory_pending"
	}

	dataThreshold := cfg.DataFilesThreshold
	if dataThreshold <= 0 {
		dataThreshold = 200
	}
	deleteThreshold := cfg.EqualityDeleteFilesThreshold
	if deleteThreshold <= 0 {
		deleteThreshold = 50
	}
	positionDeleteThreshold := cfg.PositionDeleteFilesThreshold
	if positionDeleteThreshold <= 0 {
		positionDeleteThreshold = 25
	}
	smallMinCount := cfg.SmallFilesMinCount
	if smallMinCount <= 0 {
		smallMinCount = 10
	}
	smallMinBytes := cfg.SmallFilesMinTotalBytes
	if smallMinBytes <= 0 {
		smallMinBytes = 256 * 1024 * 1024
	}

	if state.ActiveSmallFiles >= dataThreshold ||
		state.ActiveEqualityDeleteFiles >= deleteThreshold ||
		state.ActivePositionDeleteFiles >= positionDeleteThreshold ||
		(state.ActiveSmallFiles >= smallMinCount && state.ActiveSmallBytes >= smallMinBytes) {
		return "ready"
	}
	return "healthy"
}

func (s *Server) maintenanceStoreForView(ctx context.Context) (*meta.IcebergMaintenanceStore, error) {
	s.maintenanceStoreOnce.Do(func() {
		dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
		if dsn == "" {
			s.maintenanceStoreErr = fmt.Errorf("maintenance metadata store is not configured")
			return
		}
		base := context.Background()
		if ctx != nil {
			base = context.WithoutCancel(ctx)
		}
		initCtx, cancel := context.WithTimeout(base, 3*time.Second)
		defer cancel()
		store, err := meta.NewIcebergMaintenanceStore(dsn)
		if err != nil {
			s.maintenanceStoreErr = err
			return
		}
		if err := store.Init(initCtx); err != nil {
			_ = store.Close()
			s.maintenanceStoreErr = err
			return
		}
		s.maintenanceStore = store
	})
	if s.maintenanceStoreErr != nil {
		return nil, s.maintenanceStoreErr
	}
	if s.maintenanceStore == nil {
		return nil, fmt.Errorf("maintenance metadata store is unavailable")
	}
	return s.maintenanceStore, nil
}
