package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	queuedOrphanOperation = "remove_orphan_files"
	queuedOrphanPriority  = 5
	queuedOrphanMinAge    = 7 * 24 * time.Hour
)

type queuedIcebergOrphanCleanupRequest struct {
	DryRun         *bool    `json:"dry_run"`
	OlderThanHours float64  `json:"older_than_hours"`
	MaxConcurrency int      `json:"max_concurrency"`
	Tables         []string `json:"tables"`
}

type queuedOrphanOptions struct {
	DryRun         bool
	OlderThanHours float64
	Tables         []string
}

// handleQueuedJobIcebergOrphans keeps the API/master process as a pure
// control plane. The request is converted into durable table-maintenance tasks
// and the maintenance worker performs the actual object-store scan/deletion.
func (s *Server) handleQueuedJobIcebergOrphans(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	job, err := s.jobManager.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	if job.Config == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "job configuration is unavailable"})
		return
	}

	var req queuedIcebergOrphanCleanupRequest
	if r.Body != nil {
		defer r.Body.Close()
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil && err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}

	opts, err := normalizeQueuedOrphanOptions(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	states, err := store.ListStatesForOwner(r.Context(), id, 5000)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if len(states) == 0 {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "maintenance state is not initialized for this job; start the maintenance worker and retry",
		})
		return
	}

	selected, missing := selectQueuedOrphanTables(states, opts.Tables)
	if len(missing) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":          "one or more requested tables are not registered in durable maintenance state",
			"missing_tables": missing,
		})
		return
	}

	now := time.Now().UTC()
	scheduleWindow := fmt.Sprintf("manual-orphan-%d", now.UnixNano())
	payload := map[string]any{
		"manual":  true,
		"dry_run": opts.DryRun,
	}
	if opts.OlderThanHours > 0 {
		payload["older_than_hours"] = opts.OlderThanHours
	}

	queued := 0
	alreadyActive := 0
	blocked := 0
	for _, state := range states {
		if !selected[state.TableKey] {
			continue
		}
		if !state.SnapshotComplete {
			blocked++
			continue
		}
		inserted, err := store.EnqueueTask(
			r.Context(),
			state,
			queuedOrphanOperation,
			queuedOrphanPriority,
			scheduleWindow,
			now,
			payload,
		)
		if err != nil {
			maintenanceAPIError(w, err, http.StatusInternalServerError)
			return
		}
		if inserted {
			queued++
		} else {
			alreadyActive++
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id":             id,
		"operation":          queuedOrphanOperation,
		"queued":             queued,
		"already_active":     alreadyActive,
		"snapshot_blocked":   blocked,
		"dry_run":            opts.DryRun,
		"older_than_hours":   opts.OlderThanHours,
		"maintenance_worker": true,
		"message":            "orphan cleanup queued for the maintenance worker",
	})
}

func normalizeQueuedOrphanOptions(req queuedIcebergOrphanCleanupRequest) (queuedOrphanOptions, error) {
	dryRun := true
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}
	if req.OlderThanHours < 0 {
		return queuedOrphanOptions{}, fmt.Errorf("older_than_hours cannot be negative")
	}
	if req.OlderThanHours > 0 {
		age := time.Duration(req.OlderThanHours * float64(time.Hour))
		if age < queuedOrphanMinAge {
			return queuedOrphanOptions{}, fmt.Errorf(
				"older_than_hours must be at least %.0f for queued native orphan cleanup",
				queuedOrphanMinAge.Hours(),
			)
		}
	}
	if req.MaxConcurrency != 0 {
		return queuedOrphanOptions{}, fmt.Errorf(
			"max_concurrency is controlled by RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY in queued mode",
		)
	}

	tables := make([]string, 0, len(req.Tables))
	seen := make(map[string]struct{}, len(req.Tables))
	for _, raw := range req.Tables {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, value)
	}
	return queuedOrphanOptions{DryRun: dryRun, OlderThanHours: req.OlderThanHours, Tables: tables}, nil
}

func selectQueuedOrphanTables(states []meta.IcebergMaintenanceState, requested []string) (map[string]bool, []string) {
	selected := make(map[string]bool)
	if len(requested) == 0 {
		for _, state := range states {
			selected[state.TableKey] = true
		}
		return selected, nil
	}

	matched := make(map[string]bool, len(requested))
	for _, state := range states {
		for _, raw := range requested {
			if queuedOrphanTableMatches(raw, state.TableKey, state.Catalog, state.Namespace, state.Table) {
				selected[state.TableKey] = true
				matched[strings.ToLower(strings.TrimSpace(raw))] = true
			}
		}
	}
	missing := make([]string, 0)
	for _, raw := range requested {
		if !matched[strings.ToLower(strings.TrimSpace(raw))] {
			missing = append(missing, raw)
		}
	}
	sort.Strings(missing)
	return selected, missing
}

func queuedOrphanTableMatches(requested, tableKey, catalog, namespace, table string) bool {
	requested = strings.ToLower(strings.TrimSpace(requested))
	if requested == "" {
		return false
	}
	candidates := []string{
		tableKey,
		table,
		namespace + "." + table,
		catalog + "." + namespace + "." + table,
	}
	for _, candidate := range candidates {
		if requested == strings.ToLower(strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}
