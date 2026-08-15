package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func (s *Server) handleMaintenanceSummary(w http.ResponseWriter, r *http.Request) {
	store, cancel, ok := maintenanceAPIStore(w)
	if !ok {
		return
	}
	defer cancel()
	defer store.Close()
	summary, err := store.Summary(r.Context(), time.Now().UTC())
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, summary)
}

func (s *Server) handleMaintenanceRuns(w http.ResponseWriter, r *http.Request) {
	store, cancel, ok := maintenanceAPIStore(w)
	if !ok {
		return
	}
	defer cancel()
	defer store.Close()
	limit := maintenanceQueryInt(r, "limit", 50, 1, 200)
	offset := maintenanceQueryInt(r, "offset", 0, 0, 1_000_000)
	runs, err := store.ListRuns(r.Context(), limit, offset)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{
		"runs":   runs,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleMaintenanceRun(w http.ResponseWriter, r *http.Request) {
	runID, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if err != nil || runID <= 0 {
		http.Error(w, "invalid maintenance run id", http.StatusBadRequest)
		return
	}
	store, cancel, ok := maintenanceAPIStore(w)
	if !ok {
		return
	}
	defer cancel()
	defer store.Close()
	limit := maintenanceQueryInt(r, "limit", 100, 1, 500)
	results, err := store.ListResultsForRun(r.Context(), runID, limit)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{
		"run_id":  runID,
		"results": results,
	})
}

func (s *Server) handleMaintenanceTableState(w http.ResponseWriter, r *http.Request) {
	tableKey := strings.TrimSpace(r.PathValue("key"))
	if tableKey == "" {
		http.Error(w, "maintenance table key is required", http.StatusBadRequest)
		return
	}
	store, cancel, ok := maintenanceAPIStore(w)
	if !ok {
		return
	}
	defer cancel()
	defer store.Close()
	state, err := store.GetState(r.Context(), tableKey)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if state == nil {
		http.Error(w, "maintenance table state not found", http.StatusNotFound)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, state)
}

func maintenanceAPIStore(w http.ResponseWriter) (*meta.IcebergMaintenanceStore, context.CancelFunc, bool) {
	dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
	if dsn == "" {
		http.Error(w, "maintenance metadata store is not configured", http.StatusServiceUnavailable)
		return nil, func() {}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		cancel()
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return nil, func() {}, false
	}
	if err := store.Init(ctx); err != nil {
		store.Close()
		cancel()
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return nil, func() {}, false
	}
	cancel()
	return store, func() {}, true
}

func maintenanceQueryInt(r *http.Request, key string, fallback, minValue, maxValue int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minValue || value > maxValue {
		return fallback
	}
	return value
}

func maintenanceAPIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func maintenanceAPIError(w http.ResponseWriter, err error, status int) {
	maintenanceAPIJSON(w, status, map[string]string{"error": err.Error()})
}
