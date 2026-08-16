package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func (s *Server) handleMaintenanceSummary(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	jobID := maintenanceJobID(r)
	if jobID != "" {
		summary, err := store.SummaryForOwner(r.Context(), jobID)
		if err != nil {
			maintenanceAPIError(w, err, http.StatusInternalServerError)
			return
		}
		maintenanceAPIJSON(w, http.StatusOK, summary)
		return
	}

	summary, err := store.Summary(r.Context(), time.Now().UTC())
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, summary)
}

func (s *Server) handleMaintenanceRuns(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	limit := maintenanceQueryInt(r, "limit", 50, 1, 200)
	offset := maintenanceQueryInt(r, "offset", 0, 0, 1_000_000)
	jobID := maintenanceJobID(r)

	var (
		runs []meta.IcebergMaintenanceRun
		err  error
	)
	if jobID != "" {
		runs, err = store.ListRunsForOwner(r.Context(), jobID, limit, offset)
	} else {
		runs, err = store.ListRuns(r.Context(), limit, offset)
	}
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{
		"runs":   runs,
		"limit":  limit,
		"offset": offset,
		"job_id": jobID,
	})
}

func (s *Server) handleMaintenanceRun(w http.ResponseWriter, r *http.Request) {
	runID, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if err != nil || runID <= 0 {
		http.Error(w, "invalid maintenance run id", http.StatusBadRequest)
		return
	}
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	limit := maintenanceQueryInt(r, "limit", 100, 1, 500)
	jobID := maintenanceJobID(r)

	var results []meta.IcebergMaintenanceResult
	if jobID != "" {
		results, err = store.ListResultsForRunOwner(r.Context(), runID, jobID, limit)
	} else {
		results, err = store.ListResultsForRun(r.Context(), runID, limit)
	}
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{
		"run_id":  runID,
		"results": results,
		"job_id":  jobID,
	})
}

func (s *Server) handleMaintenanceTableState(w http.ResponseWriter, r *http.Request) {
	tableKey := strings.TrimSpace(r.PathValue("key"))
	if tableKey == "" {
		http.Error(w, "maintenance table key is required", http.StatusBadRequest)
		return
	}
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
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

func (s *Server) maintenanceAPIStore(w http.ResponseWriter) (*meta.IcebergMaintenanceStore, bool) {
	s.maintenanceStoreOnce.Do(func() {
		dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
		if dsn == "" {
			s.maintenanceStoreErr = fmt.Errorf("maintenance metadata store is not configured")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		store, err := meta.NewIcebergMaintenanceStore(dsn)
		if err != nil {
			s.maintenanceStoreErr = err
			return
		}
		if err := store.Init(ctx); err != nil {
			store.Close()
			s.maintenanceStoreErr = err
			return
		}
		s.maintenanceStore = store
	})
	if s.maintenanceStoreErr != nil {
		status := http.StatusInternalServerError
		if strings.Contains(s.maintenanceStoreErr.Error(), "not configured") {
			status = http.StatusServiceUnavailable
		}
		maintenanceAPIError(w, s.maintenanceStoreErr, status)
		return nil, false
	}
	return s.maintenanceStore, true
}

func maintenanceJobID(r *http.Request) string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.URL.Query().Get("job_id"))
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
