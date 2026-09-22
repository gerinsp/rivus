package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connectors/iceberg"
	"github.com/gerinsp/rivus/pkg/meta"
)

type maintenanceMonitorRepository interface {
	CreateMonitor(context.Context, meta.IcebergMaintenanceMonitor) error
	ListMonitors(context.Context) ([]meta.IcebergMaintenanceMonitor, error)
	GetMonitor(context.Context, string) (*meta.IcebergMaintenanceMonitor, error)
	SetMonitorStatus(context.Context, string, meta.MaintenanceMonitorStatus, time.Time) error
	DeleteMonitor(context.Context, string, time.Time) error
	RequestInventoryRefresh(context.Context, string, time.Time, bool) (int64, error)
}

type maintenanceMonitorView struct {
	ID              string                        `json:"id"`
	Name            string                        `json:"name"`
	Status          meta.MaintenanceMonitorStatus `json:"status"`
	Catalog         string                        `json:"catalog"`
	Executor        string                        `json:"executor"`
	ResourceProfile string                        `json:"resource_profile"`
	Tables          []config.IcebergTarget        `json:"tables"`
	TableCount      int                           `json:"table_count"`
	DiscoveredCount int                           `json:"discovered_count"`
	OwnedCount      int                           `json:"owned_count"`
	ReservedCount   int                           `json:"reserved_count"`
	ExcludedCount   int                           `json:"excluded_scope_count"`
	ConflictCount   int                           `json:"conflict_count"`
	RetiredCount    int                           `json:"retired_count"`
	LastInventoryAt *time.Time                    `json:"last_inventory_at,omitempty"`
	LastDiscoveryAt *time.Time                    `json:"last_discovery_at,omitempty"`
	DiscoveryError  string                        `json:"last_discovery_error,omitempty"`
	LastError       string                        `json:"last_error,omitempty"`
	CreatedAt       time.Time                     `json:"created_at"`
	UpdatedAt       time.Time                     `json:"updated_at"`
	Maintenance     map[string]any                `json:"maintenance,omitempty"`
}

func (s *Server) handleMaintenanceMonitors(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceMonitorAPIStore(w)
	if !ok {
		return
	}
	if r.Method == http.MethodGet {
		monitors, err := store.ListMonitors(r.Context())
		if err != nil {
			maintenanceAPIError(w, err, http.StatusInternalServerError)
			return
		}
		views := make([]maintenanceMonitorView, 0, len(monitors))
		for _, monitor := range monitors {
			view, err := maintenanceMonitorResponse(monitor)
			if err != nil {
				maintenanceAPIError(w, fmt.Errorf("monitor %s: %w", monitor.ID, err), http.StatusInternalServerError)
				return
			}
			views = append(views, view)
		}
		maintenanceAPIJSON(w, http.StatusOK, map[string]any{"monitors": views, "total": len(views)})
		return
	}

	cfg, err := decodeMaintenanceMonitorRequest(w, r)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusBadRequest)
		return
	}
	normalized, _, err := iceberg.PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusBadRequest)
		return
	}
	excludedCount, err := iceberg.MaintenanceMonitorExcludedScopeCount(normalized)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusBadRequest)
		return
	}
	monitor := meta.IcebergMaintenanceMonitor{
		ID: normalized.ID, Name: normalized.Name, Status: meta.MaintenanceMonitorActive, Config: normalized,
		ExcludedScopeCount: excludedCount,
	}
	if err := store.CreateMonitor(r.Context(), monitor); err != nil {
		if errors.Is(err, meta.ErrMaintenanceMonitorExists) {
			maintenanceAPIError(w, err, http.StatusConflict)
			return
		}
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	created, err := store.GetMonitor(r.Context(), normalized.ID)
	if err != nil || created == nil {
		if err == nil {
			err = fmt.Errorf("created maintenance monitor is unavailable")
		}
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	view, err := maintenanceMonitorResponse(*created)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusCreated, view)
}

func (s *Server) handleMaintenanceMonitor(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceMonitorAPIStore(w)
	if !ok {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		maintenanceAPIError(w, fmt.Errorf("maintenance monitor id is required"), http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodDelete {
		if err := store.DeleteMonitor(r.Context(), id, time.Now().UTC()); err != nil {
			maintenanceMonitorMutationError(w, err)
			return
		}
		maintenanceAPIJSON(w, http.StatusOK, map[string]any{"id": id, "status": "DELETED"})
		return
	}
	monitor, err := store.GetMonitor(r.Context(), id)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if monitor == nil {
		maintenanceAPIError(w, meta.ErrMaintenanceMonitorNotFound, http.StatusNotFound)
		return
	}
	view, err := maintenanceMonitorResponse(*monitor)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if s.maintenanceMonitors == nil {
		detail, detailErr := s.durableIcebergMaintenanceConfigView(
			r.Context(), monitor.Config, meta.MaintenanceMonitorOwnerID(monitor.ID), monitor.Status == meta.MaintenanceMonitorPaused,
		)
		if detailErr != nil {
			maintenanceAPIError(w, detailErr, http.StatusInternalServerError)
			return
		}
		view.Maintenance = detail
	}
	maintenanceAPIJSON(w, http.StatusOK, view)
}

func (s *Server) handleMaintenanceMonitorPause(w http.ResponseWriter, r *http.Request) {
	s.handleMaintenanceMonitorStatus(w, r, meta.MaintenanceMonitorPaused)
}

func (s *Server) handleMaintenanceMonitorResume(w http.ResponseWriter, r *http.Request) {
	s.handleMaintenanceMonitorStatus(w, r, meta.MaintenanceMonitorActive)
}

func (s *Server) handleMaintenanceMonitorStatus(w http.ResponseWriter, r *http.Request, status meta.MaintenanceMonitorStatus) {
	store, ok := s.maintenanceMonitorAPIStore(w)
	if !ok {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if err := store.SetMonitorStatus(r.Context(), id, status, time.Now().UTC()); err != nil {
		maintenanceMonitorMutationError(w, err)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{"id": id, "status": status})
}

func (s *Server) handleMaintenanceMonitorRun(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceMonitorAPIStore(w)
	if !ok {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	monitor, err := store.GetMonitor(r.Context(), id)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if monitor == nil {
		maintenanceAPIError(w, meta.ErrMaintenanceMonitorNotFound, http.StatusNotFound)
		return
	}
	if monitor.Status != meta.MaintenanceMonitorActive {
		maintenanceAPIError(w, fmt.Errorf("maintenance monitor must be ACTIVE before run now"), http.StatusConflict)
		return
	}
	requested, err := store.RequestInventoryRefresh(r.Context(), meta.MaintenanceMonitorOwnerID(id), time.Now().UTC(), true)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusAccepted, map[string]any{
		"id": id, "status": monitor.Status, "requested": requested,
		"message": "inventory refresh queued; eligible maintenance follows the refreshed table state",
	})
}

func (s *Server) maintenanceMonitorAPIStore(w http.ResponseWriter) (maintenanceMonitorRepository, bool) {
	if s.maintenanceMonitors != nil {
		return s.maintenanceMonitors, true
	}
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return nil, false
	}
	return store, true
}

func decodeMaintenanceMonitorRequest(w http.ResponseWriter, r *http.Request) (*config.JobConfig, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		var cfg config.JobConfig
		expanded := []byte(config.ExpandEnvPlaceholders(string(body)))
		if err := json.Unmarshal(expanded, &cfg); err != nil {
			return nil, err
		}
		config.ApplyDefaults(&cfg)
		return &cfg, nil
	}
	return config.LoadJobConfigFromBytes(body)
}

func maintenanceMonitorResponse(monitor meta.IcebergMaintenanceMonitor) (maintenanceMonitorView, error) {
	catalog, executor, profile, tables, err := iceberg.DescribeMaintenanceMonitorConfig(monitor.Config)
	if err != nil {
		return maintenanceMonitorView{}, err
	}
	tableCount := monitor.TableCount
	if tableCount == 0 {
		tableCount = len(tables)
	}
	return maintenanceMonitorView{
		ID: monitor.ID, Name: monitor.Name, Status: monitor.Status,
		Catalog: catalog, Executor: executor, ResourceProfile: profile,
		Tables: tables, TableCount: tableCount, LastInventoryAt: monitor.LastInventoryAt,
		DiscoveredCount: monitor.DiscoveredCount, OwnedCount: monitor.OwnedCount,
		ReservedCount: monitor.ReservedCount, ExcludedCount: monitor.ExcludedScopeCount,
		ConflictCount: monitor.ConflictCount, RetiredCount: monitor.RetiredCount,
		LastDiscoveryAt: monitor.LastDiscoveryAt, DiscoveryError: monitor.LastDiscoveryError,
		LastError: monitor.LastError, CreatedAt: monitor.CreatedAt, UpdatedAt: monitor.UpdatedAt,
	}, nil
}

func maintenanceMonitorMutationError(w http.ResponseWriter, err error) {
	if errors.Is(err, meta.ErrMaintenanceMonitorNotFound) {
		maintenanceAPIError(w, err, http.StatusNotFound)
		return
	}
	maintenanceAPIError(w, err, http.StatusInternalServerError)
}
