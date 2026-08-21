package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

type memoryMaintenanceMonitorRepository struct {
	monitors      map[string]meta.IcebergMaintenanceMonitor
	requestedID   string
	requestedRuns int64
}

func newMemoryMaintenanceMonitorRepository() *memoryMaintenanceMonitorRepository {
	return &memoryMaintenanceMonitorRepository{monitors: make(map[string]meta.IcebergMaintenanceMonitor)}
}

func (s *memoryMaintenanceMonitorRepository) CreateMonitor(_ context.Context, monitor meta.IcebergMaintenanceMonitor) error {
	if _, exists := s.monitors[monitor.ID]; exists {
		return meta.ErrMaintenanceMonitorExists
	}
	now := time.Now().UTC()
	monitor.CreatedAt = now
	monitor.UpdatedAt = now
	s.monitors[monitor.ID] = monitor
	return nil
}

func (s *memoryMaintenanceMonitorRepository) ListMonitors(context.Context) ([]meta.IcebergMaintenanceMonitor, error) {
	out := make([]meta.IcebergMaintenanceMonitor, 0, len(s.monitors))
	for _, monitor := range s.monitors {
		out = append(out, monitor)
	}
	return out, nil
}

func (s *memoryMaintenanceMonitorRepository) GetMonitor(_ context.Context, id string) (*meta.IcebergMaintenanceMonitor, error) {
	monitor, ok := s.monitors[id]
	if !ok {
		return nil, nil
	}
	return &monitor, nil
}

func (s *memoryMaintenanceMonitorRepository) SetMonitorStatus(_ context.Context, id string, status meta.MaintenanceMonitorStatus, now time.Time) error {
	monitor, ok := s.monitors[id]
	if !ok {
		return meta.ErrMaintenanceMonitorNotFound
	}
	monitor.Status = status
	monitor.UpdatedAt = now
	s.monitors[id] = monitor
	return nil
}

func (s *memoryMaintenanceMonitorRepository) DeleteMonitor(_ context.Context, id string, _ time.Time) error {
	if _, ok := s.monitors[id]; !ok {
		return meta.ErrMaintenanceMonitorNotFound
	}
	delete(s.monitors, id)
	return nil
}

func (s *memoryMaintenanceMonitorRepository) RequestInventoryRefresh(_ context.Context, ownerID string, _ time.Time, _ bool) (int64, error) {
	s.requestedID = ownerID
	return s.requestedRuns, nil
}

func maintenanceMonitorYAML() string {
	return `
id: barayax-maintenance
name: Barayax Maintenance
mode: maintenance-only
sink:
  type: iceberg_native
  config:
    rest_uri: http://iceberg-rest:8181
    warehouse: s3://warehouse
    table_maintenance:
      enabled: true
      executor: native
      catalog_name: asmat
      tables:
        - namespace: barayax_bronze
          table: tbl_absen
`
}

func TestMaintenanceMonitorLifecycleAPI(t *testing.T) {
	repo := newMemoryMaintenanceMonitorRepository()
	repo.requestedRuns = 1
	srv := newTestServer(t, AuthConfig{Enabled: false, CookieName: defaultAuthCookie})
	srv.maintenanceMonitors = repo
	router := srv.Router()

	create := httptest.NewRequest(http.MethodPost, "/api/iceberg/maintenance/monitors", strings.NewReader(maintenanceMonitorYAML()))
	create.Header.Set("Content-Type", "application/x-yaml")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", createRec.Code, createRec.Body.String())
	}
	if strings.Contains(createRec.Body.String(), "warehouse") || strings.Contains(createRec.Body.String(), "rest_uri") {
		t.Fatalf("create response exposed monitor credentials/config: %s", createRec.Body.String())
	}

	for _, action := range []struct {
		path       string
		wantStatus meta.MaintenanceMonitorStatus
	}{
		{path: "/barayax-maintenance/pause", wantStatus: meta.MaintenanceMonitorPaused},
		{path: "/barayax-maintenance/resume", wantStatus: meta.MaintenanceMonitorActive},
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/iceberg/maintenance/monitors"+action.path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || repo.monitors["barayax-maintenance"].Status != action.wantStatus {
			t.Fatalf("action %s status=%d monitor=%s body=%s", action.path, rec.Code, repo.monitors["barayax-maintenance"].Status, rec.Body.String())
		}
	}

	run := httptest.NewRequest(http.MethodPost, "/api/iceberg/maintenance/monitors/barayax-maintenance/run", nil)
	runRec := httptest.NewRecorder()
	router.ServeHTTP(runRec, run)
	if runRec.Code != http.StatusAccepted || repo.requestedID != "monitor:barayax-maintenance" {
		t.Fatalf("run status=%d owner=%q body=%s", runRec.Code, repo.requestedID, runRec.Body.String())
	}

	list := httptest.NewRequest(http.MethodGet, "/api/iceberg/maintenance/monitors", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, list)
	if listRec.Code != http.StatusOK || !strings.Contains(listRec.Body.String(), `"total":1`) {
		t.Fatalf("list status=%d body=%s", listRec.Code, listRec.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/iceberg/maintenance/monitors/barayax-maintenance", nil)
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK || len(repo.monitors) != 0 {
		t.Fatalf("delete status=%d remaining=%d body=%s", deleteRec.Code, len(repo.monitors), deleteRec.Body.String())
	}
}

func TestMaintenanceMonitorAPIRejectsSourceConnector(t *testing.T) {
	repo := newMemoryMaintenanceMonitorRepository()
	srv := newTestServer(t, AuthConfig{Enabled: false, CookieName: defaultAuthCookie})
	srv.maintenanceMonitors = repo
	body := strings.Replace(maintenanceMonitorYAML(), "sink:\n", "source:\n  type: mysql\n  config: {}\nsink:\n", 1)
	req := httptest.NewRequest(http.MethodPost, "/api/iceberg/maintenance/monitors", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-yaml")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "must not define a source") {
		t.Fatalf("source validation status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestMaintenanceMonitorAPIRejectsDuplicateTableOwnership(t *testing.T) {
	repo := newMemoryMaintenanceMonitorRepository()
	srv := newTestServer(t, AuthConfig{Enabled: false, CookieName: defaultAuthCookie})
	srv.maintenanceMonitors = repo
	router := srv.Router()

	first := httptest.NewRequest(http.MethodPost, "/api/iceberg/maintenance/monitors", strings.NewReader(maintenanceMonitorYAML()))
	first.Header.Set("Content-Type", "application/x-yaml")
	firstRec := httptest.NewRecorder()
	router.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusCreated {
		t.Fatalf("first create status=%d body=%s", firstRec.Code, firstRec.Body.String())
	}

	duplicateBody := strings.Replace(maintenanceMonitorYAML(), "id: barayax-maintenance", "id: barayax-maintenance-2", 1)
	duplicate := httptest.NewRequest(http.MethodPost, "/api/iceberg/maintenance/monitors", strings.NewReader(duplicateBody))
	duplicate.Header.Set("Content-Type", "application/x-yaml")
	duplicateRec := httptest.NewRecorder()
	router.ServeHTTP(duplicateRec, duplicate)
	if duplicateRec.Code != http.StatusConflict || !strings.Contains(duplicateRec.Body.String(), "already owned") {
		t.Fatalf("duplicate create status=%d body=%s", duplicateRec.Code, duplicateRec.Body.String())
	}
}
