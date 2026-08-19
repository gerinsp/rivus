package api

import (
	"net/http"
	"sync"

	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
)

type Server struct {
	jobManager           *core.JobManager
	uiDir                string
	metrics              *MetricsSampler
	auth                 AuthConfig
	authSessions         *authSessionStore
	maintenanceStoreOnce sync.Once
	maintenanceStore     *meta.IcebergMaintenanceStore
	maintenanceStoreErr  error
}

func NewServer(jm *core.JobManager, uiDir string, auth AuthConfig) *Server {
	ms, err := NewMetricsSampler()
	if err != nil {
		// kalau gagal, tetap jalan tanpa metrics
		ms = nil
	} else {
		ms.Start()
	}

	return &Server{
		jobManager:   jm,
		uiDir:        uiDir,
		metrics:      ms,
		auth:         auth,
		authSessions: newAuthSessionStore(auth),
	}
}

func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /auth/status", s.handleAuthStatus)
	mux.HandleFunc("GET /login", s.handleLoginPage)
	mux.HandleFunc("GET /login.html", s.handleLoginPage)
	mux.HandleFunc("GET /favicon.svg", s.handleFavicon)
	mux.HandleFunc("GET /rivus-favicon.svg", s.handleFavicon)
	mux.HandleFunc("GET /rivus-logo.png", s.handleLogo)
	mux.HandleFunc("GET /api/version", s.handleVersion)
	mux.HandleFunc("POST /auth/login", s.handleLogin)
	mux.HandleFunc("POST /auth/logout", s.handleLogout)
	mux.HandleFunc("GET /metrics", s.handlePrometheusMetrics)

	mux.HandleFunc("/api/jobs", s.requireAPIAuth(s.handleJobs))
	// Exact GET route uses durable worker state so the master UI does not show
	// stale in-memory progress/maintenance after execution moved to workers.
	mux.HandleFunc("GET /api/jobs/{id}", s.requireAPIAuth(s.handleJobDetail))
	// Keep manual orphan cleanup on the master as a control-plane action only.
	// The specific route wins over the generic /api/jobs/ fallback below.
	mux.HandleFunc("POST /api/jobs/{id}/iceberg/orphans", s.requireAPIAuth(s.handleQueuedJobIcebergOrphans))
	mux.HandleFunc("/api/jobs/", s.requireAPIAuth(s.handleJobByID))

	mux.HandleFunc("GET /api/iceberg/maintenance/summary", s.requireAPIAuth(s.handleMaintenanceSummary))
	mux.HandleFunc("GET /api/iceberg/maintenance/runs", s.requireAPIAuth(s.handleMaintenanceRuns))
	mux.HandleFunc("GET /api/iceberg/maintenance/runs/{id}", s.requireAPIAuth(s.handleMaintenanceRun))
	mux.HandleFunc("GET /api/iceberg/maintenance/tables/{key...}", s.requireAPIAuth(s.handleMaintenanceTableState))

	mux.HandleFunc("/api/metrics", s.requireAPIAuth(s.handleMetrics))
	mux.HandleFunc("/api/table-metrics", s.requireAPIAuth(s.handleTableMetrics))
	// The master reads all role directories from one shared log root while each
	// runtime keeps its own rotating files under master/streaming/snapshot/maintenance.
	mux.HandleFunc("GET /api/logs", s.requireAPIAuth(s.handleSplitLogs))
	mux.HandleFunc("GET /api/logs/tail", s.requireAPIAuth(s.handleSplitLogTail))
	mux.HandleFunc("GET /api/logs/download", s.requireAPIAuth(s.handleSplitLogDownload))

	mux.Handle("GET /api/jobs/{id}/graph", s.requireAPIAuth(s.handleGetJobGraph))

	fs := http.FileServer(http.Dir(s.uiDir))
	staticHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Dashboard assets are deployed together with the binary. Do not let an
		// old ES module survive a rollout and keep stale UI behavior in the browser.
		w.Header().Set("Cache-Control", "no-store")
		fs.ServeHTTP(w, r)
	})
	mux.Handle("/", s.requirePageAuth(staticHandler))

	return mux
}
