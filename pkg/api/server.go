package api

import (
	"net/http"

	"github.com/gerinsp/rivus/pkg/core"
)

type Server struct {
	jobManager   *core.JobManager
	uiDir        string
	metrics      *MetricsSampler
	auth         AuthConfig
	authSessions *authSessionStore
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
	mux.HandleFunc("/api/jobs/", s.requireAPIAuth(s.handleJobByID))

	mux.HandleFunc("/api/metrics", s.requireAPIAuth(s.handleMetrics))
	mux.HandleFunc("/api/table-metrics", s.requireAPIAuth(s.handleTableMetrics))
	mux.HandleFunc("GET /api/logs", s.requireAPIAuth(s.handleLogs))
	mux.HandleFunc("GET /api/logs/tail", s.requireAPIAuth(s.handleLogTail))
	mux.HandleFunc("GET /api/logs/download", s.requireAPIAuth(s.handleLogDownload))

	mux.Handle("GET /api/jobs/{id}/graph", s.requireAPIAuth(s.handleGetJobGraph))

	fs := http.FileServer(http.Dir(s.uiDir))
	mux.Handle("/", s.requirePageAuth(fs))

	return mux
}
