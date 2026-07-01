package api

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/observability"
	"github.com/shirou/gopsutil/v3/process"
)

type Metrics struct {
	CPUPercent  float64 `json:"cpu_percent"`
	RSSBytes    uint64  `json:"rss_bytes"`
	VMSBytes    uint64  `json:"vms_bytes"`
	Goroutines  int     `json:"goroutines"`
	UptimeSec   int64   `json:"uptime_sec"`
	UpdatedUnix int64   `json:"updated_unix"`
	ProcessPID  int     `json:"pid"`
}

type MetricsSampler struct {
	mu    sync.RWMutex
	last  Metrics
	start time.Time
	proc  *process.Process
}

func NewMetricsSampler() (*MetricsSampler, error) {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}
	return &MetricsSampler{
		start: time.Now(),
		proc:  p,
	}, nil
}

func (m *MetricsSampler) Start() {
	// gopsutil CPUPercent butuh dipanggil berkala untuk dapat delta
	_ = m.warmup()

	t := time.NewTicker(2 * time.Second)
	go func() {
		defer t.Stop()
		for range t.C {
			_ = m.sample()
		}
	}()
}

func (m *MetricsSampler) warmup() error {
	_, _ = m.proc.CPUPercent()
	return nil
}

func (m *MetricsSampler) sample() error {
	cpu, err := m.proc.CPUPercent()
	if err != nil {
		return err
	}
	mem, err := m.proc.MemoryInfo()
	if err != nil {
		return err
	}

	out := Metrics{
		CPUPercent:  cpu,
		RSSBytes:    mem.RSS,
		VMSBytes:    mem.VMS,
		Goroutines:  runtime.NumGoroutine(),
		UptimeSec:   int64(time.Since(m.start).Seconds()),
		UpdatedUnix: time.Now().Unix(),
		ProcessPID:  os.Getpid(),
	}

	m.mu.Lock()
	m.last = out
	m.mu.Unlock()
	return nil
}

func (m *MetricsSampler) Get() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.last
}

func writeJSONAny(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.metrics == nil {
		writeJSONAny(w, http.StatusServiceUnavailable, map[string]string{"error": "metrics not enabled"})
		return
	}
	writeJSONAny(w, http.StatusOK, s.metrics.Get())
}

func (s *Server) handleTableMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSONAny(w, http.StatusOK, observability.TableActivitiesBySink(r.URL.Query().Get("sink")))
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	observability.WritePrometheus(w)
}
