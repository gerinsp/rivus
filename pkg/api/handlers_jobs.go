package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connectors/iceberg"
	"github.com/gerinsp/rivus/pkg/core"
)

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		jobs := s.jobManager.List()
		writeJSON(w, http.StatusOK, jobs)
	case http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		defer r.Body.Close()

		var cfg *config.JobConfig
		contentType := r.Header.Get("Content-Type")

		if strings.Contains(contentType, "application/json") {
			var c config.JobConfig
			expandedBody := []byte(config.ExpandEnvPlaceholders(string(body)))
			if err := json.Unmarshal(expandedBody, &c); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			config.ApplyDefaults(&c)
			cfg = &c
		} else {
			configs, err := config.LoadJobConfigsFromBytes(body)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if len(configs) == 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job config is empty"})
				return
			}
			if len(configs) > 1 {
				results := s.jobManager.SubmitMany(configs)
				writeJSON(w, http.StatusCreated, batchSubmitResponse(results))
				return
			}
			cfg = configs[0]
		}

		if s.jobManager.HasJob(cfg.ID) {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": fmt.Sprintf("job id %q already exists", cfg.ID),
			})
			return
		}
		job, err := s.jobManager.Submit(cfg)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		resp := map[string]interface{}{
			"id":     job.Config.ID,
			"status": job.GetStatus(),
		}
		if lastErr := job.GetLastError(); lastErr != nil {
			resp["error"] = lastErr.Message
		}
		writeJSON(w, http.StatusCreated, resp)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func batchSubmitResponse(results []core.SubmitResult) map[string]interface{} {
	counts := map[string]int{
		"submitted": 0,
		"queued":    0,
		"skipped":   0,
		"failed":    0,
	}
	for _, result := range results {
		switch result.Action {
		case "submitted":
			counts["submitted"]++
			if result.Status == core.JobStatusQueued {
				counts["queued"]++
			}
		case "skipped":
			counts["skipped"]++
		case "failed":
			counts["failed"]++
		}
	}
	return map[string]interface{}{
		"batch":   true,
		"counts":  counts,
		"results": results,
	}
}

func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	id := parts[0]

	if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		if err := s.jobManager.Cancel(id); err != nil {
			if err == core.ErrJobNotFound {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			} else {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": string(core.JobStatusStopped)})
		return
	}

	if len(parts) == 2 && parts[1] == "pause" && r.Method == http.MethodPost {
		if err := s.jobManager.Pause(id); err != nil {
			switch err {
			case core.ErrJobNotFound:
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			case core.ErrJobPauseNotAllowed:
				writeJSON(w, http.StatusConflict, map[string]string{"error": "job can only be paused while RUNNING"})
			default:
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": string(core.JobStatusPausing)})
		return
	}

	if len(parts) == 2 && parts[1] == "resubmit" && r.Method == http.MethodPost {
		job, err := s.jobManager.Resubmit(id)
		if err != nil {
			switch err {
			case core.ErrJobNotFound:
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			case core.ErrJobResubmitNotAllowed:
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job can only be resubmitted from PAUSED, FAILED, or STOPPED"})
			case core.ErrJobStillStopping:
				writeJSON(w, http.StatusConflict, map[string]string{"error": "job pipeline is still stopping; retry resubmit shortly"})
			default:
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			}
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":     job.Config.ID,
			"status": job.GetStatus(),
			"mode":   "resume",
		})
		return
	}

	if len(parts) == 3 && parts[1] == "iceberg" && parts[2] == "orphans" && r.Method == http.MethodPost {
		s.handleJobIcebergOrphans(w, r, id)
		return
	}

	if r.Method == http.MethodGet {
		job, err := s.jobManager.Get(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":         job.Config.ID,
			"name":       job.Config.Name,
			"status":     job.GetStatus(),
			"created":    job.Created,
			"updated":    job.Updated,
			"meta_key":   job.MetaKey(),
			"checkpoint": job.Checkpoint(),
			"progress":   job.Progress(),
			"last_error": job.GetLastError(),
			"errors":     job.GetErrors(),
			"config":     job.Config,
		})
		return
	}

	if r.Method == http.MethodDelete {
		if err := s.jobManager.Delete(id); err != nil {
			if err == core.ErrJobNotFound {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			} else {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

type icebergOrphanCleanupRequest struct {
	DryRun         *bool    `json:"dry_run"`
	OlderThanHours float64  `json:"older_than_hours"`
	MaxConcurrency int      `json:"max_concurrency"`
	Tables         []string `json:"tables"`
}

func (s *Server) handleJobIcebergOrphans(w http.ResponseWriter, r *http.Request, id string) {
	job, err := s.jobManager.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	status := job.GetStatus()

	req := icebergOrphanCleanupRequest{}
	if r.Body != nil {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}

	dryRun := true
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}
	olderThan := time.Duration(0)
	if req.OlderThanHours > 0 {
		olderThan = time.Duration(req.OlderThanHours * float64(time.Hour))
	}
	effectiveOlderThan := olderThan
	if effectiveOlderThan <= 0 {
		effectiveOlderThan = 72 * time.Hour
	}
	if !dryRun && olderThan > 0 && olderThan < time.Hour {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "older_than_hours must be at least 1 when dry_run is false"})
		return
	}
	if !dryRun {
		switch status {
		case core.JobStatusPending, core.JobStatusQueued, core.JobStatusRunning, core.JobStatusPausing:
			if effectiveOlderThan < 72*time.Hour {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "running iceberg jobs require older_than_hours >= 72 for non-dry-run orphan cleanup"})
				return
			}
		}
	}

	result, err := iceberg.CleanupOrphanFilesForJobConfig(r.Context(), job.Config.ID, job.Config, iceberg.OrphanCleanupOptions{
		DryRun:         dryRun,
		OlderThan:      olderThan,
		MaxConcurrency: req.MaxConcurrency,
		Tables:         req.Tables,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}
