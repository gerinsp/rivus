package api

import (
	"net/http"

	"github.com/gerinsp/rivus/pkg/core"
)

func (s *Server) handleGetJobGraph(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	_ = s.jobManager.RefreshPersistedViews(r.Context())

	job, err := s.jobManager.Get(jobID)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	g := job.Graph()
	if g == nil {
		// fallback minimal
		writeJSON(w, 200, &core.JobGraph{
			JobID:    jobID,
			Status:   job.StatusValue(),
			Progress: job.Progress(),
			Nodes:    []core.GraphNode{},
			Edges:    []core.GraphEdge{},
		})
		return
	}

	writeJSON(w, 200, g)
}
