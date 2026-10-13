package api

import (
	"context"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/core"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJobNameEndpointDoesNotRestartPipeline(t *testing.T) {
	m := core.NewJobManager(connector.NewRegistry(), core.WithControlPlaneRole())
	defer m.Shutdown(context.Background())
	cfg := &config.JobConfig{ID: "rename-api", Name: "Old", Mode: config.JobModeInitial}
	job, err := m.Submit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sid := job.SubmissionID()
	status := job.GetStatus()
	s := &Server{jobManager: m}
	for _, tc := range []struct {
		path, body string
		want       int
	}{{"rename-api", `{"name":"Sync (btm)"}`, 200}, {"rename-api", `{"name":" "}`, 400}, {"rename-api", `{"name":"wrong","mode":"initial"}`, 400}, {"missing", `{"name":"new"}`, 404}} {
		req := httptest.NewRequest("PATCH", "/api/jobs/"+tc.path+"/name", strings.NewReader(tc.body))
		w := httptest.NewRecorder()
		s.handleJobByID(w, req)
		if w.Code != tc.want {
			t.Fatalf("status %d want %d: %s", w.Code, tc.want, w.Body.String())
		}
	}
	if job.Name() != "Sync (btm)" || job.GetStatus() != status || job.SubmissionID() != sid {
		t.Fatal("rename changed runtime")
	}
}
