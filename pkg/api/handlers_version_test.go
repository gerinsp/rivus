package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

type staticRuntimeInstanceReader struct {
	instances []meta.RuntimeInstance
	err       error
}

func (s staticRuntimeInstanceReader) ListRuntimeInstances(context.Context) ([]meta.RuntimeInstance, error) {
	return s.instances, s.err
}

func TestBuildRuntimeVersionsResponseReportsOnlineAndMissingContainers(t *testing.T) {
	now := time.Date(2026, 8, 20, 16, 0, 0, 0, time.UTC)
	response := buildRuntimeVersionsResponse([]meta.RuntimeInstance{
		{
			Role:        "master",
			InstanceID:  "master-1",
			Version:     "main",
			ImageTag:    "sha-abc123",
			Commit:      "abc123456789",
			StartedAt:   now.Add(-time.Hour),
			HeartbeatAt: now.Add(-15 * time.Second),
		},
		{
			Role:        "streaming",
			InstanceID:  "stream-1",
			Version:     "main",
			ImageTag:    "sha-old",
			Commit:      "oldcommit",
			StartedAt:   now.Add(-2 * time.Hour),
			HeartbeatAt: now.Add(-2 * time.Minute),
		},
	}, now)

	if len(response.Containers) != 4 {
		t.Fatalf("containers = %d, want 4", len(response.Containers))
	}
	if got := response.Containers[0]; got.Container != "rivus" || got.Status != "online" || got.ImageTag != "sha-abc123" {
		t.Fatalf("master item = %+v", got)
	}
	if got := response.Containers[1]; got.Container != "rivus-streaming" || got.Status != "offline" || got.StatusDetail != "heartbeat is stale" {
		t.Fatalf("streaming item = %+v", got)
	}
	if got := response.Containers[2]; got.Container != "rivus-snapshot" || got.StatusDetail != "no heartbeat received" {
		t.Fatalf("snapshot item = %+v", got)
	}
}

func TestHandleRuntimeVersionsReturnsRegistryState(t *testing.T) {
	now := time.Now().UTC()
	server := &Server{runtimeStore: staticRuntimeInstanceReader{instances: []meta.RuntimeInstance{
		{
			Role:        "maintenance",
			InstanceID:  "maintenance-1",
			Version:     "main",
			ImageTag:    "sha-abc123",
			Commit:      "abc123456789",
			StartedAt:   now.Add(-time.Minute),
			HeartbeatAt: now,
		},
	}}}
	req := httptest.NewRequest(http.MethodGet, "/api/runtime/versions", nil)
	recorder := httptest.NewRecorder()

	server.handleRuntimeVersions(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response runtimeVersionsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Containers) != 4 || response.Containers[3].Status != "online" {
		t.Fatalf("unexpected response: %+v", response)
	}
}
