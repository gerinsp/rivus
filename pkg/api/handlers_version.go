package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/version"
)

const runtimeHeartbeatStaleAfter = 90 * time.Second

var expectedRuntimeRoles = []struct {
	Role      string
	Container string
}{
	{Role: "master", Container: "rivus"},
	{Role: "streaming", Container: "rivus-streaming"},
	{Role: "snapshot", Container: "rivus-snapshot"},
	{Role: "maintenance", Container: "rivus-maintenance"},
}

type runtimeInstanceReader interface {
	ListRuntimeInstances(ctx context.Context) ([]meta.RuntimeInstance, error)
}

type runtimeVersionItem struct {
	Container     string     `json:"container"`
	Role          string     `json:"role"`
	InstanceID    string     `json:"instance_id,omitempty"`
	Status        string     `json:"status"`
	Version       string     `json:"version,omitempty"`
	ImageTag      string     `json:"image_tag,omitempty"`
	Commit        string     `json:"commit,omitempty"`
	BuildDate     string     `json:"build_date,omitempty"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	LastSeenAt    *time.Time `json:"last_seen_at,omitempty"`
	AgeSeconds    int64      `json:"age_seconds,omitempty"`
	UptimeSeconds int64      `json:"uptime_seconds,omitempty"`
	StatusDetail  string     `json:"status_detail,omitempty"`
}

type runtimeVersionsResponse struct {
	CheckedAt         time.Time            `json:"checked_at"`
	StaleAfterSeconds int64                `json:"stale_after_seconds"`
	Containers        []runtimeVersionItem `json:"containers"`
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, version.Current())
}

func (s *Server) handleRuntimeVersions(w http.ResponseWriter, r *http.Request) {
	store, err := s.runtimeStoreForView(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	instances, err := store.ListRuntimeInstances(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("load runtime versions: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, buildRuntimeVersionsResponse(instances, time.Now().UTC()))
}

func (s *Server) runtimeStoreForView(ctx context.Context) (runtimeInstanceReader, error) {
	s.runtimeStoreOnce.Do(func() {
		if s.runtimeStore != nil {
			return
		}
		dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
		if dsn == "" {
			s.runtimeStoreErr = fmt.Errorf("runtime version registry is not configured")
			return
		}
		store, err := meta.NewRuntimeInstanceStore(dsn)
		if err != nil {
			s.runtimeStoreErr = err
			return
		}
		base := context.Background()
		if ctx != nil {
			base = context.WithoutCancel(ctx)
		}
		initCtx, cancel := context.WithTimeout(base, 3*time.Second)
		defer cancel()
		if err := store.Init(initCtx); err != nil {
			_ = store.Close()
			s.runtimeStoreErr = err
			return
		}
		s.runtimeStore = store
	})
	if s.runtimeStoreErr != nil {
		return nil, s.runtimeStoreErr
	}
	if s.runtimeStore == nil {
		return nil, fmt.Errorf("runtime version registry is unavailable")
	}
	return s.runtimeStore, nil
}

func buildRuntimeVersionsResponse(instances []meta.RuntimeInstance, now time.Time) runtimeVersionsResponse {
	now = now.UTC()
	byRole := make(map[string][]meta.RuntimeInstance)
	for _, instance := range instances {
		role := strings.ToLower(strings.TrimSpace(instance.Role))
		if role == "" {
			continue
		}
		instance.Role = role
		byRole[role] = append(byRole[role], instance)
	}
	for role := range byRole {
		sort.SliceStable(byRole[role], func(i, j int) bool {
			return byRole[role][i].HeartbeatAt.After(byRole[role][j].HeartbeatAt)
		})
	}

	items := make([]runtimeVersionItem, 0, len(expectedRuntimeRoles))
	seen := make(map[string]bool, len(expectedRuntimeRoles))
	for _, expected := range expectedRuntimeRoles {
		seen[expected.Role] = true
		roleInstances := byRole[expected.Role]
		addedOnline := false
		for _, instance := range roleInstances {
			if runtimeInstanceOnline(instance, now) {
				items = append(items, runtimeVersionItemFromInstance(expected.Container, instance, now, true))
				addedOnline = true
			}
		}
		if addedOnline {
			continue
		}
		if len(roleInstances) > 0 {
			items = append(items, runtimeVersionItemFromInstance(expected.Container, roleInstances[0], now, false))
			continue
		}
		items = append(items, runtimeVersionItem{
			Container:    expected.Container,
			Role:         expected.Role,
			Status:       "offline",
			StatusDetail: "no heartbeat received",
		})
	}

	unknownRoles := make([]string, 0)
	for role := range byRole {
		if !seen[role] {
			unknownRoles = append(unknownRoles, role)
		}
	}
	sort.Strings(unknownRoles)
	for _, role := range unknownRoles {
		for _, instance := range byRole[role] {
			items = append(items, runtimeVersionItemFromInstance("rivus-"+role, instance, now, runtimeInstanceOnline(instance, now)))
		}
	}

	return runtimeVersionsResponse{
		CheckedAt:         now,
		StaleAfterSeconds: int64(runtimeHeartbeatStaleAfter / time.Second),
		Containers:        items,
	}
}

func runtimeInstanceOnline(instance meta.RuntimeInstance, now time.Time) bool {
	if instance.HeartbeatAt.IsZero() {
		return false
	}
	age := now.Sub(instance.HeartbeatAt.UTC())
	return age <= runtimeHeartbeatStaleAfter
}

func runtimeVersionItemFromInstance(container string, instance meta.RuntimeInstance, now time.Time, online bool) runtimeVersionItem {
	age := now.Sub(instance.HeartbeatAt.UTC())
	if age < 0 {
		age = 0
	}
	status := "offline"
	detail := "heartbeat is stale"
	if online {
		status = "online"
		detail = "heartbeat is current"
	}
	startedAt := instance.StartedAt.UTC()
	lastSeenAt := instance.HeartbeatAt.UTC()
	uptime := now.Sub(startedAt)
	if uptime < 0 {
		uptime = 0
	}
	return runtimeVersionItem{
		Container:     container,
		Role:          instance.Role,
		InstanceID:    instance.InstanceID,
		Status:        status,
		Version:       instance.Version,
		ImageTag:      instance.ImageTag,
		Commit:        instance.Commit,
		BuildDate:     instance.BuildDate,
		StartedAt:     &startedAt,
		LastSeenAt:    &lastSeenAt,
		AgeSeconds:    int64(age / time.Second),
		UptimeSeconds: int64(uptime / time.Second),
		StatusDetail:  detail,
	}
}
