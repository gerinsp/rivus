package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/model"
)

func TestLoadAuthConfigFromEnv(t *testing.T) {
	t.Setenv(EnvUILoginEnabled, "true")
	t.Setenv(EnvUILoginUsername, "admin")
	t.Setenv(EnvUILoginPassword, "secret")

	cfg, err := LoadAuthConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadAuthConfigFromEnv returned error: %v", err)
	}
	if !cfg.Enabled {
		t.Fatalf("expected auth to be enabled")
	}
	if cfg.Username != "admin" {
		t.Fatalf("unexpected username: %q", cfg.Username)
	}
	if cfg.Password != "secret" {
		t.Fatalf("unexpected password: %q", cfg.Password)
	}
}

func TestLoadAuthConfigFromEnvRequiresCredentials(t *testing.T) {
	t.Setenv(EnvUILoginEnabled, "true")
	t.Setenv(EnvUILoginUsername, "")
	t.Setenv(EnvUILoginPassword, "")

	if _, err := LoadAuthConfigFromEnv(); err == nil {
		t.Fatalf("expected credential validation error")
	}
}

func TestProtectedRoutesRequireLogin(t *testing.T) {
	srv := newTestServer(t, AuthConfig{
		Enabled:    true,
		Username:   "admin",
		Password:   "secret",
		CookieName: defaultAuthCookie,
	})
	router := srv.Router()

	rootReq := httptest.NewRequest(http.MethodGet, "/", nil)
	rootRec := httptest.NewRecorder()
	router.ServeHTTP(rootRec, rootReq)

	if rootRec.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect for unauthenticated root request, got %d", rootRec.Code)
	}
	if got := rootRec.Header().Get("Location"); got != "/login?next=%2F" {
		t.Fatalf("unexpected redirect location: %q", got)
	}

	apiReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	apiRec := httptest.NewRecorder()
	router.ServeHTTP(apiRec, apiReq)

	if apiRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized for /api/jobs, got %d", apiRec.Code)
	}

	loginReq := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{"username":"admin","password":"secret","next":"/"}`))
	loginReq.Header.Set("Content-Type", "application/json")
	loginRec := httptest.NewRecorder()
	router.ServeHTTP(loginRec, loginReq)

	if loginRec.Code != http.StatusOK {
		t.Fatalf("expected successful login, got %d", loginRec.Code)
	}

	loginRes := loginRec.Result()
	if len(loginRes.Cookies()) == 0 {
		t.Fatalf("expected session cookie after login")
	}
	sessionCookie := loginRes.Cookies()[0]

	authorizedReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	authorizedReq.AddCookie(sessionCookie)
	authorizedRec := httptest.NewRecorder()
	router.ServeHTTP(authorizedRec, authorizedReq)

	if authorizedRec.Code != http.StatusOK {
		t.Fatalf("expected authorized /api/jobs response, got %d", authorizedRec.Code)
	}

	logoutReq := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logoutReq.AddCookie(sessionCookie)
	logoutRec := httptest.NewRecorder()
	router.ServeHTTP(logoutRec, logoutReq)

	if logoutRec.Code != http.StatusOK {
		t.Fatalf("expected successful logout, got %d", logoutRec.Code)
	}

	postLogoutReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	postLogoutReq.AddCookie(sessionCookie)
	postLogoutRec := httptest.NewRecorder()
	router.ServeHTTP(postLogoutRec, postLogoutReq)

	if postLogoutRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized after logout, got %d", postLogoutRec.Code)
	}
}

func TestProtectedAPIAllowsServiceToken(t *testing.T) {
	srv := newTestServer(t, AuthConfig{
		Enabled:    true,
		Username:   "admin",
		Password:   "secret",
		CookieName: defaultAuthCookie,
		APIToken:   "maintenance-token",
	})

	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	req.Header.Set("X-Rivus-Token", "maintenance-token")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected service token to authorize API request, got %d", rec.Code)
	}
}

func TestProtectedAPIRejectsInvalidServiceToken(t *testing.T) {
	srv := newTestServer(t, AuthConfig{
		Enabled:    true,
		Username:   "admin",
		Password:   "secret",
		CookieName: defaultAuthCookie,
		APIToken:   "maintenance-token",
	})

	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected invalid service token to be rejected, got %d", rec.Code)
	}
}

func TestAuthSessionSurvivesServerRestart(t *testing.T) {
	auth := AuthConfig{
		Enabled:    true,
		Username:   "admin",
		Password:   "secret",
		CookieName: defaultAuthCookie,
	}
	srv := newTestServer(t, auth)
	router := srv.Router()

	loginReq := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{"username":"admin","password":"secret","next":"/"}`))
	loginReq.Header.Set("Content-Type", "application/json")
	loginRec := httptest.NewRecorder()
	router.ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("expected successful login, got %d", loginRec.Code)
	}
	sessionCookie := loginRec.Result().Cookies()[0]

	restarted := newTestServer(t, auth)
	restartedReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	restartedReq.AddCookie(sessionCookie)
	restartedRec := httptest.NewRecorder()
	restarted.Router().ServeHTTP(restartedRec, restartedReq)
	if restartedRec.Code != http.StatusOK {
		t.Fatalf("expected session to survive restart, got %d", restartedRec.Code)
	}
}

func TestRoutesStayOpenWhenLoginDisabled(t *testing.T) {
	srv := newTestServer(t, AuthConfig{
		Enabled:    false,
		CookieName: defaultAuthCookie,
	})
	router := srv.Router()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected open root page when auth disabled, got %d", rec.Code)
	}
}

func TestPublicBrandAssetsBypassLogin(t *testing.T) {
	srv := newTestServer(t, AuthConfig{
		Enabled:    true,
		Username:   "admin",
		Password:   "secret",
		CookieName: defaultAuthCookie,
	})
	router := srv.Router()

	for _, path := range []string{"/favicon.svg", "/rivus-logo.png", "/rivus-logo.png?v=2", "/api/version"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected %s to be public, got status %d location=%q", path, rec.Code, rec.Header().Get("Location"))
		}
	}
}

func TestSubmitImmediateStartFailureDoesNotCreateJob(t *testing.T) {
	srv := newTestServer(t, AuthConfig{
		Enabled:    false,
		CookieName: defaultAuthCookie,
	})
	router := srv.Router()

	body := strings.NewReader(`
id: bad-job
name: bad-job
source:
  type: missing_source
  config: {}
sink:
  type: doris
  config: {}
`)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", body)
	req.Header.Set("Content-Type", "application/x-yaml")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected failed job submit to return bad request, got %d body=%s", rec.Code, rec.Body.String())
	}

	var submitResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &submitResp); err != nil {
		t.Fatalf("decode submit response: %v", err)
	}
	if !strings.Contains(responseString(submitResp["error"]), "unknown source connector type") {
		t.Fatalf("submit error = %v, want unknown source connector type", submitResp["error"])
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected jobs list status OK, got %d", listRec.Code)
	}
	if strings.Contains(listRec.Body.String(), `"id":"bad-job"`) {
		t.Fatalf("jobs list includes failed submit job: %s", listRec.Body.String())
	}
}

func TestSubmitDuplicateJobIDReturnsConflict(t *testing.T) {
	reg := connector.NewRegistry()
	reg.RegisterSource("blocking_source", func(connector.JobContext, any) (connector.Source, error) {
		return apiSourceFunc(func(ctx context.Context, _ chan<- model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})
	reg.RegisterSink("blocking_sink", func(connector.JobContext, any) (connector.Sink, error) {
		return apiSinkFunc(func(ctx context.Context, _ <-chan model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})

	srv := newTestServer(t, AuthConfig{Enabled: false, CookieName: defaultAuthCookie})
	srv.jobManager = core.NewJobManager(reg)
	router := srv.Router()
	body := `
id: duplicate-job
name: duplicate-job
mode: initial
source:
  type: blocking_source
  config: {}
sink:
  type: blocking_sink
  config: {}
`

	first := httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(body))
	first.Header.Set("Content-Type", "application/x-yaml")
	firstRec := httptest.NewRecorder()
	router.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusCreated {
		t.Fatalf("first submit status = %d, want %d: %s", firstRec.Code, http.StatusCreated, firstRec.Body.String())
	}

	second := httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(body))
	second.Header.Set("Content-Type", "application/x-yaml")
	secondRec := httptest.NewRecorder()
	router.ServeHTTP(secondRec, second)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("duplicate submit status = %d, want %d: %s", secondRec.Code, http.StatusConflict, secondRec.Body.String())
	}

	var response map[string]string
	if err := json.Unmarshal(secondRec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode duplicate response: %v", err)
	}
	if response["error"] != `job id "duplicate-job" already exists` {
		t.Fatalf("duplicate error = %q, want duplicate ID error", response["error"])
	}
}

func newTestServer(t *testing.T, auth AuthConfig) *Server {
	t.Helper()

	uiDir := t.TempDir()
	writeTestFile(t, filepath.Join(uiDir, "index.html"), "<html><body>dashboard</body></html>")
	writeTestFile(t, filepath.Join(uiDir, "login.html"), "<html><body>login</body></html>")
	writeTestFile(t, filepath.Join(uiDir, "favicon.svg"), "<svg></svg>")
	writeTestFile(t, filepath.Join(uiDir, "rivus-logo.png"), "logo")

	return &Server{
		jobManager:   core.NewJobManager(connector.NewRegistry()),
		uiDir:        uiDir,
		auth:         auth,
		authSessions: newAuthSessionStore(auth),
	}
}

func responseString(value any) string {
	if value == nil {
		return ""
	}
	out, _ := value.(string)
	return out
}

func writeTestFile(t *testing.T, path, body string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write test file %s: %v", path, err)
	}
}

type apiSourceFunc func(context.Context, chan<- model.Event) error

func (f apiSourceFunc) Run(ctx context.Context, out chan<- model.Event) error {
	return f(ctx, out)
}

type apiSinkFunc func(context.Context, <-chan model.Event) error

func (f apiSinkFunc) Run(ctx context.Context, in <-chan model.Event) error {
	return f(ctx, in)
}
