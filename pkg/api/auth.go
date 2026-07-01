package api

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	EnvUILoginEnabled  = "RIVUS_UI_LOGIN_ENABLED"
	EnvUILoginUsername = "RIVUS_UI_LOGIN_USERNAME"
	EnvUILoginPassword = "RIVUS_UI_LOGIN_PASSWORD"
	EnvUISessionSecret = "RIVUS_UI_SESSION_SECRET"
	EnvAPIToken        = "RIVUS_API_TOKEN"
	defaultAuthCookie  = "rivus_session"
	defaultSessionTTL  = 7 * 24 * time.Hour
)

type AuthConfig struct {
	Enabled    bool
	Username   string
	Password   string
	CookieName string
	Secret     string
	APIToken   string
}

type authLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Next     string `json:"next"`
}

type authSessionStore struct {
	mu      sync.RWMutex
	secret  []byte
	revoked map[string]time.Time
}

func LoadAuthConfigFromEnv() (AuthConfig, error) {
	cfg := AuthConfig{
		Enabled:    parseAuthEnvBool(os.Getenv(EnvUILoginEnabled)),
		Username:   os.Getenv(EnvUILoginUsername),
		Password:   os.Getenv(EnvUILoginPassword),
		CookieName: defaultAuthCookie,
		Secret:     os.Getenv(EnvUISessionSecret),
		APIToken:   strings.TrimSpace(os.Getenv(EnvAPIToken)),
	}

	if !cfg.Enabled {
		return cfg, nil
	}

	if strings.TrimSpace(cfg.Username) == "" {
		return AuthConfig{}, fmt.Errorf("%s is required when %s=true", EnvUILoginUsername, EnvUILoginEnabled)
	}
	if strings.TrimSpace(cfg.Password) == "" {
		return AuthConfig{}, fmt.Errorf("%s is required when %s=true", EnvUILoginPassword, EnvUILoginEnabled)
	}

	return cfg, nil
}

func newAuthSessionStore(auth AuthConfig) *authSessionStore {
	secret := strings.TrimSpace(auth.Secret)
	if secret == "" {
		secret = auth.Username + "\x00" + auth.Password
	}
	return &authSessionStore{
		secret:  []byte(secret),
		revoked: make(map[string]time.Time),
	}
}

func (s *authSessionStore) Create() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	nonce := hex.EncodeToString(buf)
	expires := time.Now().Add(defaultSessionTTL).Unix()
	message := fmt.Sprintf("v1.%d.%s", expires, nonce)
	return message + "." + s.sign(message), nil
}

func (s *authSessionStore) Delete(token string) {
	if token == "" {
		return
	}

	s.mu.Lock()
	s.revoked[token] = time.Now()
	s.mu.Unlock()
}

func (s *authSessionStore) Exists(token string) bool {
	if token == "" {
		return false
	}

	s.mu.RLock()
	_, revoked := s.revoked[token]
	s.mu.RUnlock()
	if revoked {
		return false
	}

	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != "v1" {
		return false
	}
	expires, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || time.Now().Unix() > expires {
		return false
	}

	message := strings.Join(parts[:3], ".")
	want := s.sign(message)
	ok := hmac.Equal([]byte(parts[3]), []byte(want))

	return ok
}

func (s *authSessionStore) sign(message string) string {
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

func parseAuthEnvBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on", "enable", "enabled":
		return true
	default:
		return false
	}
}

func sanitizeAuthRedirectPath(next string) string {
	next = strings.TrimSpace(next)
	if next == "" {
		return "/"
	}
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		return "/"
	}
	return next
}

func (s *Server) credentialsMatch(username, password string) bool {
	usernameOK := subtle.ConstantTimeCompare([]byte(username), []byte(s.auth.Username)) == 1
	passwordOK := subtle.ConstantTimeCompare([]byte(password), []byte(s.auth.Password)) == 1
	return usernameOK && passwordOK
}

func (s *Server) currentSessionToken(r *http.Request) string {
	if !s.auth.Enabled {
		return ""
	}

	cookie, err := r.Cookie(s.auth.CookieName)
	if err != nil {
		return ""
	}
	return cookie.Value
}

func (s *Server) isAuthenticated(r *http.Request) bool {
	if !s.auth.Enabled {
		return true
	}
	return s.authSessions.Exists(s.currentSessionToken(r))
}

func (s *Server) isServiceAuthenticated(r *http.Request) bool {
	expected := strings.TrimSpace(s.auth.APIToken)
	if expected == "" {
		return false
	}

	provided := strings.TrimSpace(r.Header.Get("X-Rivus-Token"))
	if provided == "" {
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(authorization) > len("Bearer ") && strings.EqualFold(authorization[:len("Bearer ")], "Bearer ") {
			provided = strings.TrimSpace(authorization[len("Bearer "):])
		}
	}
	if provided == "" {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
}

func (s *Server) setAuthCookie(w http.ResponseWriter, token string, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     s.auth.CookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
	})
}

func (s *Server) clearAuthCookie(w http.ResponseWriter, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     s.auth.CookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
}

func (s *Server) requireAPIAuth(next http.HandlerFunc) http.HandlerFunc {
	if !s.auth.Enabled {
		return next
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if !s.isAuthenticated(r) && !s.isServiceAuthenticated(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func (s *Server) requirePageAuth(next http.Handler) http.Handler {
	if !s.auth.Enabled {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.isAuthenticated(r) {
			next.ServeHTTP(w, r)
			return
		}

		target := sanitizeAuthRedirectPath(r.URL.RequestURI())
		http.Redirect(w, r, "/login?next="+url.QueryEscape(target), http.StatusSeeOther)
	})
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username := ""
	if s.isAuthenticated(r) {
		username = s.auth.Username
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"enabled":       s.auth.Enabled,
		"authenticated": s.isAuthenticated(r),
		"username":      username,
	})
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !s.auth.Enabled {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	if s.isAuthenticated(r) {
		http.Redirect(w, r, sanitizeAuthRedirectPath(r.URL.Query().Get("next")), http.StatusSeeOther)
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, filepath.Join(s.uiDir, "login.html"))
}

func (s *Server) handleFavicon(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "public, max-age=3600")
	http.ServeFile(w, r, filepath.Join(s.uiDir, "favicon.svg"))
}

func (s *Server) handleLogo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "public, max-age=3600")
	http.ServeFile(w, r, filepath.Join(s.uiDir, "rivus-logo.png"))
}

func decodeAuthLoginRequest(r *http.Request) (authLoginRequest, error) {
	var payload authLoginRequest

	if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			return payload, err
		}
		if len(strings.TrimSpace(string(body))) == 0 {
			return payload, nil
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return payload, err
		}
		return payload, nil
	}

	if err := r.ParseForm(); err != nil {
		return payload, err
	}

	payload.Username = r.Form.Get("username")
	payload.Password = r.Form.Get("password")
	payload.Next = r.Form.Get("next")

	return payload, nil
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !s.auth.Enabled {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "login is disabled"})
		return
	}

	payload, err := decodeAuthLoginRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if !s.credentialsMatch(payload.Username, payload.Password) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "username atau password salah"})
		return
	}

	token, err := s.authSessions.Create()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.setAuthCookie(w, token, r.TLS != nil)
	writeJSON(w, http.StatusOK, map[string]string{
		"next":     sanitizeAuthRedirectPath(payload.Next),
		"username": s.auth.Username,
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.auth.Enabled {
		s.authSessions.Delete(s.currentSessionToken(r))
	}
	s.clearAuthCookie(w, r.TLS != nil)

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
