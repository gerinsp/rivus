package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHandleSplitLogsListsRoleDirectories(t *testing.T) {
	root := t.TempDir()
	t.Setenv(envLogRoot, root)
	t.Setenv(envLogPrefix, "rivus")

	files := map[string]string{
		filepath.Join(root, "master", "rivus-2026-08-19.log"):                    "master line\n",
		filepath.Join(root, "streaming", "rivus-streaming-2026-08-19.log"):       "stream line\n",
		filepath.Join(root, "snapshot", "rivus-snapshot-2026-08-19.log"):          "snapshot line\n",
		filepath.Join(root, "maintenance", "rivus-maintenance-2026-08-19.log"):    "maintenance line\n",
	}
	for path, content := range files {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/logs", nil)
	rec := httptest.NewRecorder()
	(&Server{}).handleSplitLogs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", rec.Code, rec.Body.String())
	}

	var got []logFileInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	seen := map[string]bool{}
	for _, file := range got {
		seen[file.Name] = true
	}
	for _, name := range []string{
		"master/rivus-2026-08-19.log",
		"streaming/rivus-streaming-2026-08-19.log",
		"snapshot/rivus-snapshot-2026-08-19.log",
		"maintenance/rivus-maintenance-2026-08-19.log",
	} {
		if !seen[name] {
			t.Fatalf("missing split log file %q in %#v", name, seen)
		}
	}
}

func TestHandleSplitLogTailReadsStreamingFile(t *testing.T) {
	root := t.TempDir()
	t.Setenv(envLogRoot, root)
	t.Setenv(envLogPrefix, "rivus")

	dir := filepath.Join(root, "streaming")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	name := "rivus-streaming-2026-08-19.log"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("one\ntwo\nthree\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/logs/tail?file=streaming/"+name+"&lines=2", nil)
	rec := httptest.NewRecorder()
	(&Server{}).handleSplitLogTail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", rec.Code, rec.Body.String())
	}

	var got logTailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.File != "streaming/"+name {
		t.Fatalf("unexpected file: %q", got.File)
	}
	if len(got.Lines) != 2 || got.Lines[0] != "two" || got.Lines[1] != "three" {
		t.Fatalf("unexpected lines: %#v", got.Lines)
	}
}

func TestResolveSplitLogPathRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	t.Setenv(envLogRoot, root)
	for _, name := range []string{"../secret.log", "streaming/../secret.log", "streaming/subdir/rivus-streaming-2026-08-19.log"} {
		if _, err := resolveSplitLogPath(root, name); err == nil {
			t.Fatalf("expected %q to be rejected", name)
		}
	}
}
