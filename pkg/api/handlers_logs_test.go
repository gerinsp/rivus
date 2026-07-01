package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTailLogLinesReturnsLastLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rivus-2026-05-22.log")
	content := "line-001\nline-002\nline-003\nline-004\nline-005\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	lines, truncated, err := tailLogLines(path, 3, maxLogTailBytes)
	if err != nil {
		t.Fatalf("tailLogLines returned error: %v", err)
	}

	want := []string{"line-003", "line-004", "line-005"}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("unexpected lines:\nwant %#v\ngot  %#v", want, lines)
	}
	if !truncated {
		t.Fatalf("expected truncated tail")
	}
}

func TestHandleLogTailRejectsPathTraversal(t *testing.T) {
	t.Setenv(envLogDir, t.TempDir())
	req := httptest.NewRequest(http.MethodGet, "/api/logs/tail?file=../rivus-2026-05-22.log", nil)
	rec := httptest.NewRecorder()

	(&Server{}).handleLogTail(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d", rec.Code)
	}
}

func TestHandleLogTailReturnsJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envLogDir, dir)
	path := filepath.Join(dir, "rivus-2026-05-22.log")
	if err := os.WriteFile(path, []byte("a\nb\nc\n"), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/logs/tail?file=rivus-2026-05-22.log&lines=2", nil)
	rec := httptest.NewRecorder()

	(&Server{}).handleLogTail(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", rec.Code, rec.Body.String())
	}

	var payload logTailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := []string{"b", "c"}
	if !reflect.DeepEqual(payload.Lines, want) {
		t.Fatalf("unexpected lines:\nwant %#v\ngot  %#v", want, payload.Lines)
	}
	if payload.File != "rivus-2026-05-22.log" {
		t.Fatalf("unexpected file: %q", payload.File)
	}
}

func TestHandleLogTailSearchesMatchingLinesAcrossRotatedFiles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envLogDir, dir)
	files := map[string]string{
		"rivus-2026-05-22.log":   "newest unrelated\n",
		"rivus-2026-05-22-1.log": "old unrelated\n2026/05/22 [iceberg][job cluster-iceberg-kurir] committed offset\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write log %s: %v", name, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/logs/tail?filter=cluster-iceberg-kurir&lines=20", nil)
	rec := httptest.NewRecorder()

	(&Server{}).handleLogTail(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", rec.Code, rec.Body.String())
	}

	var payload logTailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := []string{"2026/05/22 [iceberg][job cluster-iceberg-kurir] committed offset"}
	if !reflect.DeepEqual(payload.Lines, want) {
		t.Fatalf("unexpected lines:\nwant %#v\ngot  %#v", want, payload.Lines)
	}
	if !reflect.DeepEqual(payload.Files, []string{"rivus-2026-05-22-1.log"}) {
		t.Fatalf("unexpected files: %#v", payload.Files)
	}
}
