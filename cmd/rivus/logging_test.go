package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRotatingLogWriterDeletesOldLogsByRetention(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "rivus-2000-01-01.log")
	writeLogFile(t, oldPath, 1024)
	oldTime := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	w, err := newRotatingLogWriter(logConfig{
		enabled:        true,
		dir:            dir,
		prefix:         "rivus",
		retentionDays:  1,
		maxSizeMB:      1,
		maxTotalSizeMB: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expected old log to be deleted, stat err=%v", err)
	}
}

func TestRotatingLogWriterCapsTotalLogSize(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	today := now.Format("2006-01-02")
	currentPath := filepath.Join(dir, fmt.Sprintf("rivus-%s.log", today))
	oldPathA := filepath.Join(dir, "rivus-2000-01-01.log")
	oldPathB := filepath.Join(dir, "rivus-2000-01-02.log")

	for _, path := range []string{currentPath, oldPathA, oldPathB} {
		writeLogFile(t, path, 700*1024)
	}
	setMTime(t, oldPathA, now.Add(-3*time.Hour))
	setMTime(t, oldPathB, now.Add(-2*time.Hour))
	setMTime(t, currentPath, now.Add(-time.Hour))

	w, err := newRotatingLogWriter(logConfig{
		enabled:        true,
		dir:            dir,
		prefix:         "rivus",
		retentionDays:  0,
		maxSizeMB:      10,
		maxTotalSizeMB: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for _, path := range []string{oldPathA, oldPathB} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be deleted by total size cap, stat err=%v", path, err)
		}
	}
	if _, err := os.Stat(currentPath); err != nil {
		t.Fatalf("expected current log to be kept: %v", err)
	}
}

func TestEnvBool(t *testing.T) {
	t.Setenv("RIVUS_TEST_BOOL", "false")
	if got := envBool("RIVUS_TEST_BOOL", true); got {
		t.Fatalf("envBool false = true")
	}

	t.Setenv("RIVUS_TEST_BOOL", "on")
	if got := envBool("RIVUS_TEST_BOOL", false); !got {
		t.Fatalf("envBool on = false")
	}

	t.Setenv("RIVUS_TEST_BOOL", "unexpected")
	if got := envBool("RIVUS_TEST_BOOL", true); !got {
		t.Fatalf("envBool unexpected did not use fallback")
	}
}

func writeLogFile(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), size), 0o644); err != nil {
		t.Fatal(err)
	}
}

func setMTime(t *testing.T, path string, ts time.Time) {
	t.Helper()
	if err := os.Chtimes(path, ts, ts); err != nil {
		t.Fatal(err)
	}
}
