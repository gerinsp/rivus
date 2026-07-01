package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultLogPrefix         = "rivus"
	defaultLogRetentionDays  = 7
	defaultLogMaxSizeMB      = 64
	defaultLogMaxTotalSizeMB = 20480
)

type logConfig struct {
	enabled        bool
	dir            string
	prefix         string
	retentionDays  int
	maxSizeMB      int
	maxTotalSizeMB int
	stderrEnabled  bool
}

type rotatingLogWriter struct {
	mu sync.Mutex

	dir              string
	prefix           string
	retentionDays    int
	maxSizeBytes     int64
	maxTotalBytes    int64
	currentDate      string
	currentSeq       int
	lastCleanupDate  string
	currentSizeBytes int64
	file             *os.File
}

type logFileCandidate struct {
	name    string
	path    string
	size    int64
	modTime time.Time
	current bool
}

func logConfigFromEnv() logConfig {
	dir := strings.TrimSpace(os.Getenv("RIVUS_LOG_DIR"))
	prefix := strings.TrimSpace(os.Getenv("RIVUS_LOG_PREFIX"))
	if prefix == "" {
		prefix = defaultLogPrefix
	}

	if legacyPath := strings.TrimSpace(os.Getenv("RIVUS_LOG_FILE")); legacyPath != "" {
		if dir == "" {
			dir = filepath.Dir(legacyPath)
		}
		base := strings.TrimSuffix(filepath.Base(legacyPath), filepath.Ext(legacyPath))
		if base != "" && strings.TrimSpace(os.Getenv("RIVUS_LOG_PREFIX")) == "" {
			prefix = base
		}
	}

	retentionDays := envInt("RIVUS_LOG_RETENTION_DAYS", defaultLogRetentionDays)
	maxSizeMB := envInt("RIVUS_LOG_MAX_SIZE_MB", defaultLogMaxSizeMB)
	maxTotalSizeMB := envInt("RIVUS_LOG_MAX_TOTAL_SIZE_MB", defaultLogMaxTotalSizeMB)
	stderrEnabled := envBool("RIVUS_LOG_STDERR", true)

	return logConfig{
		enabled:        dir != "",
		dir:            dir,
		prefix:         safeLogPrefix(prefix),
		retentionDays:  retentionDays,
		maxSizeMB:      maxSizeMB,
		maxTotalSizeMB: maxTotalSizeMB,
		stderrEnabled:  stderrEnabled,
	}
}

func newRotatingLogWriter(cfg logConfig) (*rotatingLogWriter, error) {
	if err := os.MkdirAll(cfg.dir, 0o755); err != nil {
		return nil, err
	}

	w := &rotatingLogWriter{
		dir:           cfg.dir,
		prefix:        cfg.prefix,
		retentionDays: cfg.retentionDays,
		maxSizeBytes:  int64(cfg.maxSizeMB) * 1024 * 1024,
		maxTotalBytes: int64(cfg.maxTotalSizeMB) * 1024 * 1024,
	}
	if err := w.rotateLocked(time.Now()); err != nil {
		return nil, err
	}
	w.cleanupLocked(time.Now())
	return w, nil
}

func (w *rotatingLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	date := now.Format("2006-01-02")
	if w.file == nil || w.currentDate != date || w.exceedsMaxSize(len(p)) {
		if w.currentDate == date && w.file != nil {
			w.currentSeq++
		} else {
			w.currentSeq = 0
		}
		if err := w.rotateLocked(now); err != nil {
			return 0, err
		}
		w.cleanupLocked(now)
	}

	if w.lastCleanupDate != date {
		w.cleanupLocked(now)
	}

	n, err := w.file.Write(p)
	w.currentSizeBytes += int64(n)
	return n, err
}

func (w *rotatingLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *rotatingLogWriter) exceedsMaxSize(incoming int) bool {
	return w.maxSizeBytes > 0 && w.currentSizeBytes > 0 && w.currentSizeBytes+int64(incoming) > w.maxSizeBytes
}

func (w *rotatingLogWriter) rotateLocked(now time.Time) error {
	date := now.Format("2006-01-02")
	seq, path, size := w.chooseLogPath(date)

	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	w.file = f
	w.currentDate = date
	w.currentSeq = seq
	w.currentSizeBytes = size
	return nil
}

func (w *rotatingLogWriter) chooseLogPath(date string) (int, string, int64) {
	seq := w.currentSeq
	for {
		path := w.logPath(date, seq)
		info, err := os.Stat(path)
		if err != nil {
			return seq, path, 0
		}
		size := info.Size()
		if w.maxSizeBytes <= 0 || size < w.maxSizeBytes {
			return seq, path, size
		}
		seq++
	}
}

func (w *rotatingLogWriter) logPath(date string, seq int) string {
	name := fmt.Sprintf("%s-%s.log", w.prefix, date)
	if seq > 0 {
		name = fmt.Sprintf("%s-%s-%03d.log", w.prefix, date, seq)
	}
	return filepath.Join(w.dir, name)
}

func (w *rotatingLogWriter) cleanupLocked(now time.Time) {
	w.lastCleanupDate = now.Format("2006-01-02")
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}

	currentPath := ""
	if w.file != nil {
		currentPath, _ = filepath.Abs(w.file.Name())
	}

	var cutoff time.Time
	if w.retentionDays > 0 {
		cutoff = now.AddDate(0, 0, -w.retentionDays)
	}

	candidates := make([]logFileCandidate, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !w.matchesLogFile(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(w.dir, entry.Name())
		absPath, _ := filepath.Abs(path)
		isCurrent := currentPath != "" && absPath == currentPath
		if w.retentionDays > 0 && info.ModTime().Before(cutoff) && !isCurrent {
			_ = os.Remove(path)
			continue
		}
		candidates = append(candidates, logFileCandidate{
			name:    entry.Name(),
			path:    path,
			size:    info.Size(),
			modTime: info.ModTime(),
			current: isCurrent,
		})
	}

	w.cleanupByTotalSizeLocked(candidates)
}

func (w *rotatingLogWriter) cleanupByTotalSizeLocked(candidates []logFileCandidate) {
	if w.maxTotalBytes <= 0 {
		return
	}

	var total int64
	for _, candidate := range candidates {
		total += candidate.size
	}
	if total <= w.maxTotalBytes {
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].name < candidates[j].name
		}
		return candidates[i].modTime.Before(candidates[j].modTime)
	})

	for _, candidate := range candidates {
		if total <= w.maxTotalBytes {
			return
		}
		if candidate.current {
			continue
		}
		if err := os.Remove(candidate.path); err == nil {
			total -= candidate.size
		}
	}
}

func (w *rotatingLogWriter) matchesLogFile(name string) bool {
	return strings.HasPrefix(name, w.prefix+"-") && strings.HasSuffix(name, ".log")
}

func envInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func envBool(name string, fallback bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func safeLogPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return defaultLogPrefix
	}

	var b strings.Builder
	for _, r := range prefix {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "._-")
	if out == "" {
		return defaultLogPrefix
	}
	return out
}
