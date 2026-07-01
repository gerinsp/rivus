package api

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	envLogDir           = "RIVUS_LOG_DIR"
	envLogFile          = "RIVUS_LOG_FILE"
	envLogPrefix        = "RIVUS_LOG_PREFIX"
	logDateOnly         = "2006-01-02"
	defaultLogTailLines = 500
	maxLogTailLines     = 5000
	maxLogTailBytes     = 8 * 1024 * 1024
	maxLogSearchBytes   = 64 * 1024 * 1024
	maxLogSearchFiles   = 12
	maxLogFilterLength  = 512
)

type logFileInfo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
	modUnix int64
}

type logTailResponse struct {
	File      string   `json:"file"`
	Files     []string `json:"files,omitempty"`
	Lines     []string `json:"lines"`
	LineCount int      `json:"line_count"`
	TotalSize int64    `json:"total_size"`
	ModTime   string   `json:"mod_time"`
	Truncated bool     `json:"truncated"`
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	logDir := apiLogDir()
	if logDir == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "RIVUS_LOG_DIR is not configured"})
		return
	}

	files, err := listLogFiles(logDir, apiLogPrefix())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, files)
}

func (s *Server) handleLogTail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	logDir := apiLogDir()
	if logDir == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "RIVUS_LOG_DIR is not configured"})
		return
	}

	prefix := apiLogPrefix()
	name := strings.TrimSpace(r.URL.Query().Get("file"))
	filter := normalizeLogFilter(r.URL.Query().Get("filter"))
	limit := parseLogTailLineLimit(r.URL.Query().Get("lines"))

	if name == "" && filter != "" {
		s.handleFilteredLogTail(w, logDir, prefix, filter, limit)
		return
	}

	if !validLogFileName(name, prefix) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid log file"})
		return
	}

	path := filepath.Join(logDir, name)
	if !isPathWithinDir(logDir, path) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid log file"})
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "log file not found"})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}
	if info.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid log file"})
		return
	}

	lines, truncated, err := tailLogLines(path, limit, maxLogTailBytes)
	if filter != "" {
		lines, truncated, err = tailMatchingLogLines(path, filter, limit, maxLogSearchBytes)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, logTailResponse{
		File:      name,
		Lines:     lines,
		LineCount: len(lines),
		TotalSize: info.Size(),
		ModTime:   info.ModTime().Format(time.RFC3339),
		Truncated: truncated,
	})
}

func (s *Server) handleFilteredLogTail(w http.ResponseWriter, logDir, prefix, filter string, limit int) {
	files, err := listLogFiles(logDir, prefix)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	lines, usedFiles, totalSize, modTime, truncated, err := tailMatchingLogLinesAcrossFiles(logDir, files, filter, limit, maxLogSearchFiles, maxLogSearchBytes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	fileLabel := ""
	if len(usedFiles) == 1 {
		fileLabel = usedFiles[0]
	} else if len(usedFiles) > 1 {
		fileLabel = fmt.Sprintf("%d matching log files", len(usedFiles))
	}

	writeJSON(w, http.StatusOK, logTailResponse{
		File:      fileLabel,
		Files:     usedFiles,
		Lines:     lines,
		LineCount: len(lines),
		TotalSize: totalSize,
		ModTime:   modTime,
		Truncated: truncated,
	})
}

func (s *Server) handleLogDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	logDir := apiLogDir()
	if logDir == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "RIVUS_LOG_DIR is not configured"})
		return
	}

	prefix := apiLogPrefix()
	if date := strings.TrimSpace(r.URL.Query().Get("date")); date != "" {
		s.serveLogDateZip(w, logDir, prefix, date)
		return
	}

	name := strings.TrimSpace(r.URL.Query().Get("file"))
	if !validLogFileName(name, prefix) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid log file"})
		return
	}

	path := filepath.Join(logDir, name)
	if !isPathWithinDir(logDir, path) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid log file"})
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "log file not found"})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}
	if info.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid log file"})
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, name))
	http.ServeFile(w, r, path)
}

func (s *Server) serveLogDateZip(w http.ResponseWriter, logDir, prefix, date string) {
	if _, err := time.Parse(logDateOnly, date); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "date must use YYYY-MM-DD"})
		return
	}

	files, err := logFilesForDate(logDir, prefix, date)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if len(files) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "log files not found for date"})
		return
	}

	zipName := fmt.Sprintf("%s-%s.zip", prefix, date)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, zipName))

	zw := zip.NewWriter(w)
	defer zw.Close()

	for _, file := range files {
		if err := addFileToZip(zw, filepath.Join(logDir, file), file); err != nil {
			return
		}
	}
}

func listLogFiles(logDir, prefix string) ([]logFileInfo, error) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}

	files := make([]logFileInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !validLogFileName(entry.Name(), prefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, logFileInfo{
			Name:    entry.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime().Format(time.RFC3339),
			modUnix: info.ModTime().UnixNano(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].modUnix != files[j].modUnix {
			return files[i].modUnix > files[j].modUnix
		}
		return files[i].Name > files[j].Name
	})
	return files, nil
}

func logFilesForDate(logDir, prefix, date string) ([]string, error) {
	files, err := listLogFiles(logDir, prefix)
	if err != nil {
		return nil, err
	}

	needle := prefix + "-" + date
	out := make([]string, 0)
	for _, file := range files {
		if file.Name == needle+".log" || strings.HasPrefix(file.Name, needle+"-") {
			out = append(out, file.Name)
		}
	}
	sort.Strings(out)
	return out, nil
}

func validLogFileName(name, prefix string) bool {
	if name == "" || strings.ContainsAny(name, `/\`) {
		return false
	}
	if !strings.HasPrefix(name, prefix+"-") || !strings.HasSuffix(name, ".log") {
		return false
	}
	return true
}

func isPathWithinDir(dir, path string) bool {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absDir, absPath)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel))
}

func parseLogTailLineLimit(raw string) int {
	if strings.TrimSpace(raw) == "" {
		return defaultLogTailLines
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return defaultLogTailLines
	}
	if n > maxLogTailLines {
		return maxLogTailLines
	}
	return n
}

func normalizeLogFilter(raw string) string {
	filter := strings.TrimSpace(raw)
	if filter == "" {
		return ""
	}
	if len(filter) > maxLogFilterLength {
		filter = filter[:maxLogFilterLength]
	}
	return strings.ToLower(filter)
}

func tailLogLines(path string, limit int, maxBytes int64) ([]string, bool, error) {
	if limit <= 0 {
		limit = defaultLogTailLines
	}
	if limit > maxLogTailLines {
		limit = maxLogTailLines
	}
	if maxBytes <= 0 {
		maxBytes = maxLogTailBytes
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	size := info.Size()
	if size == 0 {
		return []string{}, false, nil
	}

	const chunkSize int64 = 32 * 1024
	var buf []byte
	var readBytes int64
	var newlineCount int
	offset := size

	for offset > 0 && newlineCount <= limit && readBytes < maxBytes {
		n := chunkSize
		if offset < n {
			n = offset
		}
		if readBytes+n > maxBytes {
			n = maxBytes - readBytes
		}
		if n <= 0 {
			break
		}
		offset -= n
		chunk := make([]byte, n)
		if _, err := f.ReadAt(chunk, offset); err != nil && err != io.EOF {
			return nil, false, err
		}
		buf = append(chunk, buf...)
		readBytes += n
		newlineCount += strings.Count(string(chunk), "\n")
	}

	truncated := offset > 0
	text := strings.ReplaceAll(string(buf), "\r\n", "\n")
	text = strings.TrimSuffix(text, "\n")
	lines := strings.Split(text, "\n")
	if len(lines) == 1 && lines[0] == "" {
		lines = []string{}
	}
	if truncated && len(lines) > 0 {
		lines = lines[1:]
	}
	if len(lines) > limit {
		lines = lines[len(lines)-limit:]
		truncated = true
	}
	return lines, truncated, nil
}

func tailMatchingLogLines(path string, filter string, limit int, maxBytes int64) ([]string, bool, error) {
	lines, truncated, err := readLogLinesFromEnd(path, maxBytes)
	if err != nil {
		return nil, false, err
	}
	if filter == "" {
		if len(lines) > limit {
			return lines[len(lines)-limit:], true, nil
		}
		return lines, truncated, nil
	}

	matches := make([]string, 0)
	for _, line := range lines {
		if strings.Contains(strings.ToLower(line), filter) {
			matches = append(matches, line)
		}
	}
	if len(matches) > limit {
		matches = matches[len(matches)-limit:]
		truncated = true
	}
	return matches, truncated, nil
}

func readLogLinesFromEnd(path string, maxBytes int64) ([]string, bool, error) {
	if maxBytes <= 0 {
		maxBytes = maxLogSearchBytes
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	size := info.Size()
	if size == 0 {
		return []string{}, false, nil
	}

	readBytes := size
	if readBytes > maxBytes {
		readBytes = maxBytes
	}
	offset := size - readBytes
	buf := make([]byte, readBytes)
	if _, err := f.ReadAt(buf, offset); err != nil && err != io.EOF {
		return nil, false, err
	}

	truncated := offset > 0
	text := strings.ReplaceAll(string(buf), "\r\n", "\n")
	text = strings.TrimSuffix(text, "\n")
	lines := strings.Split(text, "\n")
	if len(lines) == 1 && lines[0] == "" {
		lines = []string{}
	}
	if truncated && len(lines) > 0 {
		lines = lines[1:]
	}
	return lines, truncated, nil
}

func tailMatchingLogLinesAcrossFiles(logDir string, files []logFileInfo, filter string, limit int, maxFiles int, maxBytesPerFile int64) ([]string, []string, int64, string, bool, error) {
	if limit <= 0 {
		limit = defaultLogTailLines
	}
	if limit > maxLogTailLines {
		limit = maxLogTailLines
	}
	if maxFiles <= 0 {
		maxFiles = maxLogSearchFiles
	}

	type matchChunk struct {
		file  string
		lines []string
	}

	remaining := limit
	chunks := make([]matchChunk, 0)
	usedFilesDesc := make([]string, 0)
	var totalSize int64
	modTime := ""
	truncated := false

	for idx, file := range files {
		if idx >= maxFiles {
			truncated = true
			break
		}
		if remaining <= 0 {
			truncated = true
			break
		}

		path := filepath.Join(logDir, file.Name)
		if !isPathWithinDir(logDir, path) {
			continue
		}
		matches, fileTruncated, err := tailMatchingLogLines(path, filter, remaining, maxBytesPerFile)
		if err != nil {
			return nil, nil, 0, "", false, err
		}
		if fileTruncated {
			truncated = true
		}
		if len(matches) == 0 {
			continue
		}

		chunks = append(chunks, matchChunk{file: file.Name, lines: matches})
		usedFilesDesc = append(usedFilesDesc, file.Name)
		totalSize += file.Size
		if modTime == "" {
			modTime = file.ModTime
		}
		remaining -= len(matches)
	}

	lines := make([]string, 0, limit-remaining)
	usedFiles := make([]string, 0, len(usedFilesDesc))
	for i := len(chunks) - 1; i >= 0; i-- {
		lines = append(lines, chunks[i].lines...)
		usedFiles = append(usedFiles, chunks[i].file)
	}

	return lines, usedFiles, totalSize, modTime, truncated, nil
}

func addFileToZip(zw *zip.Writer, path, name string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w, err := zw.Create(name)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, f)
	return err
}

func apiLogDir() string {
	dir := strings.TrimSpace(os.Getenv(envLogDir))
	if dir != "" {
		return dir
	}
	if legacyPath := strings.TrimSpace(os.Getenv(envLogFile)); legacyPath != "" {
		return filepath.Dir(legacyPath)
	}
	return ""
}

func apiLogPrefix() string {
	prefix := strings.TrimSpace(os.Getenv(envLogPrefix))
	if prefix != "" {
		return safeLogName(prefix)
	}
	if legacyPath := strings.TrimSpace(os.Getenv(envLogFile)); legacyPath != "" {
		base := strings.TrimSuffix(filepath.Base(legacyPath), filepath.Ext(legacyPath))
		if base != "" {
			return safeLogName(base)
		}
	}
	return "rivus"
}

func safeLogName(value string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(value) {
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
		return "rivus"
	}
	return out
}
