package api

import (
	"archive/zip"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const envLogRoot = "RIVUS_LOG_ROOT"

type splitLogRole struct {
	Dir    string
	Prefix string
}

var splitLogRoles = map[string]splitLogRole{
	"master":      {Dir: "master", Prefix: "rivus"},
	"streaming":   {Dir: "streaming", Prefix: "rivus-streaming"},
	"snapshot":    {Dir: "snapshot", Prefix: "rivus-snapshot"},
	"maintenance": {Dir: "maintenance", Prefix: "rivus-maintenance"},
}

func splitLogRoot() string {
	if root := strings.TrimSpace(os.Getenv(envLogRoot)); root != "" {
		return root
	}
	return apiLogDir()
}

func splitLogFiles(root string) ([]logFileInfo, error) {
	if root == "" {
		return nil, fmt.Errorf("RIVUS_LOG_ROOT or RIVUS_LOG_DIR is not configured")
	}

	files := make([]logFileInfo, 0)
	if _, err := os.Stat(root); err != nil {
		return nil, err
	}

	// Keep flat files visible for upgrades from the old single-process layout.
	if legacy, err := listLogFiles(root, apiLogPrefix()); err == nil {
		files = append(files, legacy...)
	}

	for role, spec := range splitLogRoles {
		dir := filepath.Join(root, spec.Dir)
		roleFiles, err := listLogFiles(dir, spec.Prefix)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, file := range roleFiles {
			file.Name = filepath.ToSlash(filepath.Join(role, file.Name))
			files = append(files, file)
		}
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].modUnix != files[j].modUnix {
			return files[i].modUnix > files[j].modUnix
		}
		return files[i].Name > files[j].Name
	})
	return files, nil
}

func resolveSplitLogPath(root, name string) (string, error) {
	name = strings.TrimSpace(name)
	if root == "" || name == "" || strings.Contains(name, "\\") {
		return "", fmt.Errorf("invalid log file")
	}

	clean := filepath.ToSlash(filepath.Clean(name))
	if clean != name || strings.HasPrefix(clean, "../") || clean == ".." || filepath.IsAbs(name) {
		return "", fmt.Errorf("invalid log file")
	}

	parts := strings.Split(clean, "/")
	if len(parts) == 1 {
		if !validLogFileName(parts[0], apiLogPrefix()) {
			return "", fmt.Errorf("invalid log file")
		}
		path := filepath.Join(root, parts[0])
		if !isPathWithinDir(root, path) {
			return "", fmt.Errorf("invalid log file")
		}
		return path, nil
	}
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid log file")
	}

	spec, ok := splitLogRoles[parts[0]]
	if !ok || !validLogFileName(parts[1], spec.Prefix) {
		return "", fmt.Errorf("invalid log file")
	}
	roleDir := filepath.Join(root, spec.Dir)
	path := filepath.Join(roleDir, parts[1])
	if !isPathWithinDir(roleDir, path) {
		return "", fmt.Errorf("invalid log file")
	}
	return path, nil
}

func (s *Server) handleSplitLogs(w http.ResponseWriter, r *http.Request) {
	root := splitLogRoot()
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "RIVUS_LOG_ROOT or RIVUS_LOG_DIR is not configured"})
		return
	}
	files, err := splitLogFiles(root)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, files)
}

func (s *Server) handleSplitLogTail(w http.ResponseWriter, r *http.Request) {
	root := splitLogRoot()
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "RIVUS_LOG_ROOT or RIVUS_LOG_DIR is not configured"})
		return
	}

	name := strings.TrimSpace(r.URL.Query().Get("file"))
	filter := normalizeLogFilter(r.URL.Query().Get("filter"))
	limit := parseLogTailLineLimit(r.URL.Query().Get("lines"))

	if name == "" && filter != "" {
		files, err := splitLogFiles(root)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		lines, usedFiles, totalSize, modTime, truncated, err := tailMatchingLogLinesAcrossFiles(root, files, filter, limit, maxLogSearchFiles, maxLogSearchBytes)
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
			File: fileLabel, Files: usedFiles, Lines: lines, LineCount: len(lines),
			TotalSize: totalSize, ModTime: modTime, Truncated: truncated,
		})
		return
	}

	path, err := resolveSplitLogPath(root, name)
	if err != nil {
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
		File: name, Lines: lines, LineCount: len(lines), TotalSize: info.Size(),
		ModTime: info.ModTime().Format(time.RFC3339), Truncated: truncated,
	})
}

func (s *Server) handleSplitLogDownload(w http.ResponseWriter, r *http.Request) {
	root := splitLogRoot()
	if root == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "RIVUS_LOG_ROOT or RIVUS_LOG_DIR is not configured"})
		return
	}

	if date := strings.TrimSpace(r.URL.Query().Get("date")); date != "" {
		if _, err := time.Parse(logDateOnly, date); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "date must use YYYY-MM-DD"})
			return
		}
		files, err := splitLogFiles(root)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		matched := make([]logFileInfo, 0)
		needle := "-" + date
		for _, file := range files {
			base := filepath.Base(file.Name)
			if strings.Contains(base, needle) {
				matched = append(matched, file)
			}
		}
		if len(matched) == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "log files not found for date"})
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="rivus-%s.zip"`, date))
		zw := zip.NewWriter(w)
		defer zw.Close()
		for _, file := range matched {
			path, err := resolveSplitLogPath(root, file.Name)
			if err != nil {
				continue
			}
			if err := addFileToZip(zw, path, file.Name); err != nil {
				return
			}
		}
		return
	}

	name := strings.TrimSpace(r.URL.Query().Get("file"))
	path, err := resolveSplitLogPath(root, name)
	if err != nil {
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
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filepath.Base(name)))
	http.ServeFile(w, r, path)
}
