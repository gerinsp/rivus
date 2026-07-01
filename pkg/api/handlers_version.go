package api

import (
	"net/http"

	"github.com/gerinsp/rivus/pkg/version"
)

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, version.Current())
}
