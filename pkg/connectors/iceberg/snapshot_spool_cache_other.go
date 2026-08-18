//go:build !linux

package iceberg

import "os"

// Keep the rolling snapshot implementation portable for local development.
// Linux containers use the implementation in snapshot_spool_cache_linux.go.
func releaseSnapshotSpoolCache(_ *os.File, _, _ int64) {}
