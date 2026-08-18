//go:build linux

package iceberg

import (
	"os"

	"golang.org/x/sys/unix"
)

// releaseSnapshotSpoolCache tells Linux that already-synced Arrow spool pages
// are not needed until the final sequential read. The data remains safely on
// disk; Linux simply may reclaim its page-cache memory under pressure.
func releaseSnapshotSpoolCache(file *os.File, offset, length int64) {
	if file == nil || offset < 0 || length <= 0 {
		return
	}
	_ = unix.Fadvise(int(file.Fd()), offset, length, unix.FADV_DONTNEED)
}
