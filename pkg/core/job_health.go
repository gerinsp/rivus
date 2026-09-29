package core

import (
	"fmt"
	"strings"
	"time"
)

type JobHealth struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

const (
	JobHealthHealthy  = "HEALTHY"
	JobHealthDegraded = "DEGRADED"
	JobHealthCritical = "CRITICAL"
	JobHealthStale    = "STALE"
)

// Active jobs receive a CDC health heartbeat every 30 seconds. A two-minute
// grace period tolerates transient source/metadata delays without letting an
// hours-old durable RUNNING state remain green in the control plane.
const jobHealthStaleAfter = 2 * time.Minute

func (j *Job) Health() JobHealth {
	if j == nil {
		return JobHealth{}
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return deriveJobHealth(j.status, j.progress, j.Updated, time.Now())
}

func deriveJobHealth(status JobStatus, progress *JobProgress, updated, now time.Time) JobHealth {
	if status != JobStatusRunning && status != JobStatusPending && status != JobStatusPausing {
		return JobHealth{}
	}

	if progress != nil {
		switch strings.ToLower(strings.TrimSpace(progress.CDCBinlogStatus)) {
		case "purged":
			return JobHealth{Status: JobHealthCritical, Detail: "Saved CDC checkpoint has been purged from MySQL"}
		case "missing", "no_binlogs":
			return JobHealth{Status: JobHealthCritical, Detail: "Saved CDC checkpoint is not resumable from the available MySQL binlogs"}
		}
		if isBackpressureProgress(progress) || progress.CheckpointPending {
			detail := "Sink is not draining events fast enough"
			if progress.CheckpointPending {
				detail = "Checkpoint is waiting for pending sink events"
			}
			return JobHealth{Status: JobHealthDegraded, Detail: detail}
		}
	}

	if !updated.IsZero() && now.Sub(updated) > jobHealthStaleAfter {
		return JobHealth{
			Status: JobHealthStale,
			Detail: fmt.Sprintf("No runtime heartbeat or progress update for %s", compactHealthDuration(now.Sub(updated))),
		}
	}
	return JobHealth{Status: JobHealthHealthy}
}

func compactHealthDuration(value time.Duration) string {
	value = value.Round(time.Second)
	if value < 0 {
		value = 0
	}
	return value.String()
}
