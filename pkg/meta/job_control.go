package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// JobControlState is the small durable control-plane view needed to coordinate
// lifecycle actions with split snapshot and streaming workers.
type JobControlState struct {
	DesiredState  DesiredState
	ExecutionRole JobExecutionRole
	LeaseOwner    string
	LeaseUntil    time.Time
	LastStatus    string
}

// JobControlStore is implemented by durable job stores that can coordinate
// lifecycle requests from a control-plane process with remote workers.
type JobControlStore interface {
	LoadJobControl(ctx context.Context, jobID string) (*JobControlState, error)
	LoadJobPauseRequests(ctx context.Context, owner string) ([]string, error)
	RequestJobStop(ctx context.Context, jobID string) (bool, error)
	RequestJobPause(ctx context.Context, jobID string) (bool, error)
	RequestJobResume(ctx context.Context, jobID string, role JobExecutionRole) (bool, error)
}

func (s *MySQLJobStore) LoadJobControl(ctx context.Context, jobID string) (*JobControlState, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("job id is empty")
	}

	var (
		state      JobControlState
		leaseOwner sql.NullString
		leaseUntil sql.NullTime
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT desired_state, execution_role, lease_owner, lease_until, last_status
		FROM job_registry
		WHERE job_id=?`, jobID).Scan(
		&state.DesiredState,
		&state.ExecutionRole,
		&leaseOwner,
		&leaseUntil,
		&state.LastStatus,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state.LeaseOwner = leaseOwner.String
	if leaseUntil.Valid {
		state.LeaseUntil = leaseUntil.Time
	}
	return &state, nil
}

// LoadJobPauseRequests returns only pause requests that are still owned by this
// worker. One indexed query per poll avoids an O(number-of-leased-jobs) control
// plane query pattern on busy streaming workers.
func (s *MySQLJobStore) LoadJobPauseRequests(ctx context.Context, owner string) ([]string, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, fmt.Errorf("worker owner is empty")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id
		FROM job_registry
		WHERE lease_owner=?
		  AND lease_until >= UTC_TIMESTAMP(6)
		  AND desired_state=?
		  AND last_status='PAUSING'
		ORDER BY job_id`, owner, string(DesiredStateRunning))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			return nil, err
		}
		out = append(out, jobID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// RequestJobStop is a hard durable fence. Clearing the lease immediately makes
// every late SaveClaimedJob from the old worker fail, while desired_state=STOPPED
// prevents another worker from reclaiming the job.
func (s *MySQLJobStore) RequestJobStop(ctx context.Context, jobID string) (bool, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return false, fmt.Errorf("job id is empty")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_registry
		SET desired_state=?, last_status='STOPPED', lease_owner=NULL, lease_until=NULL, updated_at=NOW()
		WHERE job_id=?`, string(DesiredStateStopped), jobID)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

// RequestJobPause leaves the current lease in place so the owning worker can
// stop its source, drain the sink, and persist PAUSED at a committed checkpoint.
// The worker observes last_status=PAUSING through LoadJobPauseRequests.
func (s *MySQLJobStore) RequestJobPause(ctx context.Context, jobID string) (bool, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return false, fmt.Errorf("job id is empty")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_registry
		SET last_status='PAUSING', updated_at=NOW()
		WHERE job_id=? AND desired_state=? AND last_status='RUNNING'`,
		jobID, string(DesiredStateRunning))
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

// RequestJobResume makes a terminal job claimable again. The terminal-status
// predicate closes the read/update race: if another caller already resumed and
// a worker has started the job, this update cannot clear that worker's lease.
func (s *MySQLJobStore) RequestJobResume(ctx context.Context, jobID string, role JobExecutionRole) (bool, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return false, fmt.Errorf("job id is empty")
	}
	if role != JobExecutionRoleSnapshot && role != JobExecutionRoleStreaming {
		return false, fmt.Errorf("unsupported execution role %q", role)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_registry
		SET desired_state=?, execution_role=?, last_status='QUEUED', lease_owner=NULL, lease_until=NULL, updated_at=NOW()
		WHERE job_id=? AND last_status IN ('PAUSED','FAILED','STOPPED')`,
		string(DesiredStateRunning), string(role), jobID)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}
