package core

import "sort"

type WorkerLease struct {
	JobID        string
	SubmissionID string
}

// WorkerLeaseSnapshot returns the stable worker identity and the job submissions whose
// durable leases are currently owned by this process. Runtime wrappers use the
// snapshot during graceful shutdown to extend those leases before stopping the
// reconciliation loop, preventing another worker from reclaiming a job while
// its source/sink is still draining.
func (m *JobManager) WorkerLeaseSnapshot() (string, []WorkerLease) {
	if m == nil {
		return "", nil
	}
	m.mu.RLock()
	owner := m.workerID
	leases := make([]WorkerLease, 0, len(m.workerLeases))
	for id, submissionID := range m.workerLeases {
		leases = append(leases, WorkerLease{JobID: id, SubmissionID: submissionID})
	}
	m.mu.RUnlock()
	sort.Slice(leases, func(i, j int) bool {
		return leases[i].JobID < leases[j].JobID
	})
	return owner, leases
}
