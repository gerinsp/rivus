package core

import "sort"

// WorkerLeaseSnapshot returns the stable worker identity and the job IDs whose
// durable leases are currently owned by this process. Runtime wrappers use the
// snapshot during graceful shutdown to extend those leases before stopping the
// reconciliation loop, preventing another worker from reclaiming a job while
// its source/sink is still draining.
func (m *JobManager) WorkerLeaseSnapshot() (string, []string) {
	if m == nil {
		return "", nil
	}
	m.mu.RLock()
	owner := m.workerID
	ids := make([]string, 0, len(m.workerLeases))
	for id := range m.workerLeases {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	sort.Strings(ids)
	return owner, ids
}
