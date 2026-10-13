package core

type JobStatus string

const (
	JobStatusCreated  JobStatus = "CREATED"
	JobStatusQueued   JobStatus = "QUEUED"
	JobStatusPending  JobStatus = "PENDING"
	JobStatusRunning  JobStatus = "RUNNING"
	JobStatusPausing  JobStatus = "PAUSING"
	JobStatusPaused   JobStatus = "PAUSED"
	JobStatusFailed   JobStatus = "FAILED"
	JobStatusStopped  JobStatus = "STOPPED"
	JobStatusStopping JobStatus = "STOPPING"
	JobStatusDone     JobStatus = "DONE"
)
