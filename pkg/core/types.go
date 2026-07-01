package core

type JobStatus string

const (
	JobStatusCreated JobStatus = "CREATED"
	JobStatusQueued  JobStatus = "QUEUED"
	JobStatusPending JobStatus = "PENDING"
	JobStatusRunning JobStatus = "RUNNING"
	JobStatusPausing JobStatus = "PAUSING"
	JobStatusPaused  JobStatus = "PAUSED"
	JobStatusFailed  JobStatus = "FAILED"
	JobStatusStopped JobStatus = "STOPPED"
	JobStatusDone    JobStatus = "DONE"
)
