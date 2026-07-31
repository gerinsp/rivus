package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
)

type DesiredState string

const (
	DesiredStateRunning DesiredState = "RUNNING"
	DesiredStateStopped DesiredState = "STOPPED"
)

type PersistedJob struct {
	ID           string
	Name         string
	Config       *config.JobConfig
	DesiredState DesiredState
	LastStatus   string
	Errors       []PersistedJobError
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type PersistedJobError struct {
	Component string    `json:"component"`
	Message   string    `json:"message"`
	Time      time.Time `json:"time"`
}

type FailureNotificationState string

const (
	FailureNotificationPending FailureNotificationState = "PENDING"
	FailureNotificationSent    FailureNotificationState = "SENT"
	FailureNotificationFailed  FailureNotificationState = "FAILED"
)

type FailureNotificationPayload struct {
	JobID           string `json:"job_id"`
	JobName         string `json:"job_name,omitempty"`
	SinkType        string `json:"sink_type,omitempty"`
	ErrorComponent  string `json:"error_component,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
	ProgressSummary string `json:"progress_summary,omitempty"`
	ProgressDetail  string `json:"progress_detail,omitempty"`
	DashboardURL    string `json:"dashboard_url,omitempty"`
}

type FailureNotification struct {
	IncidentID    string
	JobID         string
	Payload       FailureNotificationPayload
	State         FailureNotificationState
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	SentAt        time.Time
}

type JobStore interface {
	Init(ctx context.Context) error
	SaveJob(ctx context.Context, job PersistedJob) error
	LoadJobs(ctx context.Context) ([]PersistedJob, error)
	DeleteJob(ctx context.Context, jobID string) error
}

// FailureNotificationStore is an optional durable outbox capability. Job
// managers continue to work with JobStore implementations that do not provide
// it, but notification retries cannot survive a process restart without it.
type FailureNotificationStore interface {
	SaveFailureNotification(ctx context.Context, notification FailureNotification) error
	LoadPendingFailureNotifications(ctx context.Context) ([]FailureNotification, error)
}

type MySQLJobStore struct {
	db *sql.DB
}

func NewMySQLJobStore(dsn string) (*MySQLJobStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	return &MySQLJobStore{db: db}, nil
}

func (s *MySQLJobStore) Init(ctx context.Context) error {
	const ddl = `
	CREATE TABLE IF NOT EXISTS job_registry (
	  job_id        VARCHAR(255) NOT NULL PRIMARY KEY,
	  job_name      VARCHAR(255) NOT NULL,
	  config_json   LONGTEXT NOT NULL,
	  desired_state VARCHAR(32) NOT NULL,
	  last_status   VARCHAR(32) NOT NULL,
	  errors_json   LONGTEXT NULL,
	  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	  updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE job_registry ADD COLUMN errors_json LONGTEXT NULL AFTER last_status`); err != nil && !isDuplicateColumnError(err) {
		return err
	}
	const notificationDDL = `
	CREATE TABLE IF NOT EXISTS job_failure_notifications (
	  incident_id     VARCHAR(255) NOT NULL PRIMARY KEY,
	  job_id          VARCHAR(255) NOT NULL,
	  payload_json    LONGTEXT NOT NULL,
	  state           VARCHAR(32) NOT NULL,
	  attempts        INT NOT NULL DEFAULT 0,
	  next_attempt_at DATETIME(6) NULL,
	  last_error      LONGTEXT NULL,
	  created_at      DATETIME(6) NOT NULL,
	  updated_at      DATETIME(6) NOT NULL,
	  sent_at         DATETIME(6) NULL,
	  INDEX idx_failure_notifications_pending (state, next_attempt_at),
	  INDEX idx_failure_notifications_job (job_id)
	);`
	if _, err := s.db.ExecContext(ctx, notificationDDL); err != nil {
		return err
	}
	return nil
}

func (s *MySQLJobStore) SaveJob(ctx context.Context, job PersistedJob) error {
	if job.Config == nil {
		return fmt.Errorf("persisted job config is nil for job_id=%s", job.ID)
	}

	cfg := *job.Config
	config.ApplyDefaults(&cfg)

	payload, err := json.Marshal(&cfg)
	if err != nil {
		return err
	}
	errorsJSON, err := json.Marshal(job.Errors)
	if err != nil {
		return err
	}

	id := job.ID
	if id == "" {
		id = cfg.ID
	}
	if id == "" {
		return fmt.Errorf("persisted job id is empty")
	}

	name := job.Name
	if name == "" {
		name = cfg.Name
	}

	desired := job.DesiredState
	if desired == "" {
		desired = DesiredStateStopped
	}
	status := job.LastStatus
	if status == "" {
		status = "CREATED"
	}

	const stmt = `
	INSERT INTO job_registry (job_id, job_name, config_json, desired_state, last_status, errors_json, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())
	ON DUPLICATE KEY UPDATE
	  job_name = VALUES(job_name),
	  config_json = VALUES(config_json),
	  desired_state = VALUES(desired_state),
	  last_status = VALUES(last_status),
	  errors_json = VALUES(errors_json),
	  updated_at = NOW();`
	_, err = s.db.ExecContext(ctx, stmt, id, name, string(payload), string(desired), status, string(errorsJSON))
	return err
}

func (s *MySQLJobStore) LoadJobs(ctx context.Context) ([]PersistedJob, error) {
	const q = `
	SELECT job_id, job_name, config_json, desired_state, last_status, errors_json, created_at, updated_at
	FROM job_registry
	ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]PersistedJob, 0)
	for rows.Next() {
		var (
			jobID, name, configJSON, desiredState, lastStatus string
			errorsJSON                                        sql.NullString
			createdAt, updatedAt                              time.Time
		)
		if err := rows.Scan(&jobID, &name, &configJSON, &desiredState, &lastStatus, &errorsJSON, &createdAt, &updatedAt); err != nil {
			return nil, err
		}

		var cfg config.JobConfig
		if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
			return nil, fmt.Errorf("decode persisted job %s: %w", jobID, err)
		}
		config.ApplyDefaults(&cfg)
		if cfg.ID == "" {
			cfg.ID = jobID
		}
		if cfg.Name == "" {
			cfg.Name = name
		}

		var errorHistory []PersistedJobError
		if errorsJSON.Valid && errorsJSON.String != "" && errorsJSON.String != "null" {
			if err := json.Unmarshal([]byte(errorsJSON.String), &errorHistory); err != nil {
				return nil, fmt.Errorf("decode persisted job errors %s: %w", jobID, err)
			}
		}

		out = append(out, PersistedJob{
			ID:           jobID,
			Name:         name,
			Config:       &cfg,
			DesiredState: DesiredState(desiredState),
			LastStatus:   lastStatus,
			Errors:       errorHistory,
			CreatedAt:    createdAt,
			UpdatedAt:    updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *MySQLJobStore) DeleteJob(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM job_failure_notifications WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM job_registry WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLJobStore) SaveFailureNotification(ctx context.Context, notification FailureNotification) error {
	if notification.IncidentID == "" {
		return fmt.Errorf("failure notification incident id is empty")
	}
	if notification.JobID == "" {
		return fmt.Errorf("failure notification job id is empty")
	}
	payload, err := json.Marshal(notification.Payload)
	if err != nil {
		return err
	}
	state := notification.State
	if state == "" {
		state = FailureNotificationPending
	}
	createdAt := notification.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	updatedAt := notification.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	var nextAttemptAt any
	if !notification.NextAttemptAt.IsZero() {
		nextAttemptAt = notification.NextAttemptAt.UTC()
	}
	var sentAt any
	if !notification.SentAt.IsZero() {
		sentAt = notification.SentAt.UTC()
	}

	const stmt = `
	INSERT INTO job_failure_notifications (
	  incident_id, job_id, payload_json, state, attempts, next_attempt_at,
	  last_error, created_at, updated_at, sent_at
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
	  payload_json = IF(state IN ('SENT', 'FAILED'), payload_json, VALUES(payload_json)),
	  attempts = IF(state IN ('SENT', 'FAILED'), attempts, VALUES(attempts)),
	  next_attempt_at = IF(state IN ('SENT', 'FAILED'), next_attempt_at, VALUES(next_attempt_at)),
	  last_error = IF(state IN ('SENT', 'FAILED'), last_error, VALUES(last_error)),
	  updated_at = IF(state IN ('SENT', 'FAILED'), updated_at, VALUES(updated_at)),
	  sent_at = IF(state = 'SENT', sent_at, VALUES(sent_at)),
	  state = IF(state IN ('SENT', 'FAILED'), state, VALUES(state));`
	_, err = s.db.ExecContext(
		ctx,
		stmt,
		notification.IncidentID,
		notification.JobID,
		string(payload),
		string(state),
		notification.Attempts,
		nextAttemptAt,
		notification.LastError,
		createdAt.UTC(),
		updatedAt.UTC(),
		sentAt,
	)
	return err
}

func (s *MySQLJobStore) LoadPendingFailureNotifications(ctx context.Context) ([]FailureNotification, error) {
	const query = `
	SELECT incident_id, job_id, payload_json, state, attempts, next_attempt_at,
	       last_error, created_at, updated_at, sent_at
	FROM job_failure_notifications
	WHERE state = 'PENDING'
	ORDER BY created_at ASC;`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]FailureNotification, 0)
	for rows.Next() {
		var (
			notification          FailureNotification
			payloadJSON, state    string
			nextAttemptAt, sentAt sql.NullTime
			lastError             sql.NullString
		)
		if err := rows.Scan(
			&notification.IncidentID,
			&notification.JobID,
			&payloadJSON,
			&state,
			&notification.Attempts,
			&nextAttemptAt,
			&lastError,
			&notification.CreatedAt,
			&notification.UpdatedAt,
			&sentAt,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(payloadJSON), &notification.Payload); err != nil {
			return nil, fmt.Errorf("decode failure notification %s: %w", notification.IncidentID, err)
		}
		notification.State = FailureNotificationState(state)
		if nextAttemptAt.Valid {
			notification.NextAttemptAt = nextAttemptAt.Time
		}
		if sentAt.Valid {
			notification.SentAt = sentAt.Time
		}
		if lastError.Valid {
			notification.LastError = lastError.String
		}
		out = append(out, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
