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

type JobStore interface {
	Init(ctx context.Context) error
	SaveJob(ctx context.Context, job PersistedJob) error
	LoadJobs(ctx context.Context) ([]PersistedJob, error)
	DeleteJob(ctx context.Context, jobID string) error
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
	_, err := s.db.ExecContext(ctx, `DELETE FROM job_registry WHERE job_id = ?`, jobID)
	return err
}
