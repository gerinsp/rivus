package meta

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

// RuntimeInstance is the last reported build and heartbeat for one Rivus
// process. Role and InstanceID form the durable identity, allowing multiple
// replicas of the same role to be reported independently.
type RuntimeInstance struct {
	Role        string    `json:"role"`
	InstanceID  string    `json:"instance_id"`
	Version     string    `json:"version,omitempty"`
	ImageTag    string    `json:"image_tag,omitempty"`
	Commit      string    `json:"commit,omitempty"`
	BuildDate   string    `json:"build_date,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

type RuntimeInstanceStore struct {
	db *sql.DB
}

func NewRuntimeInstanceStore(dsn string) (*RuntimeInstanceStore, error) {
	cfg, err := drivermysql.ParseDSN(strings.TrimSpace(dsn))
	if err != nil {
		return nil, fmt.Errorf("parse runtime instance store dsn: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &RuntimeInstanceStore{db: db}, nil
}

func (s *RuntimeInstanceStore) Init(ctx context.Context) error {
	const ddl = `
	CREATE TABLE IF NOT EXISTS rivus_runtime_instances (
	  role         VARCHAR(32) NOT NULL,
	  instance_id  VARCHAR(255) NOT NULL,
	  version      VARCHAR(128) NOT NULL DEFAULT '',
	  image_tag    VARCHAR(255) NOT NULL DEFAULT '',
	  commit_hash  VARCHAR(255) NOT NULL DEFAULT '',
	  build_date   VARCHAR(128) NOT NULL DEFAULT '',
	  started_at   DATETIME(6) NOT NULL,
	  heartbeat_at DATETIME(6) NOT NULL,
	  PRIMARY KEY (role, instance_id),
	  INDEX idx_runtime_instances_heartbeat (heartbeat_at),
	  INDEX idx_runtime_instances_role_heartbeat (role, heartbeat_at)
	);`
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

func (s *RuntimeInstanceStore) RegisterRuntimeInstance(ctx context.Context, instance RuntimeInstance) error {
	return s.writeRuntimeInstance(ctx, instance, true)
}

func (s *RuntimeInstanceStore) Heartbeat(ctx context.Context, instance RuntimeInstance) error {
	return s.writeRuntimeInstance(ctx, instance, false)
}

func (s *RuntimeInstanceStore) writeRuntimeInstance(ctx context.Context, instance RuntimeInstance, resetStartedAt bool) error {
	instance.Role = strings.TrimSpace(instance.Role)
	instance.InstanceID = strings.TrimSpace(instance.InstanceID)
	if instance.Role == "" || instance.InstanceID == "" {
		return fmt.Errorf("runtime role and instance id are required")
	}
	now := time.Now().UTC()
	if instance.StartedAt.IsZero() {
		instance.StartedAt = now
	}
	if instance.HeartbeatAt.IsZero() {
		instance.HeartbeatAt = now
	}

	query := `
	INSERT INTO rivus_runtime_instances
	  (role, instance_id, version, image_tag, commit_hash, build_date, started_at, heartbeat_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
	  version=VALUES(version), image_tag=VALUES(image_tag),
	  commit_hash=VALUES(commit_hash), build_date=VALUES(build_date),
	  heartbeat_at=VALUES(heartbeat_at)`
	if resetStartedAt {
		query += `, started_at=VALUES(started_at)`
	}
	_, err := s.db.ExecContext(ctx, query,
		instance.Role, instance.InstanceID, instance.Version, instance.ImageTag,
		instance.Commit, instance.BuildDate, instance.StartedAt.UTC(), instance.HeartbeatAt.UTC(),
	)
	return err
}

func (s *RuntimeInstanceStore) ListRuntimeInstances(ctx context.Context) ([]RuntimeInstance, error) {
	const query = `
	SELECT role, instance_id, version, image_tag, commit_hash, build_date, started_at, heartbeat_at
	FROM rivus_runtime_instances
	WHERE heartbeat_at >= DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 30 DAY)
	ORDER BY role ASC, heartbeat_at DESC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	instances := make([]RuntimeInstance, 0, 8)
	for rows.Next() {
		var instance RuntimeInstance
		if err := rows.Scan(
			&instance.Role, &instance.InstanceID, &instance.Version, &instance.ImageTag,
			&instance.Commit, &instance.BuildDate, &instance.StartedAt, &instance.HeartbeatAt,
		); err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	return instances, rows.Err()
}

func (s *RuntimeInstanceStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
