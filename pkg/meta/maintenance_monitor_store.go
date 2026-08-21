package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
)

type MaintenanceMonitorStatus string

const (
	MaintenanceMonitorActive MaintenanceMonitorStatus = "ACTIVE"
	MaintenanceMonitorPaused MaintenanceMonitorStatus = "PAUSED"
)

var (
	ErrMaintenanceMonitorExists   = errors.New("maintenance monitor already exists")
	ErrMaintenanceMonitorNotFound = errors.New("maintenance monitor not found")
)

type IcebergMaintenanceMonitor struct {
	ID              string                   `json:"id"`
	Name            string                   `json:"name"`
	Status          MaintenanceMonitorStatus `json:"status"`
	Config          *config.JobConfig        `json:"-"`
	TableCount      int                      `json:"table_count"`
	LastInventoryAt *time.Time               `json:"last_inventory_at,omitempty"`
	LastError       string                   `json:"last_error,omitempty"`
	CreatedAt       time.Time                `json:"created_at"`
	UpdatedAt       time.Time                `json:"updated_at"`
}

// MaintenanceMonitorOwnerID namespaces monitor ownership away from ingestion
// job IDs while preserving the existing owner_job_id columns and run filters.
func MaintenanceMonitorOwnerID(id string) string {
	return "monitor:" + strings.TrimSpace(id)
}

func (s *IcebergMaintenanceStore) CreateMonitor(ctx context.Context, monitor IcebergMaintenanceMonitor) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("maintenance store is nil")
	}
	monitor.ID = strings.TrimSpace(monitor.ID)
	monitor.Name = strings.TrimSpace(monitor.Name)
	if monitor.ID == "" || monitor.Config == nil {
		return fmt.Errorf("maintenance monitor id and config are required")
	}
	if monitor.Name == "" {
		monitor.Name = monitor.ID
	}
	if monitor.Status == "" {
		monitor.Status = MaintenanceMonitorActive
	}
	payload, err := json.Marshal(monitor.Config)
	if err != nil {
		return fmt.Errorf("encode maintenance monitor config: %w", err)
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `INSERT INTO iceberg_maintenance_monitors
		(monitor_id, monitor_name, status, config_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`, monitor.ID, monitor.Name, string(monitor.Status), string(payload), now, now)
	if err != nil {
		var mysqlErr *drivermysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return ErrMaintenanceMonitorExists
		}
		return err
	}
	return nil
}

func (s *IcebergMaintenanceStore) ListMonitors(ctx context.Context) ([]IcebergMaintenanceMonitor, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT m.monitor_id, m.monitor_name, m.status, m.config_json,
		COUNT(st.table_key), MAX(st.last_inventory_at),
		COALESCE(MAX(NULLIF(st.last_error, '')), ''), m.created_at, m.updated_at
	FROM iceberg_maintenance_monitors m
	LEFT JOIN iceberg_maintenance_state st ON st.owner_job_id=CONCAT('monitor:', m.monitor_id)
	GROUP BY m.monitor_id, m.monitor_name, m.status, m.config_json, m.created_at, m.updated_at
	ORDER BY m.monitor_name, m.monitor_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var monitors []IcebergMaintenanceMonitor
	for rows.Next() {
		monitor, err := scanMaintenanceMonitor(rows)
		if err != nil {
			return nil, err
		}
		monitors = append(monitors, monitor)
	}
	return monitors, rows.Err()
}

func (s *IcebergMaintenanceStore) GetMonitor(ctx context.Context, id string) (*IcebergMaintenanceMonitor, error) {
	row := s.db.QueryRowContext(ctx, `SELECT m.monitor_id, m.monitor_name, m.status, m.config_json,
		COUNT(st.table_key), MAX(st.last_inventory_at),
		COALESCE(MAX(NULLIF(st.last_error, '')), ''), m.created_at, m.updated_at
	FROM iceberg_maintenance_monitors m
	LEFT JOIN iceberg_maintenance_state st ON st.owner_job_id=CONCAT('monitor:', m.monitor_id)
	WHERE m.monitor_id=?
	GROUP BY m.monitor_id, m.monitor_name, m.status, m.config_json, m.created_at, m.updated_at`, strings.TrimSpace(id))
	monitor, err := scanMaintenanceMonitor(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &monitor, nil
}

func (s *IcebergMaintenanceStore) SetMonitorStatus(ctx context.Context, id string, status MaintenanceMonitorStatus, now time.Time) error {
	if status != MaintenanceMonitorActive && status != MaintenanceMonitorPaused {
		return fmt.Errorf("invalid maintenance monitor status %q", status)
	}
	id = strings.TrimSpace(id)
	ownerID := MaintenanceMonitorOwnerID(id)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitors SET status=?, updated_at=? WHERE monitor_id=?`, status, now.UTC(), id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrMaintenanceMonitorNotFound
	}
	if status == MaintenanceMonitorPaused {
		if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state
			SET next_inventory_check_at=NULL, next_compaction_check_at=NULL,
			    next_expire_check_at=NULL, next_orphan_check_at=NULL, updated_at=?
			WHERE owner_job_id=?`, now.UTC(), ownerID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
			SET status=?, lease_owner=NULL, lease_until=NULL, updated_at=?
			WHERE owner_job_id=? AND status IN (?, ?)`, MaintenanceTaskCancelled, now.UTC(), ownerID, MaintenanceTaskQueued, MaintenanceTaskRetry); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state
			SET next_inventory_check_at=?, inventory_priority=100,
			    next_compaction_check_at=?, next_expire_check_at=?, next_orphan_check_at=?,
			    last_error=NULL, updated_at=?
			WHERE owner_job_id=?`, now.UTC(), now.UTC(), now.UTC(), now.UTC(), now.UTC(), ownerID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *IcebergMaintenanceStore) DeleteMonitor(ctx context.Context, id string, now time.Time) error {
	id = strings.TrimSpace(id)
	ownerID := MaintenanceMonitorOwnerID(id)
	deletedOwnerSum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", ownerID, now.UTC().UnixNano())))
	deletedOwnerID := fmt.Sprintf("deleted-monitor:%x", deletedOwnerSum[:16])
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM iceberg_maintenance_monitors WHERE monitor_id=?`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrMaintenanceMonitorNotFound
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state
		SET owner_type='deleted-monitor', owner_job_id=?,
		    next_inventory_check_at=NULL, next_compaction_check_at=NULL,
		    next_expire_check_at=NULL, next_orphan_check_at=NULL, updated_at=?
		WHERE owner_job_id=?`, deletedOwnerID, now.UTC(), ownerID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
		SET owner_job_id=?,
		    lease_owner=CASE WHEN status IN (?, ?) THEN NULL ELSE lease_owner END,
		    lease_until=CASE WHEN status IN (?, ?) THEN NULL ELSE lease_until END,
		    status=CASE WHEN status IN (?, ?) THEN ? ELSE status END,
		    updated_at=?
		WHERE owner_job_id=?`, deletedOwnerID,
		MaintenanceTaskQueued, MaintenanceTaskRetry,
		MaintenanceTaskQueued, MaintenanceTaskRetry,
		MaintenanceTaskQueued, MaintenanceTaskRetry, MaintenanceTaskCancelled,
		now.UTC(), ownerID); err != nil {
		return err
	}
	return tx.Commit()
}

type maintenanceMonitorScanner interface {
	Scan(dest ...any) error
}

func scanMaintenanceMonitor(scanner maintenanceMonitorScanner) (IcebergMaintenanceMonitor, error) {
	var monitor IcebergMaintenanceMonitor
	var status, configJSON string
	var lastInventory sql.NullTime
	if err := scanner.Scan(&monitor.ID, &monitor.Name, &status, &configJSON, &monitor.TableCount,
		&lastInventory, &monitor.LastError, &monitor.CreatedAt, &monitor.UpdatedAt); err != nil {
		return monitor, err
	}
	monitor.Status = MaintenanceMonitorStatus(status)
	if lastInventory.Valid {
		value := lastInventory.Time.UTC()
		monitor.LastInventoryAt = &value
	}
	var cfg config.JobConfig
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return monitor, fmt.Errorf("decode maintenance monitor %s config: %w", monitor.ID, err)
	}
	monitor.Config = &cfg
	return monitor, nil
}
