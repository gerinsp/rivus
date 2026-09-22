package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MaintenanceMonitorClaimStatus string

const (
	MaintenanceMonitorTargetOwned      MaintenanceMonitorClaimStatus = "owned"
	MaintenanceMonitorTargetReserved   MaintenanceMonitorClaimStatus = "reserved"
	MaintenanceMonitorTargetConflicted MaintenanceMonitorClaimStatus = "conflicted"
	MaintenanceMonitorTargetRetired    MaintenanceMonitorClaimStatus = "retired"
)

type IcebergMaintenanceMonitorTarget struct {
	MonitorID   string                        `json:"monitor_id"`
	TableKey    string                        `json:"table_key"`
	Catalog     string                        `json:"catalog"`
	Namespace   string                        `json:"namespace"`
	Table       string                        `json:"table"`
	Specificity int                           `json:"specificity"`
	ClaimStatus MaintenanceMonitorClaimStatus `json:"claim_status"`
	LastError   string                        `json:"last_error,omitempty"`
	CreatedAt   time.Time                     `json:"created_at"`
	UpdatedAt   time.Time                     `json:"updated_at"`

	NextInventoryAt  *time.Time `json:"-"`
	NextCompactionAt time.Time  `json:"-"`
	NextExpireAt     time.Time  `json:"-"`
	NextOrphanAt     time.Time  `json:"-"`
}

type IcebergMaintenanceMonitorDelta struct {
	Added   int `json:"added"`
	Removed int `json:"removed"`
}

func (s *IcebergMaintenanceStore) ListMonitorTargets(ctx context.Context, monitorID string) ([]IcebergMaintenanceMonitorTarget, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT monitor_id, table_key, catalog, namespace_name, table_name,
		specificity, claim_status, COALESCE(last_error, ''), created_at, updated_at
		FROM iceberg_maintenance_monitor_targets WHERE monitor_id=?
		ORDER BY catalog, namespace_name, table_name`, strings.TrimSpace(monitorID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []IcebergMaintenanceMonitorTarget
	for rows.Next() {
		var target IcebergMaintenanceMonitorTarget
		if err := rows.Scan(&target.MonitorID, &target.TableKey, &target.Catalog, &target.Namespace,
			&target.Table, &target.Specificity, &target.ClaimStatus, &target.LastError,
			&target.CreatedAt, &target.UpdatedAt); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (s *IcebergMaintenanceStore) RecordMonitorDiscoveryFailure(ctx context.Context, monitorID, message string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_monitors
		SET last_discovery_error=?, updated_at=? WHERE monitor_id=?`, message, now.UTC(), strings.TrimSpace(monitorID))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrMaintenanceMonitorNotFound
	}
	return nil
}

func (s *IcebergMaintenanceStore) ApplyMonitorDiscovery(
	ctx context.Context,
	monitor IcebergMaintenanceMonitor,
	targets []IcebergMaintenanceMonitorTarget,
	now time.Time,
) (IcebergMaintenanceMonitorDelta, error) {
	var delta IcebergMaintenanceMonitorDelta
	monitor.ID = strings.TrimSpace(monitor.ID)
	if monitor.ID == "" {
		return delta, fmt.Errorf("maintenance monitor id is required")
	}
	now = now.UTC()
	normalized := make(map[string]IcebergMaintenanceMonitorTarget, len(targets))
	for _, target := range targets {
		target.MonitorID = monitor.ID
		target.Catalog = strings.TrimSpace(target.Catalog)
		target.Namespace = strings.TrimSpace(target.Namespace)
		target.Table = strings.TrimSpace(target.Table)
		if target.Catalog == "" || target.Namespace == "" || target.Table == "" {
			return delta, fmt.Errorf("monitor target catalog, namespace, and table are required")
		}
		target.TableKey = strings.TrimSpace(target.TableKey)
		if target.TableKey == "" {
			target.TableKey = target.Catalog + "." + target.Namespace + "." + target.Table
		}
		if target.Specificity <= 0 {
			return delta, fmt.Errorf("monitor target specificity must be positive")
		}
		normalized[target.TableKey] = target
	}
	catalogSet := make(map[string]struct{})
	for _, target := range normalized {
		catalogSet[target.Catalog] = struct{}{}
	}
	knownRows, err := s.db.QueryContext(ctx, `SELECT DISTINCT catalog FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=? AND claim_status<>?`, monitor.ID, MaintenanceMonitorTargetRetired)
	if err != nil {
		return delta, err
	}
	for knownRows.Next() {
		var catalog string
		if err := knownRows.Scan(&catalog); err != nil {
			knownRows.Close()
			return delta, err
		}
		catalogSet[catalog] = struct{}{}
	}
	if err := knownRows.Close(); err != nil {
		return delta, err
	}
	if err := knownRows.Err(); err != nil {
		return delta, err
	}
	catalogs := make([]string, 0, len(catalogSet))
	for catalog := range catalogSet {
		catalogs = append(catalogs, catalog)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return delta, err
	}
	defer tx.Rollback()
	if err := lockMaintenanceCatalogs(ctx, tx, catalogs, now); err != nil {
		return delta, err
	}
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM iceberg_maintenance_monitors
		WHERE monitor_id=? FOR UPDATE`, monitor.ID).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return delta, ErrMaintenanceMonitorNotFound
		}
		return delta, err
	}
	if MaintenanceMonitorStatus(status) != MaintenanceMonitorActive {
		return delta, fmt.Errorf("maintenance monitor %s is not active", monitor.ID)
	}

	existingRows, err := tx.QueryContext(ctx, `SELECT monitor_id, table_key, catalog, namespace_name, table_name,
		specificity, claim_status, COALESCE(last_error, ''), created_at, updated_at
		FROM iceberg_maintenance_monitor_targets WHERE monitor_id=? FOR UPDATE`, monitor.ID)
	if err != nil {
		return delta, err
	}
	existing := make(map[string]IcebergMaintenanceMonitorTarget)
	for existingRows.Next() {
		var target IcebergMaintenanceMonitorTarget
		if err := existingRows.Scan(&target.MonitorID, &target.TableKey, &target.Catalog, &target.Namespace,
			&target.Table, &target.Specificity, &target.ClaimStatus, &target.LastError,
			&target.CreatedAt, &target.UpdatedAt); err != nil {
			existingRows.Close()
			return delta, err
		}
		existing[target.TableKey] = target
	}
	if err := existingRows.Close(); err != nil {
		return delta, err
	}
	if err := existingRows.Err(); err != nil {
		return delta, err
	}

	affected := make(map[string]IcebergMaintenanceMonitorTarget, len(existing)+len(normalized))
	for key, target := range normalized {
		affected[key] = target
	}
	for key, target := range existing {
		if _, keep := normalized[key]; !keep && target.ClaimStatus != MaintenanceMonitorTargetRetired {
			affected[key] = target
		}
	}
	for _, target := range affected {
		if _, locked := catalogSet[target.Catalog]; !locked {
			return delta, fmt.Errorf("maintenance monitor %s target catalog changed during reconciliation; retry", monitor.ID)
		}
	}

	for key, target := range normalized {
		old, exists := existing[key]
		switch {
		case !exists:
			_, err = tx.ExecContext(ctx, `INSERT INTO iceberg_maintenance_monitor_targets
				(monitor_id, table_key, catalog, namespace_name, table_name, specificity, claim_status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, monitor.ID, key, target.Catalog, target.Namespace,
				target.Table, target.Specificity, MaintenanceMonitorTargetConflicted, now, now)
			delta.Added++
		case old.ClaimStatus == MaintenanceMonitorTargetRetired:
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets
				SET catalog=?, namespace_name=?, table_name=?, specificity=?, claim_status=?, last_error=NULL, updated_at=?
				WHERE monitor_id=? AND table_key=?`, target.Catalog, target.Namespace, target.Table,
				target.Specificity, MaintenanceMonitorTargetConflicted, now, monitor.ID, key)
			delta.Added++
		case old.Specificity != target.Specificity:
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets SET specificity=?, updated_at=?
				WHERE monitor_id=? AND table_key=?`, target.Specificity, now, monitor.ID, key)
		}
		if err != nil {
			return delta, err
		}
	}
	for key, target := range existing {
		if _, keep := normalized[key]; keep || target.ClaimStatus == MaintenanceMonitorTargetRetired {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets
			SET claim_status=?, last_error=NULL, updated_at=? WHERE monitor_id=? AND table_key=?`,
			MaintenanceMonitorTargetRetired, now, monitor.ID, key); err != nil {
			return delta, err
		}
		delta.Removed++
	}

	keys := make([]string, 0, len(affected))
	for key := range affected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	reservationCache := make(map[string][]IcebergMaintenanceReservation)
	for _, key := range keys {
		if err := s.reconcileMonitorTargetOwnership(ctx, tx, key, normalized, reservationCache, now); err != nil {
			return delta, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitors
		SET last_discovery_at=?, last_discovery_error=NULL, updated_at=? WHERE monitor_id=?`, now, now, monitor.ID); err != nil {
		return delta, err
	}
	if err := tx.Commit(); err != nil {
		return delta, err
	}
	return delta, nil
}

func (s *IcebergMaintenanceStore) reconcileMonitorTargetOwnership(
	ctx context.Context,
	tx *sql.Tx,
	tableKey string,
	newTargets map[string]IcebergMaintenanceMonitorTarget,
	reservationCache map[string][]IcebergMaintenanceReservation,
	now time.Time,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT target.monitor_id, target.catalog, target.namespace_name, target.table_name,
		target.specificity, target.claim_status, target.created_at
		FROM iceberg_maintenance_monitor_targets AS target
		JOIN iceberg_maintenance_monitors AS monitor ON monitor.monitor_id=target.monitor_id
		WHERE target.table_key=? AND target.claim_status<>? AND monitor.status='ACTIVE'
		ORDER BY target.specificity DESC, target.created_at ASC, target.monitor_id ASC FOR UPDATE`,
		tableKey, MaintenanceMonitorTargetRetired)
	if err != nil {
		return err
	}
	var candidates []IcebergMaintenanceMonitorTarget
	for rows.Next() {
		var target IcebergMaintenanceMonitorTarget
		target.TableKey = tableKey
		if err := rows.Scan(&target.MonitorID, &target.Catalog, &target.Namespace, &target.Table,
			&target.Specificity, &target.ClaimStatus, &target.CreatedAt); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, target)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var state maintenanceReservationState
	stateErr := tx.QueryRowContext(ctx, `SELECT table_key, catalog, namespace_name, table_name,
		owner_type, owner_job_id, lease_until FROM iceberg_maintenance_state WHERE table_key=? FOR UPDATE`, tableKey).Scan(
		&state.TableKey, &state.Catalog, &state.Namespace, &state.Table, &state.OwnerType, &state.OwnerJobID, &state.LeaseUntil,
	)
	if stateErr != nil && !errors.Is(stateErr, sql.ErrNoRows) {
		return stateErr
	}
	if len(candidates) == 0 {
		if stateErr == nil && state.LeaseUntil.Valid && state.LeaseUntil.Time.After(now) && strings.HasPrefix(state.OwnerJobID, "monitor:") {
			return nil
		}
		if stateErr == nil && strings.HasPrefix(state.OwnerJobID, "monitor:") {
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type='unmanaged', owner_job_id='unmanaged',
				next_inventory_check_at=NULL, next_compaction_check_at=NULL, next_expire_check_at=NULL,
				next_orphan_check_at=NULL, updated_at=? WHERE table_key=?`, now, tableKey); err != nil {
				return err
			}
			return cancelQueuedMaintenanceTasksForOwner(ctx, tx, tableKey, state.OwnerJobID, now)
		}
		return nil
	}
	if stateErr == nil && state.LeaseUntil.Valid && state.LeaseUntil.Time.After(now) && strings.HasPrefix(state.OwnerJobID, "monitor:") {
		for _, candidate := range candidates {
			desired := MaintenanceMonitorTargetConflicted
			if MaintenanceMonitorOwnerID(candidate.MonitorID) == state.OwnerJobID {
				desired = MaintenanceMonitorTargetOwned
			}
			if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, desired, now); err != nil {
				return err
			}
		}
		return nil
	}

	sample := candidates[0]
	if stateErr != nil {
		state = maintenanceReservationState{TableKey: tableKey, Catalog: sample.Catalog, Namespace: sample.Namespace, Table: sample.Table}
	}
	reservations, ok := reservationCache[sample.Catalog]
	if !ok {
		reservations, err = loadActiveMaintenanceReservations(ctx, tx, sample.Catalog)
		if err != nil {
			return err
		}
		reservationCache[sample.Catalog] = reservations
	}
	reservation := matchingMaintenanceReservation(reservations, state)
	if reservation.OwnerJobID != "" {
		for _, candidate := range candidates {
			if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, MaintenanceMonitorTargetReserved, now); err != nil {
				return err
			}
		}
		if stateErr == nil && strings.HasPrefix(state.OwnerJobID, "monitor:") {
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type=?, owner_job_id=?,
				next_inventory_check_at=NULL, next_compaction_check_at=NULL, next_expire_check_at=NULL,
				next_orphan_check_at=NULL, updated_at=? WHERE table_key=?`, reservation.Kind, reservation.OwnerJobID, now, tableKey); err != nil {
				return err
			}
			return cancelQueuedMaintenanceTasksForOwner(ctx, tx, tableKey, state.OwnerJobID, now)
		}
		return nil
	}
	if stateErr == nil && !strings.HasPrefix(state.OwnerJobID, "monitor:") && state.OwnerType != "unmanaged" && state.OwnerType != "stream-excluded" && state.OwnerType != "deleted-monitor" {
		for _, candidate := range candidates {
			if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, MaintenanceMonitorTargetReserved, now); err != nil {
				return err
			}
		}
		return nil
	}

	topSpecificity := candidates[0].Specificity
	winner := candidates[0]
	if stateErr == nil && strings.HasPrefix(state.OwnerJobID, "monitor:") {
		for _, candidate := range candidates {
			if candidate.Specificity == topSpecificity && MaintenanceMonitorOwnerID(candidate.MonitorID) == state.OwnerJobID {
				winner = candidate
				break
			}
		}
	}
	for _, candidate := range candidates {
		desired := MaintenanceMonitorTargetConflicted
		if candidate.MonitorID == winner.MonitorID {
			desired = MaintenanceMonitorTargetOwned
		}
		if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, desired, now); err != nil {
			return err
		}
	}

	ownerID := MaintenanceMonitorOwnerID(winner.MonitorID)
	if stateErr == nil && state.OwnerJobID == ownerID {
		return nil
	}
	var nextInventory any = now
	compaction, expire, orphan := now.Add(time.Hour), now.Add(24*time.Hour), now.Add(7*24*time.Hour)
	if target, ok := newTargets[tableKey]; ok && target.MonitorID == winner.MonitorID {
		nextInventory = target.NextInventoryAt
		if target.NextCompactionAt.IsZero() == false {
			compaction = target.NextCompactionAt
		}
		if target.NextExpireAt.IsZero() == false {
			expire = target.NextExpireAt
		}
		if target.NextOrphanAt.IsZero() == false {
			orphan = target.NextOrphanAt
		}
	}
	if stateErr != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO iceberg_maintenance_state
			(table_key, catalog, namespace_name, table_name, owner_type, owner_job_id, snapshot_complete,
			 next_inventory_check_at, inventory_priority, next_compaction_check_at, next_expire_check_at,
			 next_orphan_check_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, 'monitor', ?, 1, ?, 0, ?, ?, ?, ?, ?)`, tableKey, winner.Catalog,
			winner.Namespace, winner.Table, ownerID, nextInventory, compaction.UTC(), expire.UTC(), orphan.UTC(), now, now)
		return err
	}
	oldOwner := state.OwnerJobID
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type='monitor', owner_job_id=?, snapshot_complete=1,
		next_inventory_check_at=COALESCE(next_inventory_check_at, ?), inventory_priority=0,
		next_compaction_check_at=COALESCE(next_compaction_check_at, ?),
		next_expire_check_at=COALESCE(next_expire_check_at, ?), next_orphan_check_at=COALESCE(next_orphan_check_at, ?),
		updated_at=? WHERE table_key=?`, ownerID, nextInventory, compaction.UTC(), expire.UTC(), orphan.UTC(), now, tableKey); err != nil {
		return err
	}
	if oldOwner != ownerID {
		return cancelQueuedMaintenanceTasksForOwner(ctx, tx, tableKey, oldOwner, now)
	}
	return nil
}

func updateMonitorTargetClaimStatus(ctx context.Context, tx *sql.Tx, target IcebergMaintenanceMonitorTarget, status MaintenanceMonitorClaimStatus, now time.Time) error {
	if target.ClaimStatus == status {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets SET claim_status=?, last_error=NULL, updated_at=?
		WHERE monitor_id=? AND table_key=? AND claim_status<>?`, status, now, target.MonitorID, target.TableKey, status)
	return err
}

func cancelQueuedMaintenanceTasksForOwner(ctx context.Context, tx *sql.Tx, tableKey, ownerID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks SET status=?, lease_owner=NULL, lease_until=NULL, updated_at=?
		WHERE table_key=? AND owner_job_id=? AND status IN (?, ?)`, MaintenanceTaskCancelled, now, tableKey, ownerID,
		MaintenanceTaskQueued, MaintenanceTaskRetry)
	return err
}
