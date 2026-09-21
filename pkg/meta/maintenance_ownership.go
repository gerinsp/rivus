package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	MaintenanceReservationStreaming = "streaming"
	MaintenanceReservationSnapshot  = "snapshot"
)

var ErrMaintenanceOwnershipBusy = errors.New("maintenance ownership is busy")

// IcebergMaintenanceReservationSelector identifies a physical Iceberg target
// scope. NamespacePattern and TablePattern use path.Match syntax so one
// streaming reservation can protect both existing and future matching tables.
type IcebergMaintenanceReservationSelector struct {
	Catalog          string
	NamespacePattern string
	TablePattern     string
}

func (s IcebergMaintenanceReservationSelector) Validate() error {
	s.Catalog = strings.TrimSpace(s.Catalog)
	s.NamespacePattern = strings.TrimSpace(s.NamespacePattern)
	s.TablePattern = strings.TrimSpace(s.TablePattern)
	if s.Catalog == "" {
		return fmt.Errorf("reservation catalog is required")
	}
	if strings.ContainsAny(s.Catalog, "*?[") {
		return fmt.Errorf("reservation catalog must be an exact physical catalog")
	}
	if s.NamespacePattern == "" {
		return fmt.Errorf("reservation namespace pattern is required")
	}
	if s.TablePattern == "" {
		return fmt.Errorf("reservation table pattern is required")
	}
	if _, err := path.Match(s.NamespacePattern, s.NamespacePattern); err != nil {
		return fmt.Errorf("invalid namespace pattern %q: %w", s.NamespacePattern, err)
	}
	if _, err := path.Match(s.TablePattern, s.TablePattern); err != nil {
		return fmt.Errorf("invalid table pattern %q: %w", s.TablePattern, err)
	}
	return nil
}

type IcebergMaintenanceReservation struct {
	IcebergMaintenanceReservationSelector
	OwnerJobID   string
	SubmissionID string
	Kind         string
	Active       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (r IcebergMaintenanceReservation) Matches(catalog, namespace, table string) bool {
	if !r.Active {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(r.Catalog), strings.TrimSpace(catalog)) {
		return false
	}
	namespaceMatch, namespaceErr := path.Match(strings.ToLower(r.NamespacePattern), strings.ToLower(strings.TrimSpace(namespace)))
	tableMatch, tableErr := path.Match(strings.ToLower(r.TablePattern), strings.ToLower(strings.TrimSpace(table)))
	return namespaceErr == nil && tableErr == nil && namespaceMatch && tableMatch
}

func preferredMaintenanceReservation(current, candidate IcebergMaintenanceReservation) IcebergMaintenanceReservation {
	if current.OwnerJobID == "" {
		return candidate
	}
	if current.Kind != MaintenanceReservationStreaming && candidate.Kind == MaintenanceReservationStreaming {
		return candidate
	}
	return current
}

type maintenanceReservationState struct {
	TableKey   string
	Catalog    string
	Namespace  string
	Table      string
	OwnerType  string
	OwnerJobID string
	LeaseUntil sql.NullTime
}

func (s *IcebergMaintenanceStore) SyncMaintenanceReservations(
	ctx context.Context,
	ownerJobID, submissionID, kind string,
	selectors []IcebergMaintenanceReservationSelector,
	now time.Time,
) error {
	ownerJobID = strings.TrimSpace(ownerJobID)
	submissionID = strings.TrimSpace(submissionID)
	kind = strings.ToLower(strings.TrimSpace(kind))
	if ownerJobID == "" || submissionID == "" {
		return fmt.Errorf("reservation owner job id and submission id are required")
	}
	if kind != MaintenanceReservationStreaming && kind != MaintenanceReservationSnapshot {
		return fmt.Errorf("unsupported maintenance reservation kind %q", kind)
	}
	normalized, err := normalizeMaintenanceReservationSelectors(selectors)
	if err != nil {
		return err
	}
	if len(normalized) == 0 {
		return fmt.Errorf("at least one maintenance reservation selector is required")
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	catalogs := make(map[string]struct{}, len(normalized))
	for _, selector := range normalized {
		catalogs[selector.Catalog] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT catalog FROM iceberg_maintenance_reservations
		WHERE owner_job_id=? AND active=1`, ownerJobID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var catalog string
		if err := rows.Scan(&catalog); err != nil {
			rows.Close()
			return err
		}
		catalogs[catalog] = struct{}{}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := lockMaintenanceCatalogs(ctx, tx, sortedStringSet(catalogs), now); err != nil {
		return err
	}

	statesByCatalog := make(map[string][]maintenanceReservationState, len(catalogs))
	for _, catalog := range sortedStringSet(catalogs) {
		states, err := loadMaintenanceReservationStates(ctx, tx, catalog)
		if err != nil {
			return err
		}
		statesByCatalog[catalog] = states
	}
	for _, selector := range normalized {
		reservation := IcebergMaintenanceReservation{IcebergMaintenanceReservationSelector: selector, Active: true}
		for _, state := range statesByCatalog[selector.Catalog] {
			if !reservation.Matches(state.Catalog, state.Namespace, state.Table) {
				continue
			}
			if state.LeaseUntil.Valid && state.LeaseUntil.Time.After(now) && state.OwnerJobID != ownerJobID {
				return fmt.Errorf("%w: table %s is leased until %s", ErrMaintenanceOwnershipBusy, state.TableKey, state.LeaseUntil.Time.UTC().Format(time.RFC3339))
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_reservations
		SET active=0, updated_at=? WHERE owner_job_id=? AND active=1`, now.UTC(), ownerJobID); err != nil {
		return err
	}
	for _, selector := range normalized {
		reservationKey := maintenanceReservationKey(ownerJobID, submissionID, selector)
		if _, err := tx.ExecContext(ctx, `INSERT INTO iceberg_maintenance_reservations
			(reservation_key, owner_job_id, submission_id, reservation_kind, catalog, namespace_pattern, table_pattern, active, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
			ON DUPLICATE KEY UPDATE reservation_kind=VALUES(reservation_kind), active=1, updated_at=VALUES(updated_at)`,
			reservationKey, ownerJobID, submissionID, kind, selector.Catalog, selector.NamespacePattern, selector.TablePattern, now.UTC(), now.UTC()); err != nil {
			return err
		}
	}

	matchingKeys := make([]string, 0)
	for catalog, states := range statesByCatalog {
		for _, state := range states {
			if !maintenanceStateMatchesSelectors(state, normalized) || (!strings.HasPrefix(state.OwnerJobID, "monitor:") && state.OwnerJobID != "stream-excluded" && state.OwnerJobID != "unmanaged") {
				continue
			}
			matchingKeys = append(matchingKeys, state.TableKey)
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET
				owner_type=?, owner_job_id=?, snapshot_complete=0,
				next_inventory_check_at=NULL, inventory_priority=0,
				next_compaction_check_at=NULL, next_expire_check_at=NULL, next_orphan_check_at=NULL,
				updated_at=? WHERE table_key=?`, kind, ownerJobID, now.UTC(), state.TableKey); err != nil {
				return err
			}
		}
		_ = catalog
	}
	if err := cancelMaintenanceTasksForTables(ctx, tx, matchingKeys, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *IcebergMaintenanceStore) ReleaseMaintenanceReservations(
	ctx context.Context,
	ownerJobID, submissionID string,
	now time.Time,
) error {
	ownerJobID = strings.TrimSpace(ownerJobID)
	submissionID = strings.TrimSpace(submissionID)
	if ownerJobID == "" || submissionID == "" {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT catalog FROM iceberg_maintenance_reservations
		WHERE owner_job_id=? AND submission_id=? AND active=1`, ownerJobID, submissionID)
	if err != nil {
		return err
	}
	var catalogs []string
	for rows.Next() {
		var catalog string
		if err := rows.Scan(&catalog); err != nil {
			rows.Close()
			return err
		}
		catalogs = append(catalogs, catalog)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(catalogs) == 0 {
		return tx.Commit()
	}
	if err := lockMaintenanceCatalogs(ctx, tx, catalogs, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_reservations SET active=0, updated_at=?
		WHERE owner_job_id=? AND submission_id=? AND active=1`, now.UTC(), ownerJobID, submissionID); err != nil {
		return err
	}

	for _, catalog := range catalogs {
		states, err := loadMaintenanceReservationStates(ctx, tx, catalog)
		if err != nil {
			return err
		}
		reservations, err := loadActiveMaintenanceReservations(ctx, tx, catalog)
		if err != nil {
			return err
		}
		for _, state := range states {
			if state.OwnerJobID != ownerJobID {
				continue
			}
			winner := matchingMaintenanceReservation(reservations, state)
			ownerType, nextOwner := "unmanaged", "unmanaged"
			if winner.OwnerJobID != "" {
				ownerType, nextOwner = winner.Kind, winner.OwnerJobID
			}
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type=?, owner_job_id=?,
				next_inventory_check_at=NULL, inventory_priority=0,
				next_compaction_check_at=NULL, next_expire_check_at=NULL, next_orphan_check_at=NULL,
				updated_at=? WHERE table_key=? AND owner_job_id=?`, ownerType, nextOwner, now.UTC(), state.TableKey, ownerJobID); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks SET status=?, lease_owner=NULL, lease_until=NULL, updated_at=?
		WHERE owner_job_id=? AND status IN (?, ?)`, MaintenanceTaskCancelled, now.UTC(), ownerJobID, MaintenanceTaskQueued, MaintenanceTaskRetry); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *IcebergMaintenanceStore) ReleaseMaintenanceReservationsExcept(ctx context.Context, keep map[string]string, now time.Time) error {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT owner_job_id, submission_id
		FROM iceberg_maintenance_reservations WHERE active=1`)
	if err != nil {
		return err
	}
	type ownerSubmission struct{ owner, submission string }
	var stale []ownerSubmission
	for rows.Next() {
		var pair ownerSubmission
		if err := rows.Scan(&pair.owner, &pair.submission); err != nil {
			rows.Close()
			return err
		}
		if current, exists := keep[pair.owner]; !exists || current != pair.submission {
			stale = append(stale, pair)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, pair := range stale {
		if err := s.ReleaseMaintenanceReservations(ctx, pair.owner, pair.submission, now); err != nil {
			return err
		}
	}
	return nil
}

func normalizeMaintenanceReservationSelectors(selectors []IcebergMaintenanceReservationSelector) ([]IcebergMaintenanceReservationSelector, error) {
	seen := make(map[string]IcebergMaintenanceReservationSelector, len(selectors))
	for _, selector := range selectors {
		selector.Catalog = strings.TrimSpace(selector.Catalog)
		selector.NamespacePattern = strings.ToLower(strings.TrimSpace(selector.NamespacePattern))
		selector.TablePattern = strings.ToLower(strings.TrimSpace(selector.TablePattern))
		if err := selector.Validate(); err != nil {
			return nil, err
		}
		key := strings.ToLower(selector.Catalog) + "\x00" + selector.NamespacePattern + "\x00" + selector.TablePattern
		seen[key] = selector
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]IcebergMaintenanceReservationSelector, 0, len(keys))
	for _, key := range keys {
		out = append(out, seen[key])
	}
	return out, nil
}

func maintenanceReservationKey(ownerJobID, submissionID string, selector IcebergMaintenanceReservationSelector) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(ownerJobID),
		strings.TrimSpace(submissionID),
		strings.ToLower(strings.TrimSpace(selector.Catalog)),
		strings.ToLower(strings.TrimSpace(selector.NamespacePattern)),
		strings.ToLower(strings.TrimSpace(selector.TablePattern)),
	}, "\x00")))
	return fmt.Sprintf("%x", sum[:])
}

func sortedStringSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func lockMaintenanceCatalogs(ctx context.Context, tx *sql.Tx, catalogs []string, now time.Time) error {
	sort.Strings(catalogs)
	for _, catalog := range catalogs {
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO iceberg_maintenance_catalog_guards (catalog, updated_at) VALUES (?, ?)`, catalog, now.UTC()); err != nil {
			return err
		}
		var locked string
		if err := tx.QueryRowContext(ctx, `SELECT catalog FROM iceberg_maintenance_catalog_guards WHERE catalog=? FOR UPDATE`, catalog).Scan(&locked); err != nil {
			return err
		}
	}
	return nil
}

func loadMaintenanceReservationStates(ctx context.Context, tx *sql.Tx, catalog string) ([]maintenanceReservationState, error) {
	rows, err := tx.QueryContext(ctx, `SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id, lease_until
		FROM iceberg_maintenance_state WHERE catalog=? FOR UPDATE`, catalog)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var states []maintenanceReservationState
	for rows.Next() {
		var state maintenanceReservationState
		if err := rows.Scan(&state.TableKey, &state.Catalog, &state.Namespace, &state.Table, &state.OwnerType, &state.OwnerJobID, &state.LeaseUntil); err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, rows.Err()
}

func loadActiveMaintenanceReservations(ctx context.Context, tx *sql.Tx, catalog string) ([]IcebergMaintenanceReservation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT owner_job_id, submission_id, reservation_kind, catalog,
		namespace_pattern, table_pattern, active, created_at, updated_at
		FROM iceberg_maintenance_reservations WHERE catalog=? AND active=1
		ORDER BY CASE reservation_kind WHEN 'streaming' THEN 0 ELSE 1 END, id FOR UPDATE`, catalog)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reservations []IcebergMaintenanceReservation
	for rows.Next() {
		var reservation IcebergMaintenanceReservation
		if err := rows.Scan(
			&reservation.OwnerJobID, &reservation.SubmissionID, &reservation.Kind,
			&reservation.Catalog, &reservation.NamespacePattern, &reservation.TablePattern,
			&reservation.Active, &reservation.CreatedAt, &reservation.UpdatedAt,
		); err != nil {
			return nil, err
		}
		reservations = append(reservations, reservation)
	}
	return reservations, rows.Err()
}

func maintenanceStateMatchesSelectors(state maintenanceReservationState, selectors []IcebergMaintenanceReservationSelector) bool {
	for _, selector := range selectors {
		reservation := IcebergMaintenanceReservation{IcebergMaintenanceReservationSelector: selector, Active: true}
		if reservation.Matches(state.Catalog, state.Namespace, state.Table) {
			return true
		}
	}
	return false
}

func matchingMaintenanceReservation(reservations []IcebergMaintenanceReservation, state maintenanceReservationState) IcebergMaintenanceReservation {
	var winner IcebergMaintenanceReservation
	for _, reservation := range reservations {
		if reservation.Matches(state.Catalog, state.Namespace, state.Table) {
			winner = preferredMaintenanceReservation(winner, reservation)
		}
	}
	return winner
}

func cancelMaintenanceTasksForTables(ctx context.Context, tx *sql.Tx, tableKeys []string, now time.Time) error {
	if len(tableKeys) == 0 {
		return nil
	}
	const chunkSize = 200
	for start := 0; start < len(tableKeys); start += chunkSize {
		end := min(start+chunkSize, len(tableKeys))
		chunk := tableKeys[start:end]
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		args := make([]any, 0, len(chunk)+4)
		args = append(args, MaintenanceTaskCancelled, now.UTC(), MaintenanceTaskQueued, MaintenanceTaskRetry)
		for _, tableKey := range chunk {
			args = append(args, tableKey)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
			SET status=?, lease_owner=NULL, lease_until=NULL, updated_at=?
			WHERE status IN (?, ?) AND table_key IN (`+placeholders+`)`, args...); err != nil {
			return err
		}
	}
	return nil
}
