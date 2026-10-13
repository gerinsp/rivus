package iceberg

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/util"
	"github.com/google/uuid"
)

const maxRebuildStatementBytes = 512 * 1024

func trinoRebuildType(typ iceberglib.Type) (string, error) {
	switch t := typ.(type) {
	case iceberglib.BooleanType:
		return "BOOLEAN", nil
	case iceberglib.Int32Type:
		return "INTEGER", nil
	case iceberglib.Int64Type:
		return "BIGINT", nil
	case iceberglib.Float32Type:
		return "REAL", nil
	case iceberglib.Float64Type:
		return "DOUBLE", nil
	case iceberglib.DecimalType:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision(), t.Scale()), nil
	case iceberglib.StringType:
		return "VARCHAR", nil
	case iceberglib.BinaryType, iceberglib.FixedType:
		return "VARBINARY", nil
	case iceberglib.UUIDType:
		return "UUID", nil
	case iceberglib.DateType:
		return "DATE", nil
	case iceberglib.TimestampType:
		return "TIMESTAMP(6)", nil
	case iceberglib.TimestampTzType:
		return "TIMESTAMP(6) WITH TIME ZONE", nil
	default:
		return "", fmt.Errorf("scoped rebuild does not support Iceberg type %s", typ)
	}
}

func trinoRebuildLiteral(typ iceberglib.Type, value interface{}) (string, error) {
	typeSQL, err := trinoRebuildType(typ)
	if err != nil {
		return "", err
	}
	if value == nil {
		return "CAST(NULL AS " + typeSQL + ")", nil
	}
	var literal string
	switch typ.(type) {
	case iceberglib.BinaryType, iceberglib.FixedType:
		literal = "X'" + hex.EncodeToString(toBytes(value)) + "'"
	case iceberglib.DecimalType:
		literal = quoteTrinoStringLiteral(toString(value))
	case iceberglib.UUIDType:
		id, err := uuid.Parse(toString(value))
		if err != nil {
			return "", err
		}
		literal = quoteTrinoStringLiteral(id.String())
	case iceberglib.TimestampType, iceberglib.TimestampTzType:
		tm, err := toTime(value)
		if err != nil {
			return "", err
		}
		text := tm.UTC().Format("2006-01-02 15:04:05.000000")
		if _, tz := typ.(iceberglib.TimestampTzType); tz {
			text += " UTC"
		}
		literal = "TIMESTAMP " + quoteTrinoStringLiteral(text)
	default:
		literal, err = trinoLiteralForAny(typ, value)
		if err != nil {
			return "", err
		}
	}
	return "CAST(" + literal + " AS " + typeSQL + ")", nil
}

// Match by PK across the whole target. A row moving across the cutoff must
// update its existing row rather than create a second row in another partition.
func (s *Sink) snapshotMergeTrinoQueries(state *tableState, rows []map[string]interface{}) ([]string, error) {
	pk, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
	if err != nil {
		return nil, fmt.Errorf("scoped rebuild requires a primary key: %w", err)
	}
	if len(pk) == 0 {
		return nil, fmt.Errorf("scoped rebuild requires a primary key")
	}
	reader, release, err := buildRecordReader(state.table.Schema(), rows)
	if err != nil {
		return nil, err
	}
	reader.Release()
	release()
	fields := state.table.Schema().Fields()
	columns, updates, sourceColumns := []string{}, []string{}, []string{}
	for _, field := range fields {
		name := quoteTrinoIdentifier(field.Name)
		columns = append(columns, name)
		updates = append(updates, name+" = s."+name)
		sourceColumns = append(sourceColumns, "s."+name)
	}
	joins := []string{}
	for _, name := range pk {
		quoted := quoteTrinoIdentifier(name)
		joins = append(joins, "t."+quoted+" = s."+quoted)
	}
	catalog := strings.TrimSpace(s.cfg.TrinoDelete.Catalog)
	if catalog == "" {
		catalog = s.cfg.Warehouse
	}
	if catalog == "" {
		return nil, fmt.Errorf("scoped rebuild requires a Trino catalog")
	}
	prefix := "MERGE INTO " + quoteTrinoQualifiedName(catalog, state.targetNamespace, state.targetTable) + " AS t USING (VALUES "
	suffix := ") AS s (" + strings.Join(columns, ",") + ") ON " + strings.Join(joins, " AND ") + " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ",") + " WHEN NOT MATCHED THEN INSERT (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(sourceColumns, ",") + ")"
	seen := map[string]bool{}
	queries, values := []string{}, []string{}
	size := len(prefix) + len(suffix)
	for _, row := range rows {
		_, key, err := keyFromRow(row, pk)
		if err != nil {
			return nil, err
		}
		for _, name := range pk {
			value, ok := lookupColumnValue(row, name)
			if !ok || value == nil {
				return nil, fmt.Errorf("NULL or missing snapshot primary key")
			}
		}
		if seen[key] {
			return nil, fmt.Errorf("duplicate snapshot source primary key")
		}
		seen[key] = true
		literals := []string{}
		for _, field := range fields {
			value, _ := lookupColumnValue(row, field.Name)
			literal, err := trinoRebuildLiteral(field.Type, value)
			if err != nil {
				return nil, fmt.Errorf("scoped rebuild column %s: %w", field.Name, err)
			}
			literals = append(literals, literal)
		}
		encoded := "(" + strings.Join(literals, ",") + ")"
		if len(prefix)+len(suffix)+len(encoded) > maxRebuildStatementBytes {
			return nil, fmt.Errorf("snapshot row exceeds bounded Trino statement size")
		}
		if size+len(encoded)+1 > maxRebuildStatementBytes && len(values) > 0 {
			queries = append(queries, prefix+strings.Join(values, ",")+suffix)
			values = nil
			size = len(prefix) + len(suffix)
		}
		values = append(values, encoded)
		size += len(encoded) + 1
	}
	if len(values) > 0 {
		queries = append(queries, prefix+strings.Join(values, ",")+suffix)
	}
	return queries, nil
}

func (s *Sink) ValidateSnapshotSourceTable(ctx context.Context, schema, table, expression string) error {
	if s.scopedRebuildToken == "" {
		return fmt.Errorf("scoped rebuild submission is not bound")
	}
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return err
	}
	s.mu.Lock()
	state := s.states[tableKey(schema, table)]
	s.mu.Unlock()
	if state == nil || state.table == nil || state.sourceSchema == nil {
		return fmt.Errorf("scoped rebuild target was not initialized")
	}
	filter, filtered := s.snapshotReplaceFilterForState(state)
	var scope *config.IcebergSnapshotReplaceFilterConfig
	if filtered {
		scope = &filter
	}
	if err := config.ValidateIcebergRebuildFilter(expression, scope); err != nil {
		return err
	}
	if !filtered && !s.snapshotTruncateForState(state) {
		return fmt.Errorf("unfiltered source has no configured full reset")
	}
	if _, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema); err != nil {
		return err
	}
	for _, field := range state.table.Schema().Fields() {
		if _, err := trinoRebuildType(field.Type); err != nil {
			return err
		}
	}
	// Confirm that Trino and the native catalog resolve the same metadata, not
	// just tables with equal names in different catalogs. This SELECT is safe to retry.
	catalog := strings.TrimSpace(s.cfg.TrinoDelete.Catalog)
	if catalog == "" {
		catalog = s.cfg.Warehouse
	}
	query := "SELECT file FROM " + quoteTrinoQualifiedName(catalog, state.targetNamespace, state.targetTable+"$metadata_log_entries")
	rows, err := s.execTrinoStatementTracked(ctx, query, state.targetNamespace, nil)
	if err != nil {
		return err
	}
	matched := false
	for _, row := range rows {
		var file string
		if len(row) == 1 && json.Unmarshal(row[0], &file) == nil && file == state.table.MetadataLocation() {
			matched = true
		}
	}
	if !matched {
		return fmt.Errorf("Trino and native catalog target metadata do not match for %s", state.sourceKey)
	}
	if filtered {
		pk, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
		if err != nil {
			return err
		}
		columns, nulls := []string{}, []string{}
		for _, name := range pk {
			quoted := quoteTrinoIdentifier(name)
			columns = append(columns, quoted)
			nulls = append(nulls, quoted+" IS NULL")
		}
		if len(columns) == 0 {
			return fmt.Errorf("scoped rebuild requires a primary key")
		}
		duplicates, err := s.execTrinoStatementTracked(ctx, "SELECT 1 FROM "+quoteTrinoQualifiedName(catalog, state.targetNamespace, state.targetTable)+" GROUP BY "+strings.Join(columns, ",")+" HAVING COUNT(*)>1 OR "+strings.Join(nulls, " OR ")+" LIMIT 1", state.targetNamespace, nil)
		if err != nil {
			return err
		}
		if len(duplicates) > 0 {
			return fmt.Errorf("scoped rebuild target has duplicate or NULL primary keys: %s", state.sourceKey)
		}
	}
	return meta.CheckOwnership(ctx, s.offsetSto)
}

func (s *Sink) ResetSnapshotSourceTable(ctx context.Context, schema, table, expression string) error {
	s.mu.Lock()
	state := s.states[tableKey(schema, table)]
	s.mu.Unlock()
	if state == nil {
		return fmt.Errorf("scoped rebuild target unavailable")
	}
	filter, filtered := s.snapshotReplaceFilterForState(state)
	var scope *config.IcebergSnapshotReplaceFilterConfig
	if filtered {
		scope = &filter
	}
	if err := config.ValidateIcebergRebuildFilter(expression, scope); err != nil {
		return err
	}
	var query string
	var err error
	if filtered {
		query, _, err = s.snapshotReplaceFilterTrinoSQL(state)
	} else {
		query, err = s.snapshotTruncateTrinoSQL(state)
	}
	if err != nil {
		return util.Permanent(err)
	}
	if err := s.executeRebuildStatement(ctx, state, "reset", query); err != nil {
		return err
	}
	if err := s.refreshStateTable(ctx, state); err != nil {
		return err
	}
	s.mu.Lock()
	state.snapshotReplaceApplied = true
	state.snapshotTruncateApplied = true
	s.mu.Unlock()
	return nil
}

func (s *Sink) flushScopedSnapshot(ctx context.Context, state *tableState, rows []map[string]interface{}) (flushResult, error) {
	s.mu.Lock()
	reset := state.snapshotReplaceApplied && state.snapshotTruncateApplied
	s.mu.Unlock()
	if !reset {
		return flushResult{}, util.Permanent(fmt.Errorf("scoped snapshot cannot write before durable reset confirmation"))
	}
	queries, err := s.snapshotMergeTrinoQueries(state, rows)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}
	for _, query := range queries {
		if err := s.executeRebuildStatement(ctx, state, "merge", query); err != nil {
			return flushResult{}, err
		}
	}
	if err := s.refreshStateTable(ctx, state); err != nil {
		return flushResult{}, err
	}
	state.lastTouchedAt = time.Now()
	return flushResult{operation: "snapshot-merge-trino", rowCount: len(rows)}, nil
}

func (s *Sink) confirmScopedResetBarrier(ctx context.Context) error {
	store, err := s.icebergIntentStore()
	if err != nil {
		return err
	}
	intents, err := store.ListIcebergCommits(ctx, s.checkpointKey())
	if err != nil {
		return err
	}
	confirmed := map[string]bool{}
	for _, intent := range intents {
		if intent.Kind != "rebuild" || intent.Status != meta.IcebergCommitCommitted {
			return util.Permanent(fmt.Errorf("scoped snapshot start BLOCKED by unresolved operation=%s", intent.OperationID))
		}
		if err := s.confirmTrinoRebuildIntent(ctx, intent); err != nil {
			return util.Permanent(err)
		}
		if intent.Operation == "trino-reset" && intent.BatchID == "rebuild:"+s.scopedRebuildToken+":"+intent.SourceTable+":reset" {
			s.mu.Lock()
			state := s.states[intent.SourceTable]
			s.mu.Unlock()
			if state == nil {
				return util.Permanent(fmt.Errorf("scoped reset source selection changed"))
			}
			var query string
			var scopeErr error
			if _, filtered := s.snapshotReplaceFilterForState(state); filtered {
				query, _, scopeErr = s.snapshotReplaceFilterTrinoSQL(state)
			} else {
				query, scopeErr = s.snapshotTruncateTrinoSQL(state)
			}
			if scopeErr != nil || trinoStatementDigest(query) != intent.ReplayKeys {
				return util.Permanent(fmt.Errorf("scoped reset statement scope changed operation=%s", intent.OperationID))
			}
			confirmed[intent.SourceTable] = true
		}
	}
	s.mu.Lock()
	var missing string
	for _, state := range s.states {
		if !confirmed[state.sourceKey] {
			missing = state.sourceKey
			break
		}
	}
	s.mu.Unlock()
	if missing != "" {
		return util.Permanent(fmt.Errorf("scoped snapshot start lacks durable reset confirmation for %s", missing))
	}
	return meta.CheckOwnership(ctx, s.offsetSto)
}

func (s *Sink) cleanupScopedRebuildIntents(ctx context.Context) error {
	state, err := s.offsetSto.GetSnapshotState(ctx, s.checkpointKey())
	if err != nil {
		return err
	}
	if !state.CompletionConfirmed() {
		return fmt.Errorf("scoped rebuild cleanup requires confirmed snapshot completion")
	}
	store, err := s.icebergIntentStore()
	if err != nil {
		return err
	}
	intents, err := store.ListIcebergCommits(ctx, s.checkpointKey())
	if err != nil {
		return err
	}
	for _, intent := range intents {
		if intent.Kind != "rebuild" {
			continue
		}
		if intent.Status != meta.IcebergCommitCommitted {
			return fmt.Errorf("scoped rebuild has an unresolved statement")
		}
		if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
			return err
		}
		if err := store.DeleteIcebergCommit(ctx, intent.StateKey, intent.OperationID); err != nil {
			return err
		}
	}
	return nil
}
