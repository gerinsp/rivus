package iceberg

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	iceberglib "github.com/apache/iceberg-go"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/util"
)

func (s *Sink) validateScopedSnapshotKeys(state *tableState, rows []map[string]interface{}) error {
	pk, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
	if err != nil {
		return fmt.Errorf("native snapshot requires a primary key: %w", err)
	}
	if len(pk) == 0 {
		return fmt.Errorf("native snapshot requires a primary key")
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		_, key, err := keyFromRow(row, pk)
		if err != nil {
			return err
		}
		for _, name := range pk {
			value, ok := lookupColumnValue(row, name)
			if !ok || value == nil {
				return fmt.Errorf("NULL or missing snapshot primary key in %s", state.sourceKey)
			}
		}
		if seen[key] {
			return fmt.Errorf("duplicate snapshot source primary key in %s", state.sourceKey)
		}
		seen[key] = true
	}
	return nil
}

// DELETE scope is already confirmed. New rows use the native Parquet writer,
// not VALUES SQL. Only keys and partition before-images are looked up, so large
// text/binary payloads never pass through the Trino statement-size bound.
// If a key survives outside the cutoff, or an old MERGE wrote a partial batch,
// remove that existing row and append its replacement in one native RowDelta.
func (s *Sink) flushScopedNativeSnapshot(ctx context.Context, state *tableState, rows []map[string]interface{}) (flushResult, error) {
	if err := s.validateScopedSnapshotKeys(state, rows); err != nil {
		return flushResult{}, util.Permanent(err)
	}
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return flushResult{}, util.Permanent(err)
	}
	batch, err := s.scopedSnapshotReplacementKeys(ctx, state, rows)
	if err != nil {
		return flushResult{}, err
	}
	if batch == nil {
		return s.flushSnapshotAppendOnce(ctx, state, rows)
	}
	batch.rows = rows
	if err := s.applyEqualityDelta(ctx, state, batch, "delete-append-equality"); err != nil {
		return flushResult{}, err
	}
	return flushResult{operation: "delete-append-equality", rowCount: len(rows), deleteCount: batch.deleteCount}, nil
}

func (s *Sink) scopedSnapshotReplacementKeys(ctx context.Context, state *tableState, rows []map[string]interface{}) (*reducedBatch, error) {
	_, filtered := s.snapshotReplaceFilterForState(state)
	if !filtered && !s.scopedLegacyMerges[state.sourceKey] {
		return nil, nil
	}
	// Unpartitioned equality deletes apply globally. Write incoming keys directly
	// with the append; no target scan is needed, including partial v1 recovery.
	before := rows
	if !state.table.Spec().IsUnpartitioned() {
		var err error
		before, err = s.scopedSnapshotBeforeImages(ctx, state, rows)
		if err != nil {
			return nil, err
		}
	}
	if len(before) == 0 {
		return nil, nil
	}
	spec := state.table.Spec()
	if !spec.IsUnpartitioned() && len(state.table.Metadata().PartitionSpecs()) > 1 {
		tasks, err := state.table.Scan().PlanFiles(ctx)
		if err != nil {
			return nil, err
		}
		for _, task := range tasks {
			if int(task.File.SpecID()) != spec.ID() {
				return nil, util.Permanent(fmt.Errorf("native snapshot key replacement blocked by active data using a historical partition spec"))
			}
		}
	}
	pk, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
	if err != nil {
		return nil, util.Permanent(err)
	}
	batch := &reducedBatch{pkCols: pk, deleteCount: len(before)}
	seen := make(map[string]bool, len(before))
	for _, row := range before {
		key, text, err := keyFromRow(row, pk)
		if err != nil {
			return nil, util.Permanent(err)
		}
		if seen[text] {
			return nil, util.Permanent(fmt.Errorf("native snapshot target has duplicate primary keys in %s", state.sourceKey))
		}
		seen[text] = true
		batch.deleteKeys = append(batch.deleteKeys, key)
		batch.deleteRows = append(batch.deleteRows, pendingDelete{key: key, row: row})
	}
	return batch, nil
}

// Bound Arrow/spool buffers by the configured limits, then commit all staged
// files for one source span together. No source ACK/cursor is saved by staging.
func (s *Sink) flushScopedNativeSnapshotSpool(ctx context.Context, state *tableState, rows []map[string]interface{}, ts time.Time, start int64) (flushResult, error) {
	if !state.snapshotReplaceApplied || !state.snapshotTruncateApplied {
		return flushResult{}, util.Permanent(fmt.Errorf("scoped snapshot cannot write before durable reset confirmation"))
	}
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return flushResult{}, util.Permanent(err)
	}
	batch, err := s.scopedSnapshotReplacementKeys(ctx, state, rows)
	if err != nil {
		return flushResult{}, err
	}
	if s.snapshotSpoolDir == "" {
		dir, err := prepareSnapshotSpoolDirectory(s.cfg.SnapshotSpoolDirectory, s.jobID, s.stateKey)
		if err != nil {
			return flushResult{}, err
		}
		s.snapshotSpoolDir = dir
	}
	s.resetSnapshotSpool(state)
	defer s.resetSnapshotSpool(state)
	for _, chunk := range splitRowsByLimits(rows, s.cfg.SnapshotBatchSize, int64(s.cfg.MaxBatchBytes)) {
		if err := s.appendSnapshotSpoolChunk(ctx, state, chunk, ts); err != nil {
			return flushResult{}, err
		}
	}
	if err := s.finalizeSnapshotSpoolWithDeletes(ctx, state, batch); err != nil {
		return flushResult{}, err
	}
	result := flushResult{operation: "append", rowCount: len(rows)}
	if batch != nil {
		result.operation, result.deleteCount = "delete-append-equality", batch.deleteCount
	}
	return result, nil
}

func (s *Sink) scopedSnapshotBeforeImages(ctx context.Context, state *tableState, rows []map[string]interface{}) ([]map[string]interface{}, error) {
	queries, columns, err := s.scopedSnapshotLookupQueries(state, rows)
	if err != nil {
		return nil, util.Permanent(err)
	}
	var before []map[string]interface{}
	for _, query := range queries {
		var result [][]json.RawMessage
		// This is a read; transport retries cannot repeat a target mutation.
		if err := util.RetryWithBackoff(ctx, s.retry, func() error {
			var err error
			result, err = s.execTrinoStatementTracked(ctx, query, state.targetNamespace, nil)
			return err
		}); err != nil {
			return nil, fmt.Errorf("read native snapshot target keys for %s: %w", state.sourceKey, err)
		}
		for _, values := range result {
			if len(values) != len(columns) {
				return nil, util.Permanent(fmt.Errorf("native snapshot key lookup returned wrong column count"))
			}
			row := make(map[string]interface{}, len(columns))
			for i, name := range columns {
				decoder := json.NewDecoder(bytes.NewReader(values[i]))
				decoder.UseNumber()
				var value interface{}
				if err := decoder.Decode(&value); err != nil {
					return nil, util.Permanent(err)
				}
				field, ok := state.table.Schema().FindFieldByNameCaseInsensitive(name)
				if !ok {
					return nil, util.Permanent(fmt.Errorf("native snapshot lookup schema changed"))
				}
				value, err = nativeSnapshotLookupValue(field.Type, value)
				if err != nil {
					return nil, util.Permanent(err)
				}
				row[name] = value
			}
			before = append(before, row)
		}
	}
	return before, meta.CheckOwnership(ctx, s.offsetSto)
}

func nativeSnapshotLookupValue(typ iceberglib.Type, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	switch typ.(type) {
	case iceberglib.BinaryType, iceberglib.FixedType:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("native snapshot binary lookup is not base64")
		}
		return base64.StdEncoding.DecodeString(text)
	case iceberglib.TimestampTzType:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("native snapshot timestamp lookup is invalid")
		}
		if strings.HasSuffix(text, " UTC") {
			return toTime(strings.TrimSuffix(text, " UTC"))
		}
		for _, layout := range []string{"2006-01-02 15:04:05.999999 -07:00", time.RFC3339Nano} {
			if tm, err := time.Parse(layout, text); err == nil {
				return tm, nil
			}
		}
		return nil, fmt.Errorf("native snapshot timestamp timezone is unsupported")
	}
	return value, nil
}

func (s *Sink) scopedSnapshotLookupQueries(state *tableState, rows []map[string]interface{}) ([]string, []string, error) {
	pk, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
	if err != nil || len(pk) == 0 {
		return nil, nil, fmt.Errorf("native snapshot lookup requires primary keys")
	}
	columns := append([]string(nil), pk...)
	spec := state.table.Spec()
	for _, partition := range spec.Fields() {
		field, ok := state.table.Schema().FindFieldByID(partition.SourceID())
		if !ok {
			return nil, nil, fmt.Errorf("native snapshot partition source field missing")
		}
		found := false
		for _, name := range columns {
			found = found || strings.EqualFold(name, field.Name)
		}
		if !found {
			columns = append(columns, field.Name)
		}
	}
	quoted, keyColumns := []string{}, []string{}
	for _, name := range columns {
		quoted = append(quoted, quoteTrinoIdentifier(name))
	}
	for _, name := range pk {
		keyColumns = append(keyColumns, quoteTrinoIdentifier(name))
	}
	keySQL := strings.Join(keyColumns, ",")
	if len(pk) > 1 {
		keySQL = "(" + keySQL + ")"
	}
	catalog := strings.TrimSpace(s.cfg.TrinoDelete.Catalog)
	if catalog == "" {
		catalog = s.cfg.Warehouse
	}
	if catalog == "" {
		return nil, nil, fmt.Errorf("native snapshot lookup requires a Trino catalog")
	}
	prefix := "SELECT " + strings.Join(quoted, ",") + " FROM " + quoteTrinoQualifiedName(catalog, state.targetNamespace, state.targetTable) + " WHERE " + keySQL + " IN ("
	queries, values := []string{}, []string{}
	size := len(prefix) + 1
	for _, row := range rows {
		literals := []string{}
		for _, name := range pk {
			field, ok := state.table.Schema().FindFieldByNameCaseInsensitive(name)
			if !ok {
				return nil, nil, fmt.Errorf("native snapshot key column missing")
			}
			value, ok := lookupColumnValue(row, name)
			if !ok || value == nil {
				return nil, nil, fmt.Errorf("NULL or missing snapshot primary key")
			}
			literal, err := trinoRebuildLiteral(field.Type, value)
			if err != nil {
				return nil, nil, err
			}
			literals = append(literals, literal)
		}
		encoded := strings.Join(literals, ",")
		if len(pk) > 1 {
			encoded = "(" + encoded + ")"
		}
		if len(prefix)+len(encoded)+1 > maxRebuildStatementBytes {
			return nil, nil, fmt.Errorf("snapshot primary key exceeds bounded lookup size for %s", state.sourceKey)
		}
		if size+len(encoded)+1 > maxRebuildStatementBytes && len(values) > 0 {
			queries = append(queries, prefix+strings.Join(values, ",")+")")
			values = nil
			size = len(prefix) + 1
		}
		values = append(values, encoded)
		size += len(encoded) + 1
	}
	if len(values) > 0 {
		queries = append(queries, prefix+strings.Join(values, ",")+")")
	}
	return queries, columns, nil
}
