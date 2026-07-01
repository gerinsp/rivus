package iceberg

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
)

func (s *Sink) augmentSchemaForMetadata(sourceSchema *model.TableSchema) *model.TableSchema {
	out := copyTableSchema(sourceSchema)
	if out == nil {
		return nil
	}

	addTimestampColumn := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" || tableSchemaHasColumn(out, name) {
			return
		}
		out.Columns = append(out.Columns, model.TableColumn{
			Name:       name,
			DataType:   "timestamp",
			ColumnType: "timestamp",
			IsNullable: true,
		})
	}

	if s.cfg.MetadataColumns.CreatedAt.Enabled {
		addTimestampColumn(s.cfg.MetadataColumns.CreatedAt.Name)
	}
	if s.cfg.MetadataColumns.UpdatedAt.Enabled {
		addTimestampColumn(s.cfg.MetadataColumns.UpdatedAt.Name)
	}
	if s.cfg.MetadataColumns.ETLLoadedAt.Enabled {
		addTimestampColumn(s.cfg.MetadataColumns.ETLLoadedAt.Name)
	}

	return out
}

func (s *Sink) enrichPendingRows(ctx context.Context, state *tableState, pendingRows []pendingRow, pkCols []string) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, 0, len(pendingRows))
	if len(pendingRows) == 0 {
		return out, nil
	}

	cfg := s.cfg.MetadataColumns
	if !metadataColumnsEnabled(cfg) {
		for _, pending := range pendingRows {
			out = append(out, pending.row)
		}
		return out, nil
	}

	preserveCols := s.metadataPreserveColumns(state, pendingRows)
	existing := map[string]map[string]interface{}{}
	if len(preserveCols) > 0 {
		var err error
		existing, err = s.fetchExistingMetadata(ctx, state, pendingRows, pkCols, preserveCols)
		if err != nil {
			return nil, err
		}
	}

	loadTime := time.Now().UTC()
	for _, pending := range pendingRows {
		row := cloneMap(pending.row)
		existingRow := existing[keyHashFromValues(pending.key, pkCols)]

		if cfg.CreatedAt.Enabled {
			s.applyCreatedAt(row, existingRow, state, pending)
		}
		if cfg.UpdatedAt.Enabled {
			s.applyUpdatedAt(row, existingRow, pending)
		}
		if cfg.ETLLoadedAt.Enabled {
			row[cfg.ETLLoadedAt.Name] = loadTime
		}

		out = append(out, row)
	}

	return out, nil
}

func metadataColumnsEnabled(cfg config.IcebergMetadataColumnsConfig) bool {
	return cfg.CreatedAt.Enabled || cfg.UpdatedAt.Enabled || cfg.ETLLoadedAt.Enabled
}

func (s *Sink) applyCreatedAt(row, existingRow map[string]interface{}, state *tableState, pending pendingRow) {
	targetCol := s.cfg.MetadataColumns.CreatedAt.Name
	if targetCol == "" {
		return
	}

	if sourceCol, ok := s.createdAtSourceColumn(state); ok {
		if value, exists := lookupColumnValue(pending.row, sourceCol); exists {
			row[targetCol] = value
			return
		}
	}

	if value, exists := lookupColumnValue(row, targetCol); exists && value != nil {
		return
	}
	if value, exists := lookupColumnValue(existingRow, targetCol); exists {
		row[targetCol] = value
	}
}

func (s *Sink) applyUpdatedAt(row, existingRow map[string]interface{}, pending pendingRow) {
	targetCol := s.cfg.MetadataColumns.UpdatedAt.Name
	if targetCol == "" {
		return
	}

	if pending.event.Origin == model.EventOriginCDC && !pending.event.Timestamp.IsZero() {
		row[targetCol] = pending.event.Timestamp.UTC()
		return
	}
	if value, exists := lookupColumnValue(existingRow, targetCol); exists {
		row[targetCol] = value
	}
}

func (s *Sink) metadataPreserveColumns(state *tableState, rows []pendingRow) []string {
	cols := make([]string, 0, 2)
	if state == nil || state.table == nil {
		return cols
	}
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" || !icebergSchemaHasField(state.table.Schema(), name) {
			return
		}
		for _, existing := range cols {
			if strings.EqualFold(existing, name) {
				return
			}
		}
		cols = append(cols, name)
	}

	cfg := s.cfg.MetadataColumns
	if cfg.UpdatedAt.Enabled {
		for _, row := range rows {
			if row.event.Origin != model.EventOriginCDC {
				add(cfg.UpdatedAt.Name)
				break
			}
		}
	}
	if cfg.CreatedAt.Enabled {
		add(cfg.CreatedAt.Name)
	}
	return cols
}

func (s *Sink) fetchExistingMetadata(ctx context.Context, state *tableState, rows []pendingRow, pkCols []string, metadataCols []string) (map[string]map[string]interface{}, error) {
	out := make(map[string]map[string]interface{})
	if state == nil || state.table == nil || state.table.CurrentSnapshot() == nil || len(rows) == 0 || len(metadataCols) == 0 {
		return out, nil
	}

	keysByHash := make(map[string]map[string]interface{}, len(rows))
	for _, row := range rows {
		hash := keyHashFromValues(row.key, pkCols)
		if _, exists := keysByHash[hash]; !exists {
			keysByHash[hash] = row.key
		}
	}

	keys := make([]map[string]interface{}, 0, len(keysByHash))
	for _, key := range keysByHash {
		keys = append(keys, key)
	}

	filter, err := buildKeyFilter(state.table.Schema(), keys)
	if err != nil {
		return nil, err
	}

	selected := selectedExistingFields(state.table.Schema(), append(append([]string{}, pkCols...), metadataCols...))
	if len(selected) == 0 {
		return out, nil
	}

	scan := state.table.Scan(
		icetable.WithCaseSensitive(false),
		icetable.WithRowFilter(filter),
		icetable.WithSelectedFields(selected...),
	)
	_, records, err := scan.ToArrowRecords(ctx)
	if err != nil {
		if isIcebergGoEqualityDeleteUnsupported(err) {
			log.Printf("[WARN][iceberg][job %s] skip existing metadata preserve table=%s target=%s: iceberg-go scanner does not support equality deletes",
				s.jobID,
				state.sourceKey,
				tableKey(state.targetNamespace, state.targetTable),
			)
			return out, nil
		}
		return nil, s.stateOperationError("read existing metadata", state, err)
	}

	for rec, recErr := range records {
		if recErr != nil {
			if isIcebergGoEqualityDeleteUnsupported(recErr) {
				log.Printf("[WARN][iceberg][job %s] skip existing metadata preserve table=%s target=%s: iceberg-go scanner does not support equality deletes",
					s.jobID,
					state.sourceKey,
					tableKey(state.targetNamespace, state.targetTable),
				)
				return out, nil
			}
			return nil, s.stateOperationError("read existing metadata", state, recErr)
		}
		scannedRows := arrowRecordBatchRows(rec)
		rec.Release()
		for _, scanned := range scannedRows {
			_, hash, err := keyFromRow(scanned, pkCols)
			if err != nil {
				return nil, err
			}
			out[hash] = scanned
		}
	}

	return out, nil
}

func isIcebergGoEqualityDeleteUnsupported(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "does not yet support equality deletes")
}

func (s *Sink) createdAtSourceColumn(state *tableState) (string, bool) {
	if state == nil || state.sourceSchema == nil {
		return "", false
	}
	sourceKey := state.sourceKey
	srcSchema := state.sourceSchema.SchemaName
	srcTable := state.sourceSchema.TableName

	if sourceCol, ok := s.cfg.MetadataColumns.CreatedAt.SourceColumns[sourceKey]; ok {
		return sourceCol, true
	}

	matches := make([]string, 0)
	for key := range s.cfg.MetadataColumns.CreatedAt.SourceColumns {
		if key == sourceKey || !matchSourceOverrideKey(key, srcSchema, srcTable) {
			continue
		}
		matches = append(matches, key)
	}
	if len(matches) == 0 {
		return "", false
	}
	sort.Slice(matches, func(i, j int) bool {
		leftScore := globSpecificity(matches[i])
		rightScore := globSpecificity(matches[j])
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return matches[i] < matches[j]
	})
	return s.cfg.MetadataColumns.CreatedAt.SourceColumns[matches[0]], true
}

func tableSchemaHasColumn(schema *model.TableSchema, name string) bool {
	if schema == nil {
		return false
	}
	for _, col := range schema.Columns {
		if strings.EqualFold(strings.TrimSpace(col.Name), strings.TrimSpace(name)) {
			return true
		}
	}
	return false
}

func icebergSchemaHasField(schema *iceberglib.Schema, name string) bool {
	if schema == nil {
		return false
	}
	_, ok := schema.FindFieldByNameCaseInsensitive(name)
	return ok
}

func selectedExistingFields(schema *iceberglib.Schema, names []string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		field, ok := schema.FindFieldByNameCaseInsensitive(name)
		if !ok {
			continue
		}
		seen := false
		for _, existing := range out {
			if strings.EqualFold(existing, field.Name) {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, field.Name)
		}
	}
	return out
}

func keyHashFromValues(values map[string]interface{}, pkCols []string) string {
	parts := make([]string, 0, len(pkCols))
	for _, col := range pkCols {
		value, _ := lookupColumnValue(values, col)
		parts = append(parts, fmt.Sprintf("%s=%v", strings.ToLower(col), value))
	}
	return strings.Join(parts, "|")
}

func arrowRecordBatchRows(rec arrow.RecordBatch) []map[string]interface{} {
	if rec == nil {
		return nil
	}

	rows := make([]map[string]interface{}, 0, int(rec.NumRows()))
	fields := rec.Schema().Fields()
	for rowIdx := 0; rowIdx < int(rec.NumRows()); rowIdx++ {
		row := make(map[string]interface{}, int(rec.NumCols()))
		for colIdx, field := range fields {
			row[field.Name] = arrowArrayValue(rec.Column(colIdx), rowIdx)
		}
		rows = append(rows, row)
	}
	return rows
}

func arrowArrayValue(col arrow.Array, idx int) interface{} {
	if col == nil || col.IsNull(idx) {
		return nil
	}

	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(idx)
	case *array.Int8:
		return int64(arr.Value(idx))
	case *array.Int16:
		return int64(arr.Value(idx))
	case *array.Int32:
		return int64(arr.Value(idx))
	case *array.Int64:
		return arr.Value(idx)
	case *array.Uint8:
		return uint64(arr.Value(idx))
	case *array.Uint16:
		return uint64(arr.Value(idx))
	case *array.Uint32:
		return uint64(arr.Value(idx))
	case *array.Uint64:
		return arr.Value(idx)
	case *array.Float32:
		return float64(arr.Value(idx))
	case *array.Float64:
		return arr.Value(idx)
	case *array.String:
		return arr.Value(idx)
	case *array.LargeString:
		return arr.Value(idx)
	case *array.Binary:
		return append([]byte(nil), arr.Value(idx)...)
	case *array.LargeBinary:
		return append([]byte(nil), arr.Value(idx)...)
	case *array.Timestamp:
		if typ, ok := arr.DataType().(*arrow.TimestampType); ok {
			return arr.Value(idx).ToTime(typ.Unit).UTC()
		}
		return arr.Value(idx)
	case *array.Date32:
		return arr.Value(idx).ToTime()
	case *array.Date64:
		return arr.Value(idx).ToTime()
	case *array.Decimal128:
		return arr.ValueStr(idx)
	default:
		return col.ValueStr(idx)
	}
}
