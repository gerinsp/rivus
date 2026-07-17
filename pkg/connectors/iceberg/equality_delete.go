package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberglib "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/gerinsp/rivus/pkg/util"
)

type countingWriteCloser struct {
	w      io.WriteCloser
	n      int64
	closed bool
}

type cdcEqualityCommitter interface {
	CommitEqualityDelta(ctx context.Context, state *tableState, batch *reducedBatch) (*icetable.Table, error)
}

type rivusEqualityCommitter struct {
	sink *Sink
}

func (c rivusEqualityCommitter) CommitEqualityDelta(ctx context.Context, state *tableState, batch *reducedBatch) (*icetable.Table, error) {
	return c.sink.commitEqualityDelta(ctx, state, batch)
}

func (w *countingWriteCloser) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *countingWriteCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.w.Close()
}

func (s *Sink) applyEqualityDelta(ctx context.Context, state *tableState, batch *reducedBatch, operation string) error {
	if batch == nil || len(batch.deleteKeys) == 0 {
		return nil
	}
	if len(batch.pkCols) == 0 {
		return util.Permanent(fmt.Errorf("equality delete requires primary key columns for %s", state.sourceKey))
	}

	result := flushResult{
		operation:   operation,
		rowCount:    len(batch.rows),
		deleteCount: batch.deleteCount,
	}
	var updated *icetable.Table
	var startedAt time.Time
	var duration time.Duration
	err := s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
		rowCount:        result.rowCount,
		deleteCount:     result.deleteCount,
	}, func() error {
		startedAt = time.Now()
		var err error
		committer := s.equalityCommitter
		if committer == nil {
			committer = rivusEqualityCommitter{sink: s}
		}
		updated, err = committer.CommitEqualityDelta(ctx, state, batch)
		duration = time.Since(startedAt)
		return err
	})
	s.logWriteTiming(state, result, err, startedAt, duration)
	if err != nil {
		return s.stateOperationError(result.operation, state, err)
	}

	s.updateStateTableAfterWrite(state, updated)
	return nil
}

func (s *Sink) commitEqualityDelta(ctx context.Context, state *tableState, batch *reducedBatch) (*icetable.Table, error) {
	if state == nil || state.table == nil {
		return nil, fmt.Errorf("table handle is nil for equality delta")
	}
	if batch == nil || len(batch.deleteKeys) == 0 {
		return nil, fmt.Errorf("equality delta requires at least one delete key")
	}
	if len(batch.pkCols) == 0 {
		return nil, fmt.Errorf("equality delta requires primary key columns for %s", state.sourceKey)
	}

	tbl := state.table
	meta := tbl.Metadata()
	if meta.Version() < 2 {
		return nil, fmt.Errorf("equality deletes require Iceberg format-version 2 or newer")
	}

	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	writeFS, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("iceberg filesystem does not support writes for equality delete")
	}

	stagedFiles := make([]iceberglib.DataFile, 0, len(batch.rows)+len(batch.deleteKeys))
	cleanupStaged := true
	defer func() {
		if !cleanupStaged {
			return
		}
		removeStagedIcebergFiles(writeFS, stagedFiles)
	}()

	deleteFiles, err := writeEqualityDeleteFiles(ctx, tbl, writeFS, batch.deleteKeys, batch.deleteRows, batch.pkCols)
	if err != nil {
		return nil, err
	}
	if len(deleteFiles) == 0 {
		return nil, fmt.Errorf("equality delete built no delete files for %s", state.sourceKey)
	}
	stagedFiles = append(stagedFiles, deleteFiles...)

	dataFiles := make([]iceberglib.DataFile, 0, 1)
	if len(batch.rows) > 0 {
		reader, release, err := buildRecordReader(tbl.Schema(), batch.rows)
		if err != nil {
			return nil, err
		}
		defer release()

		for dataFile, writeErr := range icetable.WriteRecords(ctx, tbl, reader.Schema(), array.IterFromReader(reader)) {
			if writeErr != nil {
				return nil, writeErr
			}
			dataFiles = append(dataFiles, dataFile)
			stagedFiles = append(stagedFiles, dataFile)
		}
		if len(dataFiles) == 0 {
			return nil, fmt.Errorf("equality delta built no data files for %s", state.sourceKey)
		}
	}

	txn := tbl.NewTransaction()
	delta := txn.NewRowDelta(s.snapshotProps(state))
	delta.AddDeletes(deleteFiles...)
	if len(dataFiles) > 0 {
		delta.AddRows(dataFiles...)
	}
	if err := delta.Commit(ctx); err != nil {
		return nil, err
	}

	// Once the catalog commit starts its outcome may be unknown on transport
	// failure. Leave staged files for orphan cleanup rather than risk deleting
	// files that an accepted commit references.
	cleanupStaged = false
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func removeStagedIcebergFiles(fs iceio.WriteFileIO, files []iceberglib.DataFile) {
	for _, file := range files {
		if file == nil || strings.TrimSpace(file.FilePath()) == "" {
			continue
		}
		if err := fs.Remove(file.FilePath()); err != nil {
			log.Printf("[iceberg] failed to remove staged equality-delta file=%q: %v", file.FilePath(), err)
		}
	}
}

func writeEqualityDeleteFiles(ctx context.Context, tbl *icetable.Table, fs iceio.WriteFileIO, keys []map[string]interface{}, deleteRows []pendingDelete, pkCols []string) ([]iceberglib.DataFile, error) {
	if tbl.Spec().IsUnpartitioned() {
		deleteFile, err := writeEqualityDeleteFile(ctx, tbl, fs, keys, pkCols, map[int]any{})
		if err != nil {
			return nil, err
		}
		return []iceberglib.DataFile{deleteFile}, nil
	}

	groups, err := partitionDeleteGroups(tbl, deleteRows)
	if err != nil {
		return nil, err
	}
	deleteFiles := make([]iceberglib.DataFile, 0, len(groups))
	complete := false
	defer func() {
		if !complete {
			removeStagedIcebergFiles(fs, deleteFiles)
		}
	}()
	for _, group := range groups {
		deleteFile, err := writeEqualityDeleteFile(ctx, tbl, fs, group.keys, pkCols, group.partitionData)
		if err != nil {
			return nil, err
		}
		deleteFiles = append(deleteFiles, deleteFile)
	}
	complete = true
	return deleteFiles, nil
}

type partitionDeleteGroup struct {
	partitionKey  string
	partitionData map[int]any
	keys          []map[string]interface{}
}

func partitionDeleteGroups(tbl *icetable.Table, deleteRows []pendingDelete) ([]partitionDeleteGroup, error) {
	if len(deleteRows) == 0 {
		return nil, fmt.Errorf("partitioned equality delete requires delete row before-image data")
	}

	groupsByKey := make(map[string]*partitionDeleteGroup)
	for _, del := range deleteRows {
		rows := del.partitionRows
		if len(rows) == 0 {
			rows = []map[string]interface{}{del.row}
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("partitioned equality delete requires row data for key %s", stableStringMapKey(del.key))
		}
		for _, row := range rows {
			if len(row) == 0 {
				return nil, fmt.Errorf("partitioned equality delete requires row data for key %s", stableStringMapKey(del.key))
			}
			partitionData, err := equalityDeletePartitionData(tbl.Schema(), tbl.Spec(), row)
			if err != nil {
				return nil, err
			}
			groupKey := stableMapKey(partitionData)
			group := groupsByKey[groupKey]
			if group == nil {
				group = &partitionDeleteGroup{
					partitionKey:  groupKey,
					partitionData: partitionData,
				}
				groupsByKey[groupKey] = group
			}
			group.keys = append(group.keys, del.key)
		}
	}

	groups := make([]partitionDeleteGroup, 0, len(groupsByKey))
	for _, group := range groupsByKey {
		groups = append(groups, *group)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].partitionKey < groups[j].partitionKey
	})
	return groups, nil
}

func equalityDeletePartitionData(schema *iceberglib.Schema, spec iceberglib.PartitionSpec, row map[string]interface{}) (map[int]any, error) {
	out := make(map[int]any, spec.NumFields())
	for _, field := range spec.Fields() {
		sourceField, ok := schema.FindFieldByID(field.SourceID())
		if !ok {
			return nil, fmt.Errorf("partition field %s source id %d not found in iceberg schema", field.Name, field.SourceID())
		}
		sourceValue, ok := lookupColumnValue(row, sourceField.Name)
		if !ok {
			return nil, fmt.Errorf("partitioned equality delete requires source column %s for partition field %s", sourceField.Name, field.Name)
		}
		value, err := applyPartitionTransform(field.Transform, sourceField.Type, sourceValue)
		if err != nil {
			return nil, fmt.Errorf("partition field %s: %w", field.Name, err)
		}
		out[field.FieldID] = value
	}
	return out, nil
}

func applyPartitionTransform(transform iceberglib.Transform, sourceType iceberglib.Type, value interface{}) (any, error) {
	if value == nil {
		return nil, nil
	}
	lit, err := partitionSourceLiteral(sourceType, value)
	if err != nil {
		return nil, err
	}

	switch transform.(type) {
	case iceberglib.IdentityTransform:
		return lit.Any(), nil
	case iceberglib.YearTransform, iceberglib.MonthTransform, iceberglib.DayTransform, iceberglib.HourTransform,
		iceberglib.BucketTransform, iceberglib.TruncateTransform:
		applied := transform.Apply(iceberglib.Optional[iceberglib.Literal]{Valid: true, Val: lit})
		if !applied.Valid {
			return nil, nil
		}
		return applied.Val.Any(), nil
	case iceberglib.VoidTransform:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported partition transform %s for native equality delete", transform)
	}
}

func partitionSourceLiteral(sourceType iceberglib.Type, value interface{}) (iceberglib.Literal, error) {
	if _, ok := sourceType.(iceberglib.DateType); ok {
		tm, err := toTime(value)
		if err != nil {
			return nil, err
		}
		return iceberglib.NewLiteral(iceberglib.Date(tm.UTC().Truncate(24*time.Hour).Unix() / int64((24 * time.Hour).Seconds()))), nil
	}
	if _, ok := sourceType.(iceberglib.TimestampType); ok {
		tm, err := toTime(value)
		if err != nil {
			return nil, err
		}
		return iceberglib.NewLiteral(iceberglib.Timestamp(tm.UTC().UnixMicro())), nil
	}
	if _, ok := sourceType.(iceberglib.TimestampTzType); ok {
		tm, err := toTime(value)
		if err != nil {
			return nil, err
		}
		return iceberglib.NewLiteral(iceberglib.Timestamp(tm.UTC().UnixMicro())), nil
	}
	if _, ok := sourceType.(iceberglib.TimestampNsType); ok {
		tm, err := toTime(value)
		if err != nil {
			return nil, err
		}
		return iceberglib.NewLiteral(iceberglib.TimestampNano(tm.UTC().UnixNano())), nil
	}
	if _, ok := sourceType.(iceberglib.TimestampTzNsType); ok {
		tm, err := toTime(value)
		if err != nil {
			return nil, err
		}
		return iceberglib.NewLiteral(iceberglib.TimestampNano(tm.UTC().UnixNano())), nil
	}

	lit, err := literalForValue(sourceType, value)
	if err != nil {
		return nil, err
	}
	return lit, nil
}

func stableMapKey(m map[int]any) string {
	keys := make([]int, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		encoded, err := json.Marshal(m[key])
		if err != nil {
			encoded = []byte(fmt.Sprint(m[key]))
		}
		parts = append(parts, fmt.Sprintf("%d=%s", key, encoded))
	}
	return strings.Join(parts, "|")
}

func stableStringMapKey(m map[string]interface{}) string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		encoded, err := json.Marshal(m[key])
		if err != nil {
			encoded = []byte(fmt.Sprint(m[key]))
		}
		parts = append(parts, fmt.Sprintf("%s=%s", strings.ToLower(key), encoded))
	}
	return strings.Join(parts, "|")
}

func writeEqualityDeleteFile(ctx context.Context, tbl *icetable.Table, fs iceio.WriteFileIO, keys []map[string]interface{}, pkCols []string, partitionData map[int]any) (iceberglib.DataFile, error) {
	deleteSchema, equalityFieldIDs, err := equalityDeleteSchema(tbl.Schema(), pkCols)
	if err != nil {
		return nil, err
	}
	record, release, err := buildEqualityDeleteRecord(deleteSchema, keys, pkCols)
	if err != nil {
		return nil, err
	}
	defer release()

	arrowTable := array.NewTableFromRecords(record.Schema(), []arrow.RecordBatch{record})
	defer arrowTable.Release()

	locProvider, err := tbl.LocationProvider()
	if err != nil {
		return nil, err
	}
	filePath := locProvider.NewDataLocation(fmt.Sprintf("rivus-eq-delete-%s.parquet", uuid.NewString()))
	out, err := fs.Create(filePath)
	if err != nil {
		return nil, err
	}
	complete := false
	defer func() {
		if !complete {
			if removeErr := fs.Remove(filePath); removeErr != nil {
				log.Printf("[iceberg] failed to remove incomplete equality-delete file=%q: %v", filePath, removeErr)
			}
		}
	}()
	counter := &countingWriteCloser{w: out}
	if err := pqarrow.WriteTable(arrowTable, counter, arrowTable.NumRows(), parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()); err != nil {
		_ = counter.Close()
		return nil, err
	}
	if err := counter.Close(); err != nil {
		return nil, err
	}

	builder, err := iceberglib.NewDataFileBuilder(tbl.Spec(), iceberglib.EntryContentEqDeletes, filePath, iceberglib.ParquetFile, partitionData, nil, nil, int64(len(keys)), counter.n)
	if err != nil {
		return nil, err
	}
	deleteFile := builder.EqualityFieldIDs(equalityFieldIDs).Build()
	complete = true
	return deleteFile, nil
}

func equalityDeleteSchema(schema *iceberglib.Schema, pkCols []string) (*iceberglib.Schema, []int, error) {
	if schema == nil {
		return nil, nil, fmt.Errorf("iceberg schema is nil")
	}
	fields := make([]iceberglib.NestedField, 0, len(pkCols))
	ids := make([]int, 0, len(pkCols))
	for _, col := range pkCols {
		field, ok := schema.FindFieldByNameCaseInsensitive(col)
		if !ok {
			return nil, nil, fmt.Errorf("field %s not found in iceberg schema", col)
		}
		fields = append(fields, field)
		ids = append(ids, field.ID)
	}
	return iceberglib.NewSchemaWithIdentifiers(schema.ID, ids, fields...), ids, nil
}

func buildEqualityDeleteRecord(schema *iceberglib.Schema, keys []map[string]interface{}, pkCols []string) (arrow.RecordBatch, func(), error) {
	arrSchema, err := icetable.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, nil, err
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrSchema)
	release := func() { bldr.Release() }
	for _, key := range keys {
		for idx, col := range pkCols {
			value, _ := lookupColumnValue(key, col)
			if err := appendBuilderValue(bldr.Field(idx), arrSchema.Field(idx).Type, value); err != nil {
				release()
				return nil, nil, fmt.Errorf("column %s: %w", col, err)
			}
		}
	}

	batch := bldr.NewRecordBatch()
	return batch, func() {
		batch.Release()
		release()
	}, nil
}
