package iceberg

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
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
	w io.WriteCloser
	n int64
}

type cdcEqualityDeleter interface {
	DeleteEquality(ctx context.Context, state *tableState, keys []map[string]interface{}, deleteRows []pendingDelete, pkCols []string) (*icetable.Table, error)
}

type rivusEqualityDeleter struct {
	sink *Sink
}

func (d rivusEqualityDeleter) DeleteEquality(ctx context.Context, state *tableState, keys []map[string]interface{}, deleteRows []pendingDelete, pkCols []string) (*icetable.Table, error) {
	return d.sink.commitEqualityDelete(ctx, state, keys, deleteRows, pkCols)
}

func (w *countingWriteCloser) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *countingWriteCloser) Close() error {
	return w.w.Close()
}

func (s *Sink) applyKeyDeleteWithEqualityDeletes(ctx context.Context, state *tableState, batch *reducedBatch, operation string) error {
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
		deleter := s.equalityDeleter
		if deleter == nil {
			deleter = rivusEqualityDeleter{sink: s}
		}
		updated, err = deleter.DeleteEquality(ctx, state, batch.deleteKeys, batch.deleteRows, batch.pkCols)
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

func (s *Sink) commitEqualityDelete(ctx context.Context, state *tableState, keys []map[string]interface{}, deleteRows []pendingDelete, pkCols []string) (*icetable.Table, error) {
	if state == nil || state.table == nil {
		return nil, fmt.Errorf("table handle is nil for equality delete")
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

	deleteFiles, err := writeEqualityDeleteFiles(ctx, tbl, writeFS, keys, deleteRows, pkCols)
	if err != nil {
		return nil, err
	}
	if len(deleteFiles) == 0 {
		return nil, fmt.Errorf("equality delete built no delete files for %s", state.sourceKey)
	}

	snapshotID, err := newIcebergSnapshotID(meta)
	if err != nil {
		return nil, err
	}
	sequenceNumber := meta.LastSequenceNumber() + 1
	parentSnapshotID := (*int64)(nil)
	currentSnapshotID := (*int64)(nil)
	if current := tbl.CurrentSnapshot(); current != nil {
		parent := current.SnapshotID
		parentSnapshotID = &parent
		currentID := current.SnapshotID
		currentSnapshotID = &currentID
	}

	locProvider, err := tbl.LocationProvider()
	if err != nil {
		return nil, err
	}
	commitUUID := uuid.NewString()
	manifestPath := locProvider.NewMetadataLocation(fmt.Sprintf("%s-m0.avro", commitUUID))
	manifest, err := writeEqualityDeleteManifest(writeFS, manifestPath, meta.Version(), tbl.Spec(), tbl.Schema(), snapshotID, deleteFiles)
	if err != nil {
		return nil, err
	}

	manifests := []iceberglib.ManifestFile{manifest}
	if current := tbl.CurrentSnapshot(); current != nil {
		parentManifests, err := current.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("read current snapshot manifests before equality delete commit: %w", err)
		}
		manifests = append(parentManifests, manifest)
	}

	firstRowID := int64(0)
	if meta.Version() == 3 {
		firstRowID = meta.NextRowID()
	}
	manifestListPath := locProvider.NewMetadataLocation(fmt.Sprintf("snap-%d-0-%s.avro", snapshotID, commitUUID))
	if err := writeEqualityDeleteManifestList(writeFS, manifestListPath, meta.Version(), snapshotID, parentSnapshotID, sequenceNumber, firstRowID, manifests); err != nil {
		return nil, err
	}

	summary := equalityDeleteSummary(tbl.CurrentSnapshot(), deleteFiles, s.snapshotProps(state))
	snapshot := icetable.Snapshot{
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapshotID,
		SequenceNumber:   sequenceNumber,
		ManifestList:     manifestListPath,
		Summary:          &summary,
		SchemaID:         &tbl.Schema().ID,
		TimestampMs:      time.Now().UnixMilli(),
	}
	if meta.Version() == 3 {
		addedRows := int64(0)
		snapshot.FirstRowID = &firstRowID
		snapshot.AddedRows = &addedRows
	}
	maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep := snapshotRefRetention(meta.Ref())

	updates := []icetable.Update{
		icetable.NewAddSnapshotUpdate(&snapshot),
		icetable.NewSetSnapshotRefUpdate(icetable.MainBranch, snapshotID, icetable.BranchRef, maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep),
	}
	requirements := []icetable.Requirement{
		icetable.AssertRefSnapshotID(icetable.MainBranch, currentSnapshotID),
	}

	newMeta, newLoc, err := s.catalog.CommitTable(ctx, tbl.Identifier(), requirements, updates)
	if err != nil {
		return nil, err
	}
	return icetable.New(tbl.Identifier(), newMeta, newLoc, tbl.FS, s.catalog), nil
}

func snapshotRefRetention(ref icetable.SnapshotRef) (maxRefAgeMs, maxSnapshotAgeMs int64, minSnapshotsToKeep int) {
	if ref.MaxRefAgeMs != nil && *ref.MaxRefAgeMs > 0 {
		maxRefAgeMs = *ref.MaxRefAgeMs
	}
	if ref.MaxSnapshotAgeMs != nil && *ref.MaxSnapshotAgeMs > 0 {
		maxSnapshotAgeMs = *ref.MaxSnapshotAgeMs
	}
	if ref.MinSnapshotsToKeep != nil && *ref.MinSnapshotsToKeep > 0 {
		minSnapshotsToKeep = *ref.MinSnapshotsToKeep
	}
	return maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep
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
	for _, group := range groups {
		deleteFile, err := writeEqualityDeleteFile(ctx, tbl, fs, group.keys, pkCols, group.partitionData)
		if err != nil {
			return nil, err
		}
		deleteFiles = append(deleteFiles, deleteFile)
	}
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
	return builder.EqualityFieldIDs(equalityFieldIDs).Build(), nil
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

func writeEqualityDeleteManifest(fs iceio.WriteFileIO, path string, formatVersion int, spec iceberglib.PartitionSpec, schema *iceberglib.Schema, snapshotID int64, deleteFiles []iceberglib.DataFile) (iceberglib.ManifestFile, error) {
	out, err := fs.Create(path)
	if err != nil {
		return nil, err
	}
	counter := &countingWriteCloser{w: out}
	writer, err := iceberglib.NewManifestWriter(formatVersion, counter, spec, schema, snapshotID, iceberglib.WithManifestWriterContent(iceberglib.ManifestContentDeletes))
	if err != nil {
		_ = counter.Close()
		return nil, err
	}
	for _, deleteFile := range deleteFiles {
		entry := iceberglib.NewManifestEntry(iceberglib.EntryStatusADDED, &snapshotID, nil, nil, deleteFile)
		if err := writer.Add(entry); err != nil {
			_ = writer.Close()
			_ = counter.Close()
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		_ = counter.Close()
		return nil, err
	}
	if err := counter.Close(); err != nil {
		return nil, err
	}
	return writer.ToManifestFile(path, counter.n, iceberglib.WithManifestFileContent(iceberglib.ManifestContentDeletes))
}

func writeEqualityDeleteManifestList(fs iceio.WriteFileIO, path string, formatVersion int, snapshotID int64, parentSnapshotID *int64, sequenceNumber int64, firstRowID int64, manifests []iceberglib.ManifestFile) error {
	out, err := fs.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()
	return iceberglib.WriteManifestList(formatVersion, out, snapshotID, parentSnapshotID, &sequenceNumber, firstRowID, manifests)
}

func equalityDeleteSummary(previous *icetable.Snapshot, deleteFiles []iceberglib.DataFile, props iceberglib.Properties) icetable.Summary {
	summaryProps := iceberglib.Properties{}
	if previous != nil && previous.Summary != nil {
		for key, value := range previous.Summary.Properties {
			if len(key) >= len("total-") && key[:len("total-")] == "total-" {
				summaryProps[key] = value
			}
		}
	}
	var addedDeletes int64
	var addedFileSize int64
	for _, deleteFile := range deleteFiles {
		addedDeletes += deleteFile.Count()
		addedFileSize += deleteFile.FileSizeBytes()
	}
	addedFiles := int64(len(deleteFiles))
	setSummaryInt(summaryProps, "added-delete-files", addedFiles)
	setSummaryInt(summaryProps, "added-equality-delete-files", addedFiles)
	setSummaryInt(summaryProps, "added-equality-deletes", addedDeletes)
	setSummaryInt(summaryProps, "added-files-size", addedFileSize)
	setSummaryInt(summaryProps, "total-delete-files", summaryInt(summaryProps, "total-delete-files")+addedFiles)
	setSummaryInt(summaryProps, "total-equality-deletes", summaryInt(summaryProps, "total-equality-deletes")+addedDeletes)
	setSummaryInt(summaryProps, "total-files-size", summaryInt(summaryProps, "total-files-size")+addedFileSize)
	if _, ok := summaryProps["total-data-files"]; !ok {
		setSummaryInt(summaryProps, "total-data-files", 0)
	}
	if _, ok := summaryProps["total-records"]; !ok {
		setSummaryInt(summaryProps, "total-records", 0)
	}
	if _, ok := summaryProps["total-position-deletes"]; !ok {
		setSummaryInt(summaryProps, "total-position-deletes", 0)
	}
	for key, value := range props {
		summaryProps[key] = value
	}
	return icetable.Summary{Operation: icetable.OpDelete, Properties: summaryProps}
}

func summaryInt(props iceberglib.Properties, key string) int64 {
	value, _ := strconv.ParseInt(props[key], 10, 64)
	return value
}

func setSummaryInt(props iceberglib.Properties, key string, value int64) {
	props[key] = strconv.FormatInt(value, 10)
}

func newIcebergSnapshotID(meta icetable.Metadata) (int64, error) {
	for attempts := 0; attempts < 10; attempts++ {
		n, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			return 0, err
		}
		id := n.Int64() + 1
		if meta.SnapshotByID(id) == nil {
			return id, nil
		}
	}
	return 0, fmt.Errorf("could not generate unique iceberg snapshot id")
}
