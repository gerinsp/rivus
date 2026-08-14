package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberglib "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/util"
)

const snapshotSpoolReadBatchRows int64 = 1000

type snapshotSpool struct {
	path       string
	file       *os.File
	writer     *ipc.FileWriter
	schema     *arrow.Schema
	rowCount   int64
	batchCount int
	bytes      int64
}

func snapshotRollingEnabled(cfg config.IcebergConfig) bool {
	return cfg.SnapshotRollingEnabled == nil || *cfg.SnapshotRollingEnabled
}

func supportsRollingSnapshotMode(mode string) bool {
	switch mode {
	case snapshotWriteModeAppend, snapshotWriteModeReplaceFilterAppend, snapshotWriteModeTruncateAppend:
		return true
	default:
		return false
	}
}

func prepareSnapshotSpoolDirectory(base, jobID, stateKey string) (string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		base = filepath.Join(os.TempDir(), "rivus-snapshot-spool")
	}
	absBase, err := filepath.Abs(base)
	if err != nil {
		return "", fmt.Errorf("resolve iceberg snapshot spool directory: %w", err)
	}
	absBase = filepath.Clean(absBase)
	if absBase == string(filepath.Separator) {
		return "", fmt.Errorf("iceberg snapshot_spool_directory cannot be the filesystem root")
	}
	digest := sha256.Sum256([]byte(jobID + "\x00" + stateKey))
	jobDir := filepath.Join(absBase, "job-"+hex.EncodeToString(digest[:8]))
	if err := os.RemoveAll(jobDir); err != nil {
		return "", fmt.Errorf("clean stale iceberg snapshot spool %q: %w", jobDir, err)
	}
	if err := os.MkdirAll(jobDir, 0o700); err != nil {
		return "", fmt.Errorf("create iceberg snapshot spool %q: %w", jobDir, err)
	}
	return jobDir, nil
}

func (s *Sink) appendSnapshotSpool(ctx context.Context, state *tableState, rows []map[string]interface{}, ts time.Time, snapshotStartOffset int64) error {
	if state == nil || state.table == nil || state.sourceSchema == nil {
		return util.Permanent(fmt.Errorf("snapshot spool table state is incomplete"))
	}
	mode := s.snapshotWriteModeForTableState(state)
	switch mode {
	case snapshotWriteModeReplaceFilterAppend:
		if err := s.ensureSnapshotReplaceFilterApplied(ctx, state, snapshotStartOffset); err != nil {
			return err
		}
	case snapshotWriteModeTruncateAppend:
		if err := s.ensureSnapshotTruncateApplied(ctx, state, snapshotStartOffset); err != nil {
			return err
		}
	case snapshotWriteModeAppend:
	default:
		return util.Permanent(fmt.Errorf("snapshot rolling spool does not support write mode %q", mode))
	}
	if state.snapshotSpool == nil {
		if err := s.ensureSnapshotRollingWriteProperties(ctx, state); err != nil {
			return err
		}
	}

	pkCols, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
	if err != nil {
		return util.Permanent(err)
	}
	pendingRows := make([]pendingRow, 0, len(rows))
	for idx, row := range rows {
		key, _, err := keyFromRow(row, pkCols)
		if err != nil {
			return util.Permanent(err)
		}
		pendingRows = append(pendingRows, pendingRow{
			key: key,
			row: cloneMap(row),
			event: model.Event{
				Type:      model.EventTypeInsert,
				Schema:    state.sourceSchema.SchemaName,
				Table:     state.sourceSchema.TableName,
				Timestamp: ts,
				Origin:    model.EventOriginSnapshot,
			},
			pos: idx,
		})
	}
	enrichedRows, err := s.enrichPendingRows(ctx, state, pendingRows, pkCols)
	if err != nil {
		return util.Permanent(err)
	}
	if len(enrichedRows) == 0 {
		return nil
	}

	reader, release, err := buildRecordReader(state.table.Schema(), enrichedRows)
	if err != nil {
		return util.Permanent(err)
	}
	defer release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return err
		}
		return nil
	}

	spool, err := s.ensureSnapshotSpool(state, reader.Schema())
	if err != nil {
		return err
	}
	if err := spool.writer.Write(reader.RecordBatch()); err != nil {
		return fmt.Errorf("write iceberg snapshot spool for %s: %w", state.sourceKey, err)
	}
	if err := spool.file.Sync(); err != nil {
		return fmt.Errorf("sync iceberg snapshot spool for %s: %w", state.sourceKey, err)
	}
	info, err := spool.file.Stat()
	if err != nil {
		return fmt.Errorf("stat iceberg snapshot spool for %s: %w", state.sourceKey, err)
	}
	spool.rowCount += int64(len(enrichedRows))
	spool.batchCount++
	spool.bytes = info.Size()
	state.lastTouchedAt = time.Now()
	if s.cfg.SnapshotSpoolMaxBytes > 0 && spool.bytes > s.cfg.SnapshotSpoolMaxBytes {
		return util.Permanent(fmt.Errorf("iceberg snapshot spool for %s reached %d bytes, above snapshot_spool_max_bytes=%d", state.sourceKey, spool.bytes, s.cfg.SnapshotSpoolMaxBytes))
	}
	log.Printf("[iceberg][job %s] snapshot spooled table=%s rows=%d batches=%d spool_bytes=%d",
		s.jobID, state.sourceKey, spool.rowCount, spool.batchCount, spool.bytes)
	return nil
}

func (s *Sink) ensureSnapshotRollingWriteProperties(ctx context.Context, state *tableState) error {
	desired := s.cfg.SnapshotParquetRowGroupRows
	if desired <= 0 || state == nil || state.table == nil {
		return nil
	}
	current := state.table.Metadata().Properties().GetInt(icetable.ParquetRowGroupLimitKey, icetable.ParquetRowGroupLimitDefault)
	if current <= desired {
		return nil
	}
	var updated *icetable.Table
	err := s.withCommitSlot(ctx, commitProgress{
		operation:       "snapshot-rolling-properties",
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
	}, func() error {
		txn := state.table.NewTransaction()
		if err := txn.SetProperties(iceberglib.Properties{
			icetable.ParquetRowGroupLimitKey: strconv.Itoa(desired),
		}); err != nil {
			return err
		}
		var err error
		updated, err = txn.Commit(ctx)
		return err
	})
	if err != nil {
		return s.stateOperationError("snapshot-rolling-properties", state, err)
	}
	s.mu.Lock()
	state.table = updated
	s.updateTargetTableStatesLocked(state.targetNamespace, state.targetTable, updated, time.Now())
	s.mu.Unlock()
	log.Printf("[iceberg][job %s] snapshot rolling row group table=%s rows=%d", s.jobID, state.sourceKey, desired)
	return nil
}

func (s *Sink) ensureSnapshotSpool(state *tableState, schema *arrow.Schema) (*snapshotSpool, error) {
	if state.snapshotSpool != nil {
		if !state.snapshotSpool.schema.Equal(schema) {
			return nil, fmt.Errorf("snapshot spool schema changed for %s", state.sourceKey)
		}
		return state.snapshotSpool, nil
	}
	if strings.TrimSpace(s.snapshotSpoolDir) == "" {
		return nil, fmt.Errorf("iceberg snapshot spool directory is not initialized")
	}
	digest := sha256.Sum256([]byte(state.sourceKey))
	file, err := os.CreateTemp(s.snapshotSpoolDir, "table-"+hex.EncodeToString(digest[:6])+"-*.arrow")
	if err != nil {
		return nil, fmt.Errorf("create iceberg snapshot spool for %s: %w", state.sourceKey, err)
	}
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		file.Close()
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("open iceberg snapshot spool writer for %s: %w", state.sourceKey, err)
	}
	state.snapshotSpool = &snapshotSpool{
		path:   file.Name(),
		file:   file,
		writer: writer,
		schema: schema,
	}
	return state.snapshotSpool, nil
}

func (s *Sink) handleSnapshotTableComplete(ctx context.Context, ev model.Event) (err error) {
	if ev.Ack != nil {
		defer func() {
			ev.Ack <- err
			close(ev.Ack)
		}()
	}
	if !ev.SnapshotRolling || !snapshotRollingEnabled(s.cfg) {
		return nil
	}
	state := s.states[tableKey(ev.Schema, ev.Table)]
	if state == nil || state.snapshotSpool == nil {
		return nil
	}
	if state.table == nil {
		state, err = s.ensureOperationalState(ctx, tableKey(ev.Schema, ev.Table))
		if err != nil {
			return err
		}
	}
	return s.finalizeSnapshotSpool(ctx, state)
}

func (s *Sink) finalizeSnapshotSpool(ctx context.Context, state *tableState) error {
	spool := state.snapshotSpool
	state.snapshotSpool = nil
	if spool == nil {
		return nil
	}
	defer func() {
		_ = os.Remove(spool.path)
	}()
	if err := spool.writer.Close(); err != nil {
		_ = spool.file.Close()
		return fmt.Errorf("close iceberg snapshot spool writer for %s: %w", state.sourceKey, err)
	}
	if err := spool.file.Sync(); err != nil {
		_ = spool.file.Close()
		return fmt.Errorf("sync completed iceberg snapshot spool for %s: %w", state.sourceKey, err)
	}
	if err := spool.file.Close(); err != nil {
		return fmt.Errorf("close iceberg snapshot spool file for %s: %w", state.sourceKey, err)
	}

	result := flushResult{operation: "snapshot-rolling-append", rowCount: int(spool.rowCount)}
	var updated *icetable.Table
	var startedAt time.Time
	var duration time.Duration
	err := s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
		rowCount:        result.rowCount,
	}, func() error {
		startedAt = time.Now()
		var commitErr error
		updated, commitErr = s.commitSnapshotSpool(ctx, state, spool)
		duration = time.Since(startedAt)
		return commitErr
	})
	s.logWriteTiming(state, result, err, startedAt, duration)
	if err != nil {
		return s.stateOperationError(result.operation, state, err)
	}
	s.updateStateTableAfterWrite(state, updated)
	log.Printf("[iceberg][job %s] rolled snapshot committed table=%s rows=%d source_batches=%d target_file_bytes=%d",
		s.jobID, state.sourceKey, spool.rowCount, spool.batchCount, s.cfg.SnapshotTargetFileSizeBytes)
	return nil
}

func (s *Sink) commitSnapshotSpool(ctx context.Context, state *tableState, spool *snapshotSpool) (*icetable.Table, error) {
	file, err := os.Open(spool.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader, err := ipc.NewFileReader(file, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		for recordIndex := 0; recordIndex < reader.NumRecords(); recordIndex++ {
			record, readErr := reader.RecordBatch(recordIndex)
			if readErr != nil {
				yield(nil, readErr)
				return
			}
			for start := int64(0); start < record.NumRows(); start += snapshotSpoolReadBatchRows {
				end := start + snapshotSpoolReadBatchRows
				if end > record.NumRows() {
					end = record.NumRows()
				}
				slice := record.NewSlice(start, end)
				if !yield(slice, nil) {
					slice.Release()
					record.Release()
					return
				}
			}
			record.Release()
		}
	}

	fs, err := state.table.FS(ctx)
	if err != nil {
		return nil, err
	}
	writeFS, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("iceberg filesystem does not support snapshot spool writes")
	}
	stagedFiles := make([]iceberglib.DataFile, 0)
	cleanupStaged := true
	defer func() {
		if cleanupStaged {
			removeStagedIcebergFiles(writeFS, stagedFiles)
		}
	}()
	for dataFile, writeErr := range icetable.WriteRecords(
		ctx,
		state.table,
		reader.Schema(),
		records,
		icetable.WithTargetFileSize(s.cfg.SnapshotTargetFileSizeBytes),
		icetable.WithMaxWriteWorkers(1),
	) {
		if writeErr != nil {
			return nil, writeErr
		}
		stagedFiles = append(stagedFiles, dataFile)
	}
	if len(stagedFiles) == 0 {
		return nil, fmt.Errorf("rolled snapshot built no data files for %s", state.sourceKey)
	}

	txn := state.table.NewTransaction()
	delta := txn.NewRowDelta(s.snapshotProps(state))
	delta.AddRows(stagedFiles...)
	if err := delta.Commit(ctx); err != nil {
		return nil, err
	}
	// Once the catalog commit starts, its outcome may be unknown to this
	// process (for example, a timeout after the server accepted it). Keep the
	// staged objects in that case: deleting them could corrupt a successful
	// snapshot. Normal orphan-file maintenance can safely remove them later if
	// the catalog did not accept the commit.
	cleanupStaged = false
	return txn.Commit(ctx)
}

func (s *Sink) resetSnapshotSpool(state *tableState) {
	if state == nil || state.snapshotSpool == nil {
		return
	}
	spool := state.snapshotSpool
	state.snapshotSpool = nil
	_ = spool.writer.Close()
	_ = spool.file.Close()
	_ = os.Remove(spool.path)
}

func (s *Sink) cleanupSnapshotSpools() {
	if s == nil {
		return
	}
	for _, state := range s.states {
		s.resetSnapshotSpool(state)
	}
}

func (s *Sink) cleanupSnapshotSpoolDirectory() {
	if s == nil {
		return
	}
	s.cleanupSnapshotSpools()
	if strings.TrimSpace(s.snapshotSpoolDir) != "" {
		_ = os.RemoveAll(s.snapshotSpoolDir)
	}
}
