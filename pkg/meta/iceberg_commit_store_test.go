package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type commitIntentSQLState struct {
	intents      map[string]IcebergCommitIntent
	ddlSeen      bool
	jobIndexSeen bool
}

type commitIntentSQLConnector struct{ state *commitIntentSQLState }

func (c commitIntentSQLConnector) Connect(context.Context) (driver.Conn, error) {
	return commitIntentSQLConn{state: c.state}, nil
}

func (commitIntentSQLConnector) Driver() driver.Driver { return commitIntentSQLDriver{} }

type commitIntentSQLDriver struct{}

func (commitIntentSQLDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type commitIntentSQLConn struct{ state *commitIntentSQLState }

func (commitIntentSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}

func (commitIntentSQLConn) Close() error { return nil }

func (commitIntentSQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}

func (c commitIntentSQLConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch {
	case query == "CREATE INDEX idx_iceberg_commit_job ON iceberg_commit_intents (job_id)":
		c.state.jobIndexSeen = true
		return driver.RowsAffected(0), nil
	case strings.Contains(query, "CREATE TABLE IF NOT EXISTS iceberg_commit_intents"):
		c.state.ddlSeen = true
		return driver.RowsAffected(0), nil
	case strings.Contains(query, "CREATE TABLE IF NOT EXISTS"), strings.Contains(query, "ALTER TABLE"):
		return driver.RowsAffected(0), nil
	case strings.Contains(query, "INSERT INTO iceberg_commit_intents"):
		intent := IcebergCommitIntent{
			OperationID: args[0].Value.(string), JobID: args[1].Value.(string), StateKey: args[2].Value.(string),
			BatchID: args[3].Value.(string), Kind: args[4].Value.(string),
			SourceStartFile: args[5].Value.(string), SourceStartPos: uint32(args[6].Value.(int64)),
			SourceEndFile: args[7].Value.(string), SourceEndPos: uint32(args[8].Value.(int64)),
			SnapshotTable: args[9].Value.(string), SnapshotStartOffset: args[10].Value.(int64),
			SnapshotEndOffset: args[11].Value.(int64), Status: IcebergCommitPrepared,
			SourceTable: args[12].Value.(string), TargetNamespace: args[13].Value.(string), TargetTable: args[14].Value.(string), Operation: args[15].Value.(string), ReplayKeys: args[16].Value.(string),
		}
		c.state.intents[intent.OperationID] = intent
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO job_snapshot_progress"):
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "UPDATE iceberg_commit_intents"):
		intent, ok := c.state.intents[args[1].Value.(string)]
		if !ok || intent.StateKey != args[0].Value.(string) || intent.Status != IcebergCommitPrepared {
			return driver.RowsAffected(0), nil
		}
		intent.Status = IcebergCommitCommitted
		c.state.intents[intent.OperationID] = intent
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "DELETE FROM iceberg_commit_intents"):
		if len(args) == 3 {
			count := int64(0)
			for id, intent := range c.state.intents {
				if intent.StateKey == args[0].Value.(string) && intent.Kind == "snapshot" && intent.Status == IcebergCommitCommitted && intent.SnapshotTable == args[1].Value.(string) && intent.SnapshotEndOffset <= args[2].Value.(int64) {
					delete(c.state.intents, id)
					count++
				}
			}
			return driver.RowsAffected(count), nil
		}
		delete(c.state.intents, args[1].Value.(string))
		return driver.RowsAffected(1), nil
	default:
		return nil, errors.New("unexpected SQL: " + query)
	}
}

func (c commitIntentSQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "FROM iceberg_commit_intents") {
		return nil, errors.New("unexpected SQL: " + query)
	}
	var values [][]driver.Value
	for _, intent := range c.state.intents {
		if intent.StateKey != args[0].Value.(string) {
			continue
		}
		values = append(values, []driver.Value{intent.OperationID, intent.JobID, intent.StateKey, intent.BatchID,
			intent.Kind, intent.SourceStartFile, int64(intent.SourceStartPos), intent.SourceEndFile,
			int64(intent.SourceEndPos), intent.SnapshotTable, intent.SnapshotStartOffset,
			intent.SnapshotEndOffset, intent.Status, intent.SourceTable, intent.TargetNamespace, intent.TargetTable, intent.Operation, intent.ReplayKeys})
	}
	return &commitIntentSQLRows{values: values}, nil
}

type commitIntentSQLRows struct{ values [][]driver.Value }

func (r *commitIntentSQLRows) Columns() []string {
	return []string{"operation_id", "job_id", "state_key", "batch_id", "kind", "source_start_file", "source_start_pos", "source_end_file", "source_end_pos", "snapshot_table", "snapshot_start_offset", "snapshot_end_offset", "status", "source_table", "target_namespace", "target_table", "operation", "replay_keys"}
}

func (r *commitIntentSQLRows) Close() error { return nil }

func (r *commitIntentSQLRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}

func TestIcebergCommitIntentSQLLifecycle(t *testing.T) {
	state := &commitIntentSQLState{intents: make(map[string]IcebergCommitIntent)}
	db := sql.OpenDB(commitIntentSQLConnector{state: state})
	defer db.Close()
	store := &MySQLOffsetStore{db: db}
	ctx := context.Background()
	if err := store.Init(ctx); err != nil || !state.ddlSeen || !state.jobIndexSeen {
		t.Fatalf("Init=(%v, ddlSeen=%t, jobIndexSeen=%t), want intent table and public job index", err, state.ddlSeen, state.jobIndexSeen)
	}
	intent := IcebergCommitIntent{
		OperationID: "f77879a4-f54c-455c-a14c-8b617e5aca12", JobID: "job", StateKey: "state", BatchID: "batch-42", Kind: "cdc",
		SourceStartFile: "mysql-bin.000001", SourceStartPos: 40, SourceEndFile: "mysql-bin.000001", SourceEndPos: 42,
	}
	if err := store.BeginIcebergCommit(ctx, intent); err != nil {
		t.Fatalf("BeginIcebergCommit: %v", err)
	}
	otherStore := &MySQLOffsetStore{db: db}
	got, err := otherStore.ListIcebergCommits(ctx, "state")
	if err != nil || len(got) != 1 || got[0].BatchID != intent.BatchID || got[0].SourceEndPos != 42 || got[0].Status != IcebergCommitPrepared {
		t.Fatalf("read prepared intent=(%+v, %v)", got, err)
	}
	if err := otherStore.MarkIcebergCommitCommitted(ctx, "state", intent.OperationID); err != nil {
		t.Fatalf("MarkIcebergCommitCommitted: %v", err)
	}
	got, err = store.ListIcebergCommits(ctx, "state")
	if err != nil || len(got) != 1 || got[0].Status != IcebergCommitCommitted {
		t.Fatalf("read committed intent=(%+v, %v)", got, err)
	}
	if err := store.DeleteIcebergCommit(ctx, "state", intent.OperationID); err != nil {
		t.Fatalf("DeleteIcebergCommit: %v", err)
	}
	got, err = otherStore.ListIcebergCommits(ctx, "state")
	if err != nil || len(got) != 0 {
		t.Fatalf("read cleared intents=(%+v, %v)", got, err)
	}
	snapshotIntent := intent
	snapshotIntent.OperationID = "73e46c52-49cf-4922-9a3e-1e8d0752ed62"
	snapshotIntent.Kind = "snapshot"
	snapshotIntent.SnapshotTable = "app.orders"
	snapshotIntent.SnapshotStartOffset = 0
	snapshotIntent.SnapshotEndOffset = 10
	if err := store.BeginIcebergCommit(ctx, snapshotIntent); err != nil {
		t.Fatalf("begin snapshot intent: %v", err)
	}
	if err := store.MarkIcebergCommitCommitted(ctx, "state", snapshotIntent.OperationID); err != nil {
		t.Fatalf("mark snapshot committed: %v", err)
	}
	if err := store.SaveSnapshotProgress(ctx, "state", "app.orders", 9, ""); err != nil {
		t.Fatalf("save progress before end: %v", err)
	}
	got, err = store.ListIcebergCommits(ctx, "state")
	if err != nil || len(got) != 1 {
		t.Fatalf("intent was cleared before snapshot progress covered it: intents=%+v err=%v", got, err)
	}
	if err := store.SaveSnapshotProgress(ctx, "state", "app.orders", 10, ""); err != nil {
		t.Fatalf("save progress at end: %v", err)
	}
	got, err = store.ListIcebergCommits(ctx, "state")
	if err != nil || len(got) != 0 {
		t.Fatalf("checkpointed snapshot intent remains: intents=%+v err=%v", got, err)
	}
}
