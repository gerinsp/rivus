package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type completionSQL struct {
	exists, done bool
	version      int64
	execErr      error
	prepared     bool
	queries      []string
}
type completionConnector struct{ state *completionSQL }

func (c completionConnector) Connect(context.Context) (driver.Conn, error) {
	return completionConn{c.state}, nil
}
func (completionConnector) Driver() driver.Driver { return deleteFailureDriver{} }

type completionConn struct{ state *completionSQL }

func (completionConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (completionConn) Close() error              { return nil }
func (completionConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c completionConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.state.queries = append(c.state.queries, q)
	if c.state.execErr != nil {
		return nil, c.state.execErr
	}
	if !c.state.exists {
		return driver.RowsAffected(0), nil
	}
	if strings.Contains(q, "completion_version") {
		if c.state.prepared || c.state.done {
			return driver.RowsAffected(0), nil
		}
		c.state.version = 1
	}
	c.state.done = true
	return driver.RowsAffected(1), nil
}
func (c completionConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	values := []driver.Value(nil)
	if c.state.exists {
		done := int64(0)
		if c.state.done {
			done = 1
		}
		values = []driver.Value{"state", "mysql-bin.000001", int64(4), done, time.Now()}
		if strings.Contains(q, "completion_version") {
			values = append(values, c.state.version)
		}
	}
	return &completionRows{values: values}, nil
}

type completionRows struct{ values []driver.Value }

func (r *completionRows) Columns() []string {
	cols := []string{"job_id", "start_file", "start_pos", "done", "updated_at"}
	if len(r.values) == 6 {
		cols = append(cols, "completion_version")
	}
	return cols
}
func (*completionRows) Close() error { return nil }
func (r *completionRows) Next(dest []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(dest, r.values)
	r.values = nil
	return nil
}
func completionMetaStore(t *testing.T, state *completionSQL) *MySQLOffsetStore {
	t.Helper()
	db := sql.OpenDB(completionConnector{state})
	t.Cleanup(func() { _ = db.Close() })
	return &MySQLOffsetStore{db: db}
}

func TestSnapshotCompletionMarkRequiresExistingStart(t *testing.T) {
	store := completionMetaStore(t, &completionSQL{})
	if err := store.MarkSnapshotDone(context.Background(), "state"); err == nil {
		t.Fatal("missing start state was reported complete")
	}
}

func TestSnapshotCompletionMarkSQLFailurePropagates(t *testing.T) {
	injected := errors.New("metadata write failed")
	state := &completionSQL{exists: true, execErr: injected}
	if err := completionMetaStore(t, state).MarkSnapshotDone(context.Background(), "state"); !errors.Is(err, injected) {
		t.Fatalf("result=%v", err)
	}
	if state.done {
		t.Fatal("failed SQL advanced completion")
	}
}

func TestSnapshotCompletionMarkIdempotentAndRejectsUnprovenState(t *testing.T) {
	for _, mode := range []string{"normal", "legacy_done", "prepared"} {
		t.Run(mode, func(t *testing.T) {
			state := &completionSQL{exists: true, done: mode == "legacy_done", prepared: mode == "prepared"}
			store := completionMetaStore(t, state)
			err := store.MarkSnapshotDone(context.Background(), "state")
			if mode != "normal" {
				if err == nil || state.version != 0 {
					t.Fatal("unproven state upgraded")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := store.MarkSnapshotDone(context.Background(), "state"); err != nil {
				t.Fatalf("reprocessed completion=%v", err)
			}
			st, err := store.GetSnapshotState(context.Background(), "state")
			if err != nil || !st.CompletionConfirmed() {
				t.Fatalf("confirmed state=%#v error=%v", st, err)
			}
		})
	}
}
