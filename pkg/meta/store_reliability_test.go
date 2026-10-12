package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"testing"
)

var errInjectedOffsetDelete = errors.New("injected DELETE job_offsets failure")

type deleteFailureState struct {
	present    map[string]bool
	calls      []string
	failTable  string
	failBegin  bool
	failQuery  bool
	failCommit bool
	intent     string
	backup     map[string]bool
}

type deleteFailureConnector struct{ state *deleteFailureState }

func (c deleteFailureConnector) Connect(context.Context) (driver.Conn, error) {
	return deleteFailureConn{state: c.state}, nil
}

func (c deleteFailureConnector) Driver() driver.Driver { return deleteFailureDriver{} }

type deleteFailureDriver struct{}

func (deleteFailureDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use deleteFailureConnector")
}

type deleteFailureConn struct{ state *deleteFailureState }

func (deleteFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}

func (deleteFailureConn) Close() error { return nil }

func (c deleteFailureConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c deleteFailureConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if c.state.failBegin {
		return nil, errInjectedOffsetDelete
	}
	c.state.backup = maps.Clone(c.state.present)
	return deleteFailureTx{c.state}, nil
}

type deleteFailureTx struct{ state *deleteFailureState }

func (tx deleteFailureTx) Commit() error {
	if tx.state.failCommit {
		tx.state.present = tx.state.backup
		return errInjectedOffsetDelete
	}
	return nil
}
func (tx deleteFailureTx) Rollback() error { tx.state.present = tx.state.backup; return nil }
func (c deleteFailureConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.state.failQuery {
		return nil, errInjectedOffsetDelete
	}
	return &deleteIntentRows{intent: c.state.intent}, nil
}

type deleteIntentRows struct{ intent string }

func (*deleteIntentRows) Columns() []string { return []string{"operation_id"} }
func (*deleteIntentRows) Close() error      { return nil }
func (r *deleteIntentRows) Next(dest []driver.Value) error {
	if r.intent == "" {
		return io.EOF
	}
	dest[0] = r.intent
	r.intent = ""
	return nil
}

func (c deleteFailureConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	for _, table := range []string{"job_snapshots", "job_snapshot_progress", "job_offsets"} {
		if !strings.Contains(query, "DELETE FROM "+table) {
			continue
		}
		c.state.calls = append(c.state.calls, table)
		if table == c.state.failTable {
			return nil, errInjectedOffsetDelete
		}
		delete(c.state.present, table)
		return driver.RowsAffected(1), nil
	}
	return nil, fmt.Errorf("unexpected statement: %s", query)
}

// SQL errors must propagate and leave every checkpoint intact.
func TestDeleteJobStatePropagatesOffsetDeleteFailure(t *testing.T) {
	state := &deleteFailureState{failTable: "job_offsets", present: map[string]bool{
		"job_snapshots": true, "job_snapshot_progress": true, "job_offsets": true,
	}}
	db := sql.OpenDB(deleteFailureConnector{state: state})
	defer db.Close()

	err := (&MySQLOffsetStore{db: db}).DeleteJobState(context.Background(), "test-job")
	if !errors.Is(err, errInjectedOffsetDelete) {
		t.Fatalf("DeleteJobState returned %v after offset DELETE failed; want injected error (calls=%v remaining=%+v)", err, state.calls, state.present)
	}
}

func TestDeleteJobStateAtomicAndProtectsIntents(t *testing.T) {
	for _, fault := range []string{"job_snapshots", "job_snapshot_progress", "job_offsets", "begin", "query", "commit", "PREPARED", "COMMITTED", "success"} {
		t.Run(fault, func(t *testing.T) {
			original := map[string]bool{"job_snapshots": true, "job_snapshot_progress": true, "job_offsets": true}
			state := &deleteFailureState{present: maps.Clone(original), failTable: fault, failBegin: fault == "begin", failQuery: fault == "query", failCommit: fault == "commit"}
			if fault == "PREPARED" || fault == "COMMITTED" {
				state.intent = "protected-operation"
			}
			db := sql.OpenDB(deleteFailureConnector{state})
			defer db.Close()
			err := (&MySQLOffsetStore{db: db}).DeleteJobState(context.Background(), "job")
			if fault == "success" {
				if err != nil || len(state.present) != 0 {
					t.Fatalf("cleanup: %v remaining=%v", err, state.present)
				}
				return
			}
			if err == nil || !maps.Equal(state.present, original) {
				t.Fatalf("error=%v remaining=%v want intact", err, state.present)
			}
			if state.intent != "" && len(state.calls) != 0 {
				t.Fatalf("deletes ran with protected intent: %v", state.calls)
			}
		})
	}
}
