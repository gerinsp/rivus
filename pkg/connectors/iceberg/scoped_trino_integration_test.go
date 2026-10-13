//go:build integration

package iceberg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const scopedTrinoURI = "http://127.0.0.1:49319"

// Test-only mapping of the Trino local filesystem to its shared host directory.
// The catalog, metadata, manifests, Parquet and SQL execution remain real.
type scopedFixtureFS struct{ iceio.LocalFS }

func scopedFixturePath(name string) string {
	u, err := url.Parse(name)
	if err != nil || u.Scheme != "local" || u.Host != "" || !strings.HasPrefix(u.Path, "/warehouse/") || strings.Contains(u.Path, "..") {
		panic("unexpected fixture warehouse URI: " + name)
	}
	root := os.Getenv("RIVUS_RECOVERY_TRINO_ROOT")
	if root == "" {
		root = "/tmp/colima"
	}
	return filepath.Join(root, "rivus-recovery-trino-warehouse", strings.TrimPrefix(u.Path, "/warehouse/"))
}
func (f scopedFixtureFS) Open(name string) (iceio.File, error) {
	return f.LocalFS.Open(scopedFixturePath(name))
}
func (f scopedFixtureFS) Create(name string) (iceio.FileWriter, error) {
	return f.LocalFS.Create(scopedFixturePath(name))
}
func (f scopedFixtureFS) WriteFile(name string, data []byte) error {
	return f.LocalFS.WriteFile(scopedFixturePath(name), data)
}
func (f scopedFixtureFS) Remove(name string) error { return f.LocalFS.Remove(scopedFixturePath(name)) }

func init() {
	iceio.Register("local", func(context.Context, *url.URL, map[string]string) (iceio.IO, error) { return scopedFixtureFS{}, nil })
}

func scopedRealCatalog(t *testing.T) icecatalog.Catalog {
	t.Helper()
	db, err := sql.Open("postgres", "postgres://fixture:recovery-test-only@127.0.0.1:49318/iceberg_fixture?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	cat, err := icesql.NewCatalog("fixture", db, icesql.Postgres, iceberglib.Properties{"warehouse": "local:///warehouse", "init_catalog_tables": "false"})
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func scopedQuery(t *testing.T, s *Sink, query, ns string) [][]json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := s.execTrinoStatementTracked(ctx, query, ns, nil)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

type scopedResponseLoss struct {
	base  http.RoundTripper
	stage string
	posts int
	lost  bool
}

func (p *scopedResponseLoss) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPost && r.URL.Path == "/v1/statement" {
		p.posts++
	}
	resp, err := p.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if p.lost {
		return resp, nil
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	var result trinoStatementResponse
	if json.Unmarshal(data, &result) == nil && resp.StatusCode == 200 && result.ID != "" {
		lose := p.stage == "lost-submit" && r.Method == http.MethodPost
		terminal := r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/statement/") && result.NextURI == "" && result.Error == nil
		if p.stage == "kill-terminal" && terminal {
			killRecoveryWorker()
		}
		lose = lose || p.stage == "lost-terminal" && terminal
		if lose {
			p.lost = true
			return nil, errors.New("injected response loss after actual Trino execution")
		}
	}
	return resp, nil
}

func TestScopedTrinoRealFilteredResetMergeAndLostResponses(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated Trino fixture opt-in")
	}
	for _, stage := range []string{"success", "lost-terminal", "lost-submit", "duplicate-target"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			ns := "scope_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			id, sid, owner := ns, uuid.NewString(), "scoped-fixture"
			client := &Sink{cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}}}
			scopedQuery(t, client, `CREATE SCHEMA fixture.`+ns, "")
			scopedQuery(t, client, `CREATE TABLE fixture.`+ns+`.orders (id BIGINT, status VARCHAR, created_at TIMESTAMP(6)) WITH (format_version=2)`, ns)
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
				defer stop()
				client.execTrinoStatement(cleanup, `DROP TABLE IF EXISTS fixture.`+ns+`.orders`, ns)
				client.execTrinoStatement(cleanup, `DROP SCHEMA IF EXISTS fixture.`+ns, "")
			})
			scopedQuery(t, client, `INSERT INTO fixture.`+ns+`.orders VALUES (1,'outside',TIMESTAMP '2026-08-01 00:00:00'),(2,'stale',TIMESTAMP '2026-09-02 00:00:00'),(3,'moves',TIMESTAMP '2026-08-01 00:00:00'),(4,'null-date',NULL)`, ns)
			if stage == "duplicate-target" {
				scopedQuery(t, client, `INSERT INTO fixture.`+ns+`.orders VALUES (1,'duplicate',TIMESTAMP '2026-08-01 00:00:00')`, ns)
			}
			cat := scopedRealCatalog(t)
			tbl, err := cat.LoadTable(ctx, icetable.Identifier{ns, "orders"})
			if err != nil {
				t.Fatal(err)
			}
			store, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer registry.Close()
			if err := store.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := registry.Init(ctx); err != nil {
				t.Fatal(err)
			}
			metaDB, err := sql.Open("mysql", recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer metaDB.Close()
			defer func() {
				metaDB.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", id)
				store.DeleteJobState(context.Background(), id)
				registry.DeleteJob(context.Background(), id)
			}()
			if err := registry.SaveJob(ctx, meta.PersistedJob{ID: id, SubmissionID: sid, Config: &config.JobConfig{ID: id}, MetaKey: id, DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleStreaming, LastStatus: "RUNNING"}); err != nil {
				t.Fatal(err)
			}
			if _, err := registry.ClaimJobs(ctx, meta.JobExecutionRoleStreaming, owner, 100, 2*time.Minute); err != nil {
				t.Fatal(err)
			}
			fenced, err := registry.FenceWorkerOffsets(store, meta.WorkerOwnership{JobID: id, SubmissionID: sid, Owner: owner, StateKey: id})
			if err != nil {
				t.Fatal(err)
			}
			state := &tableState{sourceKey: "app.orders", targetNamespace: ns, targetTable: "orders", table: tbl, sourceSchema: &model.TableSchema{SchemaName: "app", TableName: "orders", Columns: []model.TableColumn{{Name: "id", DataType: "bigint", IsPK: true}, {Name: "status", DataType: "varchar"}, {Name: "created_at", DataType: "datetime"}}}}
			cfg := client.cfg
			cfg.Overrides = map[string]config.IcebergTarget{"app.orders": {Namespace: ns, Table: "orders"}}
			cfg.SnapshotReplaceFilters = map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "created_at", Op: ">=", Value: "2026-09-01 00:00:00"}}
			s := &Sink{jobID: id, stateKey: id, scopedRebuildToken: sid, catalog: cat, offsetSto: fenced, cfg: cfg, states: map[string]*tableState{"app.orders": state}}
			validationErr := s.ValidateSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'")
			if stage == "duplicate-target" {
				if validationErr == nil {
					t.Fatal("target with duplicate preserved primary keys admitted")
				}
				intents, err := store.ListIcebergCommits(ctx, id)
				if err != nil || len(intents) != 0 {
					t.Fatalf("validation mutated recovery state: %d %v", len(intents), err)
				}
				return
			}
			if validationErr != nil {
				t.Fatal(validationErr)
			}
			transport := &scopedResponseLoss{base: http.DefaultTransport, stage: stage}
			oldClient := http.DefaultClient
			http.DefaultClient = &http.Client{Transport: transport}
			defer func() { http.DefaultClient = oldClient }()
			err = s.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'")
			if stage == "success" && err != nil || stage != "success" && err == nil {
				t.Fatalf("injected outcome: %v", err)
			}
			posts := transport.posts
			// Restart from the durable DB, using a fresh native catalog lookup.
			err = s.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'")
			if transport.posts != posts {
				t.Fatal("ambiguous DELETE was submitted again")
			}
			if stage == "lost-submit" {
				if err == nil {
					t.Fatal("missing query ID authorized replay")
				}
				intents, readErr := store.ListIcebergCommits(ctx, id)
				if readErr != nil || len(intents) != 1 || intents[0].Status != meta.IcebergCommitPrepared || intents[0].TrinoQueryID != "" {
					t.Fatalf("unresolved intent not retained: %+v %v", intents, readErr)
				}
				if off, _ := store.GetOffset(ctx, id); off != nil {
					t.Fatal("unknown reset advanced checkpoint")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			rows := []map[string]interface{}{{"id": int64(3), "status": "O'Brien", "created_at": time.Date(2026, 9, 2, 0, 0, 0, 123456000, time.UTC)}, {"id": int64(5), "status": "new", "created_at": time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}}
			if _, err := s.flushScopedSnapshot(ctx, state, rows); err != nil {
				t.Fatal(err)
			}
			// Replaying the same snapshot batch must not create duplicate PKs.
			if _, err := s.flushScopedSnapshot(ctx, state, rows); err != nil {
				t.Fatal(err)
			}
			result := scopedQuery(t, client, `SELECT id,status,CAST(created_at AS VARCHAR) FROM fixture.`+ns+`.orders ORDER BY id`, ns)
			if len(result) != 4 {
				t.Fatalf("duplicate/lost rows: %s", fmt.Sprint(result))
			}
			wantIDs := []string{"1", "3", "4", "5"}
			wantStatuses := []string{"outside", "O'Brien", "null-date", "new"}
			for i, row := range result {
				var status string
				json.Unmarshal(row[1], &status)
				if string(row[0]) != wantIDs[i] || status != wantStatuses[i] {
					t.Fatalf("unexpected target row %s", fmt.Sprint(row))
				}
			}
			if !strings.Contains(string(result[1][2]), ".123456") {
				t.Fatal("timestamp precision lost")
			}
			if count, err := countIcebergTableRows(ctx, state.table); err != nil || count != 4 {
				t.Fatalf("native catalog oracle rows=%d err=%v", count, err)
			}
			if off, _ := store.GetOffset(ctx, id); off != nil {
				t.Fatal("snapshot statements advanced CDC checkpoint")
			}
			t.Log("real Trino DELETE/MERGE and JDBC Iceberg metadata verified; out-of-scope/NULL rows retained, moved PK unique, accepted reset never repeated")
		})
	}
}
