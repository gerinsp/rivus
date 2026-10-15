//go:build integration

package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/google/uuid"
)

type expiredTrinoHistory struct{ base http.RoundTripper }

func (p expiredTrinoHistory) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/query/") {
		return &http.Response{StatusCode: http.StatusGone, Body: http.NoBody, Header: make(http.Header), Request: r}, nil
	}
	return p.base.RoundTrip(r)
}

func TestScopedNativeRealUnpartitionedSkipsRedundantDeletes(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated Trino fixture opt-in")
	}
	for _, bulk := range []bool{false, true} {
		for _, survivor := range []bool{false, true} {
			t.Run(fmt.Sprintf("bulk_%t_survivor_%t", bulk, survivor), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				ns := "native_deletes_" + strings.ReplaceAll(uuid.NewString(), "-", "")
				client := &Sink{cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}}}
				scopedQuery(t, client, `CREATE SCHEMA fixture.`+ns, "")
				scopedQuery(t, client, `CREATE TABLE fixture.`+ns+`.orders (id BIGINT,status VARCHAR,created_at TIMESTAMP(6)) WITH(format_version=2)`, ns)
				t.Cleanup(func() {
					c, stop := context.WithTimeout(context.Background(), 15*time.Second)
					defer stop()
					client.execTrinoStatement(c, `DROP TABLE IF EXISTS fixture.`+ns+`.orders`, ns)
					client.execTrinoStatement(c, `DROP SCHEMA IF EXISTS fixture.`+ns, "")
				})
				scopedQuery(t, client, `INSERT INTO fixture.`+ns+`.orders VALUES (1,'retained',TIMESTAMP '2026-08-01 00:00:00'),(2,'stale',TIMESTAMP '2026-09-02 00:00:00'),(3,'moves',TIMESTAMP '2026-08-01 00:00:00'),(4,'null-date',NULL)`, ns)
				scopedQuery(t, client, `DELETE FROM fixture.`+ns+`.orders WHERE created_at >= TIMESTAMP '2026-09-01 00:00:00'`, ns)
				pos := scopedQuery(t, client, `SELECT COALESCE(SUM(record_count),0) FROM fixture.`+ns+`."orders$files" WHERE content=1`, ns)
				if len(pos) != 1 || string(pos[0][0]) != "1" {
					t.Fatalf("fixture did not create the initial position delete: %v", pos)
				}
				store := &automaticStore{guardTestStore: newGuardTestStore()}
				cat := scopedRealCatalog(t)
				s := scopedCrashSink(t, scopedCrashInput{Namespace: ns, JobID: ns, SubmissionID: "epoch"}, cat, store)
				s.scopedNativeWrites = true
				s.cfg.SnapshotBatchSize = 100
				if bulk {
					s.cfg.SnapshotBatchSize = 1
				}
				s.cfg.SnapshotSpoolDirectory = t.TempDir()
				state := s.states["app.orders"]
				state.snapshotReplaceApplied, state.snapshotTruncateApplied = true, true
				rows := []map[string]interface{}{
					{"id": int64(2), "status": "replacement", "created_at": time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)},
					{"id": int64(5), "status": "new", "created_at": time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)},
				}
				wantDeletes := "0"
				if survivor {
					rows = append(rows, map[string]interface{}{"id": int64(3), "status": "moved", "created_at": time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)})
					wantDeletes = "1"
				}
				if err := s.handleSnapshotBatch(ctx, model.Event{Schema: "app", Table: "orders", Rows: rows}); err != nil {
					t.Fatal(err)
				}
				eq := scopedQuery(t, client, `SELECT COALESCE(SUM(record_count),0) FROM fixture.`+ns+`."orders$files" WHERE content=2`, ns)
				if len(eq) != 1 || string(eq[0][0]) != wantDeletes {
					t.Fatalf("equality delete records=%v, want %s surviving keys", eq, wantDeletes)
				}
				result := scopedQuery(t, client, `SELECT id,status FROM fixture.`+ns+`.orders ORDER BY id`, ns)
				wantStatus := []string{`"retained"`, `"replacement"`, `"moves"`, `"null-date"`, `"new"`}
				if survivor {
					wantStatus[2] = `"moved"`
				}
				if len(result) != 5 {
					t.Fatalf("lost or duplicated rows: %v", result)
				}
				for i, row := range result {
					if string(row[0]) != fmt.Sprint(i+1) || string(row[1]) != wantStatus[i] {
						t.Fatalf("unexpected target rows: %v", result)
					}
				}
			})
		}
	}
}

func TestScopedNativeRealBulkLargeRowPartitionMoveAndExpiredHistory(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated Trino fixture opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ns := "native_scope_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	id, sid, owner := ns, uuid.NewString(), "native-snapshot"
	client := &Sink{cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}}}
	scopedQuery(t, client, `CREATE SCHEMA fixture.`+ns, "")
	scopedQuery(t, client, `CREATE TABLE fixture.`+ns+`.orders (id BIGINT,status VARCHAR,created_at TIMESTAMP(6)) WITH (format_version=2, partitioning=ARRAY['day(created_at)'])`, ns)
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		client.execTrinoStatement(c, `DROP TABLE IF EXISTS fixture.`+ns+`.orders`, ns)
		client.execTrinoStatement(c, `DROP SCHEMA IF EXISTS fixture.`+ns, "")
	})
	scopedQuery(t, client, `INSERT INTO fixture.`+ns+`.orders VALUES (1,'outside',TIMESTAMP '2026-08-01 00:00:00'),(2,'stale',TIMESTAMP '2026-09-02 00:00:00'),(3,'moves',TIMESTAMP '2026-08-01 00:00:00'),(4,'null-date',NULL)`, ns)
	base, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	registry, err := meta.NewMySQLJobStore(recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := base.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := registry.Init(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", recoveryMetaDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Cleanup(func() {
		db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", id)
		base.DeleteJobState(context.Background(), id)
		registry.DeleteJob(context.Background(), id)
	})
	if err := registry.SaveJob(ctx, meta.PersistedJob{ID: id, SubmissionID: sid, MetaKey: id, Config: &config.JobConfig{ID: id}, DesiredState: meta.DesiredStateRunning, ExecutionRole: meta.JobExecutionRoleSnapshot, LastStatus: "RUNNING"}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ClaimJobs(ctx, meta.JobExecutionRoleSnapshot, owner, 100, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	fenced, err := registry.FenceWorkerOffsets(base, meta.WorkerOwnership{JobID: id, StateKey: id, SubmissionID: sid, Owner: owner})
	if err != nil {
		t.Fatal(err)
	}
	cat := scopedRealCatalog(t)
	input := scopedCrashInput{Namespace: ns, JobID: id, SubmissionID: sid, Owner: owner}
	s := scopedCrashSink(t, input, cat, fenced)
	if err := s.ValidateSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
		t.Fatal(err)
	}
	if err := s.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
		t.Fatal(err)
	}
	// Reproduce a v1 MERGE accepted before source ACK, but with response loss.
	rows := []map[string]interface{}{{"id": int64(3), "status": "legacy-prefix", "created_at": time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)}}
	queries, err := s.snapshotMergeTrinoQueries(s.states["app.orders"], rows)
	if err != nil {
		t.Fatal(err)
	}
	loss := &scopedResponseLoss{base: http.DefaultTransport, stage: "lost-terminal"}
	oldClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: loss}
	err = s.executeRebuildStatement(ctx, s.states["app.orders"], "merge", queries[0])
	http.DefaultClient = oldClient
	if err == nil {
		t.Fatal("expected lost terminal response")
	}
	// Coordinator history expired; an exact query ID on the current main
	// RowDelta, bound to the original target UUID/digest/epoch, must prove it.
	http.DefaultClient = &http.Client{Transport: expiredTrinoHistory{http.DefaultTransport}}
	defer func() { http.DefaultClient = oldClient }()
	restarted := scopedCrashSink(t, input, cat, fenced)
	restarted.scopedNativeWrites = true
	if err := restarted.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	state := restarted.states["app.orders"]
	before := state.table.MetadataLocation()
	if err := restarted.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
		t.Fatal(err)
	}
	if before != state.table.MetadataLocation() {
		t.Fatal("restart repeated scope reset")
	}
	large := strings.Repeat("blob-sized payload ", 70000)
	rows[0]["status"] = large
	for n := int64(5); n < 2005; n++ {
		rows = append(rows, map[string]interface{}{"id": n, "status": strings.Repeat("wide ", 200), "created_at": time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)})
	}
	started := time.Now()
	restarted.cfg.SnapshotBatchSize = 500
	restarted.cfg.MaxBatchBytes = 32 * 1024 * 1024
	if err := restarted.handleSnapshotBatch(ctx, model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", Rows: rows}); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	result := scopedQuery(t, client, `SELECT COUNT(*),COUNT(DISTINCT id),MAX(length(status)) FROM fixture.`+ns+`.orders`, ns)
	if len(result) != 1 || string(result[0][0]) != "2003" || string(result[0][1]) != "2003" || string(result[0][2]) != fmt.Sprint(len(large)) {
		t.Fatalf("lost/duplicated payload: %v", result)
	}
	var native, merges int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM iceberg_commit_intents WHERE job_id=? AND kind='snapshot'", id).Scan(&native)
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM iceberg_commit_intents WHERE job_id=? AND operation='trino-merge'", id).Scan(&merges)
	if native != 1 || merges != 1 {
		t.Fatalf("bulk split into SQL mutations native=%d old_merges=%d", native, merges)
	}
	t.Logf("2001 wide rows including a %d-byte payload replaced via ONE native commit in %s; retained outside/null rows and moved partition PK, expired-history recovery, no new MERGE", len(large), elapsed)
}

type nativeKillCatalog struct{ icecatalog.Catalog }

func (c nativeKillCatalog) LoadTable(ctx context.Context, id icetable.Identifier) (*icetable.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return icetable.New(id, tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c nativeKillCatalog) CommitTable(ctx context.Context, id icetable.Identifier, req []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	m, loc, err := c.Catalog.CommitTable(ctx, id, req, updates)
	if err == nil {
		killRecoveryWorker()
	}
	return m, loc, err
}
