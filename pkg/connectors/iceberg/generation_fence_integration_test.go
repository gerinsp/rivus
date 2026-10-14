//go:build integration

package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"database/sql"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/google/uuid"
)

// Capture the actual native RowDelta request, either before submission or
// after a successful submission whose response the writer loses.
type heldGenerationCatalog struct {
	icecatalog.Catalog
	accepted bool
	reqs     []icetable.Requirement
	updates  []icetable.Update
}

func (c *heldGenerationCatalog) LoadTable(ctx context.Context, id icetable.Identifier) (*icetable.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return icetable.New(id, tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *heldGenerationCatalog) CommitTable(ctx context.Context, id icetable.Identifier, reqs []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	c.reqs, c.updates = reqs, updates
	if c.accepted {
		if _, _, err := c.Catalog.CommitTable(ctx, id, reqs, updates); err != nil {
			return nil, "", err
		}
	}
	return nil, "", errors.New("fixture native catalog response unavailable")
}

func TestOriginalIdentifierGenerationFenceRealNativeLateRequest(t *testing.T) {
	if os.Getenv("RIVUS_RECOVERY_TRINO_INTEGRATION") != "1" {
		t.Skip("isolated fixture opt-in")
	}
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-submitted", true: "accepted-response-lost"}[accepted], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			ns := "generation_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			client := &Sink{cfg: config.IcebergConfig{TrinoDelete: config.IcebergTrinoDeleteConfig{URI: scopedTrinoURI, Catalog: "fixture", User: "fixture"}}}
			scopedQuery(t, client, "CREATE SCHEMA fixture."+ns, "")
			scopedQuery(t, client, "CREATE TABLE fixture."+ns+".orders (id BIGINT,status VARCHAR,created_at TIMESTAMP(6)) WITH (format_version=2)", ns)
			t.Cleanup(func() {
				c, stop := context.WithTimeout(context.Background(), 15*time.Second)
				defer stop()
				client.execTrinoStatement(c, "DROP TABLE IF EXISTS fixture."+ns+".orders", ns)
				client.execTrinoStatement(c, "DROP SCHEMA IF EXISTS fixture."+ns, "")
			})
			scopedQuery(t, client, "INSERT INTO fixture."+ns+".orders VALUES (1,'outside',TIMESTAMP '2026-08-01 00:00:00'),(2,'stale',TIMESTAMP '2026-09-02 00:00:00')", ns)
			cat := scopedRealCatalog(t)
			held := &heldGenerationCatalog{Catalog: cat, accepted: accepted}
			input := scopedCrashInput{Namespace: ns, JobID: ns, SubmissionID: uuid.NewString()}
			old := scopedCrashSink(t, input, held, newGuardTestStore())
			state := old.states["app.orders"]
			rows := []map[string]interface{}{{"id": int64(2), "status": "ambiguous-old", "created_at": time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)}}
			_, err := old.commitEqualityDelta(ctx, state, &reducedBatch{pkCols: []string{"id"}, deleteKeys: []map[string]interface{}{{"id": int64(2)}}, rows: rows})
			if err == nil || len(held.reqs) == 0 {
				t.Fatal("native ambiguous request was not captured")
			}
			ident := icetable.Identifier{ns, "orders"}
			before, err := cat.LoadTable(ctx, ident)
			if err != nil {
				t.Fatal(err)
			}
			beforeRows := scopedQuery(t, client, "SELECT id,status FROM fixture."+ns+".orders ORDER BY id", ns)
			newUUID := uuid.New()
			metadata, _, err := cat.CommitTable(ctx, ident, []icetable.Requirement{icetable.AssertTableUUID(before.Metadata().TableUUID())}, []icetable.Update{icetable.NewAssignUUIDUpdate(newUUID)})
			if err != nil {
				t.Fatal(err)
			}
			oldJSON, _ := json.Marshal(before.Metadata())
			newJSON, _ := json.Marshal(metadata)
			var oldMap, newMap map[string]interface{}
			json.Unmarshal(oldJSON, &oldMap)
			json.Unmarshal(newJSON, &newMap)
			for _, key := range []string{"table-uuid", "last-updated-ms", "metadata-log"} {
				delete(oldMap, key)
				delete(newMap, key)
			}
			if metadata.TableUUID() != newUUID || !reflect.DeepEqual(oldMap, newMap) {
				t.Fatal("generation changed serialized data history, schema, or properties")
			}

			afterRows := scopedQuery(t, client, "SELECT id,status FROM fixture."+ns+".orders ORDER BY id", ns)
			if !reflect.DeepEqual(beforeRows, afterRows) {
				t.Fatal("generation fence changed rows")
			}
			fenced, err := cat.LoadTable(ctx, ident)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = cat.CommitTable(ctx, ident, held.reqs, held.updates); err == nil {
				t.Fatal("late native request from old generation was accepted")
			}
			after, err := cat.LoadTable(ctx, ident)
			if err != nil {
				t.Fatal(err)
			}
			if after.MetadataLocation() != fenced.MetadataLocation() {
				t.Fatal("rejected old writer changed metadata")
			}
			// A new writer with the same identifier retains the date scope, resets
			// that scope, and writes fresh native data. Outside rows survive.
			base, err := meta.NewMySQLOffsetStore(recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer base.Close()
			if err = base.Init(ctx); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("mysql", recoveryMetaDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			defer func() {
				db.Exec("DELETE FROM iceberg_commit_intents WHERE job_id=?", ns)
				base.DeleteJobState(context.Background(), ns)
			}()
			fresh := scopedCrashSink(t, input, cat, base)
			fresh.scopedNativeWrites = true
			if err = fresh.ValidateSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
				t.Fatal(err)
			}
			if err = fresh.ResetSnapshotSourceTable(ctx, "app", "orders", "`created_at` >= '2026-09-01 00:00:00'"); err != nil {
				t.Fatal(err)
			}
			rows[0]["status"] = "fresh-source"
			if _, err = fresh.flushScopedSnapshot(withIcebergBatch(ctx, snapshotIcebergBatch("app.orders", 0, 1)), fresh.states["app.orders"], rows); err != nil {
				t.Fatal(err)
			}
			result := scopedQuery(t, client, "SELECT id,status FROM fixture."+ns+".orders ORDER BY id", ns)
			b, _ := json.Marshal(result)
			if string(b) != `[[1,"outside"],[2,"fresh-source"]]` {
				t.Fatalf("unexpected recovered rows: %s", b)
			}
			t.Log("same identifier; schema/history/rows preserved by UUID fence; actual old native request rejected; filtered reset/native append recovered")
		})
	}
}
