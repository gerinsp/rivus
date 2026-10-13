package iceberg

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

// Model a catalog request which was already dispatched when its worker lost
// ownership. Its acceptance is deliberately delayed until recovery has written
// to another physical table identifier.
type isolatedDelayedCatalog struct {
	icecatalog.Catalog
	dispatched, resume chan struct{}
	once               sync.Once
}

// Production FencedOffsetStore rejects COMMITTED persistence after takeover.
// Keep the unit double faithful to that already-tested SQL fence.
type isolatedFencedTestStore struct{ *automaticStore }

func (s *isolatedFencedTestStore) MarkIcebergCommitCommitted(ctx context.Context, key, id string) error {
	if err := s.CheckOwnership(ctx); err != nil {
		return err
	}
	return s.automaticStore.MarkIcebergCommitCommitted(ctx, key, id)
}

func (c *isolatedDelayedCatalog) LoadTable(ctx context.Context, id icetable.Identifier) (*icetable.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return icetable.New(id, tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}
func (c *isolatedDelayedCatalog) CommitTable(ctx context.Context, id icetable.Identifier, req []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	if len(id) == 2 && id[0] == "bronze" && id[1] == "orders" {
		c.once.Do(func() { close(c.dispatched) })
		select {
		case <-c.resume:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return c.Catalog.CommitTable(ctx, id, req, updates)
}

func TestRecoveryIsolatedTargetCannotReceiveOldInFlightCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := recoveryCatalog(t)
	cat := &isolatedDelayedCatalog{Catalog: base, dispatched: make(chan struct{}), resume: make(chan struct{})}
	var resume sync.Once
	release := func() { resume.Do(func() { close(cat.resume) }) }
	defer release()
	oldStore := &automaticStore{guardTestStore: newGuardTestStore()}
	old := automaticSink(t, cat, oldStore)
	old.offsetSto = &isolatedFencedTestStore{oldStore}
	old.jobID = "old-job"
	old.stateKey = "old-state"
	ev := recoveryEvent(1, 0)
	ev.Data["status"] = "late-old-commit"
	if err := old.handleEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- old.flushAll(ctx) }()
	select {
	case <-cat.dispatched:
	case err := <-done:
		t.Fatalf("old request did not reach server barrier: %v", err)
	case <-ctx.Done():
		t.Fatal("old request did not dispatch")
	}
	if len(oldStore.intents) != 1 {
		t.Fatal("dispatched old request has no durable intent")
	}
	var oldOperation string
	for _, intent := range oldStore.intents {
		oldOperation = intent.OperationID
		if intent.Status != meta.IcebergCommitPrepared {
			t.Fatal("unresolved old intent prematurely committed")
		}
	}
	oldTable, err := base.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.CreateNamespace(ctx, icetable.Identifier{"bronze_recovery"}, nil); err != nil {
		t.Fatal(err)
	}
	newTable, err := base.CreateTable(ctx, icetable.Identifier{"bronze_recovery", "orders"}, oldTable.Schema(), icecatalog.WithProperties(oldTable.Properties()))
	if err != nil {
		t.Fatal(err)
	}
	newStore := &automaticStore{guardTestStore: newGuardTestStore()}
	replacement := automaticSink(t, cat, newStore)
	replacement.jobID = "recovery-job"
	replacement.stateKey = "recovery-state"
	state := replacement.states["app.orders"]
	state.targetNamespace = "bronze_recovery"
	state.table = newTable
	replacement.cfg.Overrides = map[string]config.IcebergTarget{"app.orders": {Namespace: "bronze_recovery", Table: "orders"}}
	if err := replacement.checkIcebergRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := recoveryEvent(1, 0)
	fresh.Data["status"] = "fresh-recovery-data"
	if err := replacement.handleEvent(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if err := replacement.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	newTable, err = base.LoadTable(ctx, icetable.Identifier{"bronze_recovery", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	beforeLocation := newTable.MetadataLocation()
	if oldTable.Metadata().TableUUID() == newTable.Metadata().TableUUID() || oldTable.Location() == newTable.Location() {
		t.Fatal("recovery reused original table identity or storage location")
	}
	// The old server-side request can still accept its commit. A lease fence
	// cannot cancel that request, but its table identifier cannot reach the new target.
	oldStore.ownershipErr = meta.ErrWorkerOwnershipLost
	release()
	select {
	case err := <-done:
		if !errors.Is(err, meta.ErrWorkerOwnershipLost) {
			t.Fatalf("old worker completion error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("old worker did not finish")
	}
	oldTable, err = base.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := countIcebergTableRows(ctx, oldTable); err != nil || count != 1 {
		t.Fatalf("late old commit was not actually accepted: rows=%d %v", count, err)
	}
	newTable, err = base.LoadTable(ctx, icetable.Identifier{"bronze_recovery", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if newTable.MetadataLocation() != beforeLocation {
		t.Fatal("late old commit changed new target metadata")
	}
	_, records, err := newTable.Scan(icetable.WithSelectedFields("id", "status")).ToArrowRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for record, readErr := range records {
		if readErr != nil {
			t.Fatal(readErr)
		}
		ids := record.Column(0).(*array.Int64)
		statuses := record.Column(1).(*array.String)
		for i := 0; i < ids.Len(); i++ {
			rows++
			if ids.Value(i) != 1 || statuses.Value(i) != "fresh-recovery-data" {
				t.Error("late old data reached the recovery target")
			}
		}
		record.Release()
	}
	if rows != 1 {
		t.Fatalf("recovery rows=%d", rows)
	}
	if oldStore.intents[oldOperation].Status != meta.IcebergCommitPrepared || oldStore.saveCount != 0 || newStore.saveCount != 0 {
		t.Fatal("isolation erased old UNKNOWN evidence or advanced a checkpoint")
	}
	for _, intent := range newStore.intents {
		if intent.JobID != "recovery-job" || intent.StateKey != "recovery-state" || intent.TargetNamespace != "bronze_recovery" || intent.OperationID == oldOperation {
			t.Fatal("recovery did not keep independent metadata and operation identities")
		}
	}
}
