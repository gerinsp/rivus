package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/util"
)

type automaticStore struct {
	*guardTestStore
	listFailures, markFailures int
	lists, marks               int
	ownershipErr               error
	markResponseLost           bool
}

func (s *automaticStore) CheckOwnership(context.Context) error { return s.ownershipErr }
func (s *automaticStore) SaveOffset(ctx context.Context, key string, off meta.Offset) error {
	if s.ownershipErr != nil {
		return s.ownershipErr
	}
	return s.testOffsetStore.SaveOffset(ctx, key, off)
}
func (s *automaticStore) ListIcebergCommits(ctx context.Context, key string) ([]meta.IcebergCommitIntent, error) {
	s.lists++
	if s.listFailures > 0 {
		s.listFailures--
		return nil, errors.New("metadata temporarily unavailable")
	}
	return s.guardTestStore.ListIcebergCommits(ctx, key)
}
func (s *automaticStore) MarkIcebergCommitCommitted(ctx context.Context, key, id string) error {
	s.marks++
	if s.markFailures > 0 {
		s.markFailures--
		return errors.New("mark temporarily unavailable")
	}
	if err := s.guardTestStore.MarkIcebergCommitCommitted(ctx, key, id); err != nil {
		return err
	}
	if s.markResponseLost {
		s.markResponseLost = false
		return errors.New("mark accepted but response lost")
	}
	return nil
}

type verificationCatalog struct {
	icecatalog.Catalog
	readFailures int
	reads        int
	hidden       *icetable.Table
	onRead       func()
}

func (c *verificationCatalog) LoadTable(ctx context.Context, ident icetable.Identifier) (*icetable.Table, error) {
	c.reads++
	if c.onRead != nil {
		c.onRead()
	}
	if c.readFailures > 0 {
		c.readFailures--
		return nil, errors.New("catalog temporarily unavailable")
	}
	if c.hidden != nil {
		return c.hidden, nil
	}
	return c.Catalog.LoadTable(ctx, ident)
}

func automaticSink(t *testing.T, cat icecatalog.Catalog, store *automaticStore) *Sink {
	t.Helper()
	s := recoverySink(t, cat, store)
	s.recoveryPolicy = &config.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Second, MaxBackoff: 2 * time.Second}
	s.recoveryWait = func(context.Context, time.Duration) error { return nil }
	return s
}

func assertAutomaticData(t *testing.T, cat icecatalog.Catalog, rows int64, snapshots int) {
	t.Helper()
	tbl, err := cat.LoadTable(context.Background(), icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	count, err := countIcebergTableRows(context.Background(), tbl)
	if err != nil || count != rows || len(tbl.Metadata().Snapshots()) != snapshots {
		t.Fatalf("rows=%d snapshots=%d error=%v", count, len(tbl.Metadata().Snapshots()), err)
	}
}

func TestAutomaticRecoveryLostCommitResponseContinuesWithoutDuplicate(t *testing.T) {
	ctx := context.Background()
	cat := &responseLossCatalog{Catalog: recoveryCatalog(t), lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	var progress []connector.ProgressInfo
	s.progress = func(p connector.ProgressInfo) {
		if p.RecoveryStatus != "" {
			progress = append(progress, p)
		}
	}
	if err := s.handleEvent(ctx, recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(ctx); err != nil {
		t.Fatalf("positive proof did not recover: %v", err)
	}
	if store.saveCount != 0 {
		t.Fatal("reconciliation advanced checkpoint")
	}
	assertAutomaticData(t, cat, 1, 1)
	if len(progress) < 2 || progress[0].RecoveryStatus != "RECOVERING" || progress[len(progress)-1].RecoveryStatus != "RECOVERED" || progress[0].RecoveryOperationID == "" || progress[0].RecoveryStartedAt.IsZero() {
		t.Fatalf("missing recovery observability: %#v", progress)
	}
	if err := s.handleEvent(ctx, recoveryEvent(2, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.handleEvent(ctx, model.Event{Type: model.EventTypeCheckpoint, SourceOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}}); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(ctx); err != nil {
		t.Fatal(err)
	}
	assertAutomaticData(t, cat, 2, 2)
	if store.saveCount != 1 || len(store.intents) != 0 {
		t.Fatalf("checkpoint/receipt saves=%d intents=%d", store.saveCount, len(store.intents))
	}
}

func TestAutomaticRecoveryMetadataMarkFailureAndResponseLoss(t *testing.T) {
	for _, responseLost := range []bool{false, true} {
		t.Run(fmt.Sprint(responseLost), func(t *testing.T) {
			cat := recoveryCatalog(t)
			store := &automaticStore{guardTestStore: newGuardTestStore(), markResponseLost: responseLost}
			if !responseLost {
				store.markFailures = 1
			}
			s := automaticSink(t, cat, store)
			if err := s.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
				t.Fatal(err)
			}
			if err := s.flushAll(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertAutomaticData(t, cat, 1, 1)
			for _, intent := range store.intents {
				if intent.Status != meta.IcebergCommitCommitted {
					t.Fatal("accepted intent not durably confirmed")
				}
			}
		})
	}
}

func TestAutomaticRecoveryTransientReadsAndBoundedBackoff(t *testing.T) {
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	verification := &verificationCatalog{Catalog: cat, readFailures: 1}
	s.catalog = verification
	store.listFailures = 1
	var delays []time.Duration
	s.recoveryWait = func(_ context.Context, d time.Duration) error { delays = append(delays, d); return nil }
	if err := s.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delays) != 2 || delays[0] != time.Second || delays[1] != 2*time.Second {
		t.Fatalf("backoff=%v", delays)
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoveryMissingProofExhaustsWithoutReplay(t *testing.T) {
	base := recoveryCatalog(t)
	initial, err := base.LoadTable(context.Background(), icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	s.catalog = &verificationCatalog{Catalog: cat, hidden: initial}
	var last connector.ProgressInfo
	s.progress = func(p connector.ProgressInfo) {
		if p.RecoveryStatus != "" {
			last = p
		}
	}
	if err := s.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(context.Background()); err == nil {
		t.Fatal("missing proof authorized replay")
	}
	if last.RecoveryStatus != "BLOCKED" || last.RecoveryAttempts != 3 || last.RecoveryLastError == "" {
		t.Fatalf("exhaustion=%#v", last)
	}
	if store.saveCount != 0 || len(store.intents) != 1 {
		t.Fatal("exhaustion discarded recovery state")
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoveryRetainsInitialCommitFailure(t *testing.T) {
	for _, scenario := range []string{"missing-proof", "metadata-unavailable", "deadline", "ownership-lost", "invalid-replay-identities", "committed-mark-failed"} {
		t.Run(scenario, func(t *testing.T) {
			cat := recoveryCatalog(t)
			store := &automaticStore{guardTestStore: newGuardTestStore()}
			s := automaticSink(t, cat, store)
			state := s.states["app.orders"]
			recovered := state.table
			initial := errors.New("original commit transport failure")
			var verification error
			var last connector.ProgressInfo
			s.progress = func(p connector.ProgressInfo) { last = p }
			batch := cdcIcebergBatch("app.orders", []model.Event{recoveryEvent(1, 0)})
			if scenario == "invalid-replay-identities" {
				batch.replayKeys = ""
			}
			if scenario == "committed-mark-failed" {
				store.markErr = initial
			}
			calls := 0
			err := s.guardedIcebergCommit(withIcebergBatch(context.Background(), batch), commitProgress{
				operation: "append", sourceKey: "app.orders", targetNamespace: "bronze", targetTable: "orders", recoveredTable: &recovered,
			}, func(context.Context) error {
				calls++
				switch scenario {
				case "metadata-unavailable":
					store.listFailures = 10
				case "deadline":
					verification = context.DeadlineExceeded
					s.recoveryWait = func(context.Context, time.Duration) error { return verification }
				case "ownership-lost":
					verification = meta.ErrWorkerOwnershipLost
					store.ownershipErr = verification
				case "committed-mark-failed":
					return nil
				}
				return initial
			})
			if !errors.Is(err, initial) || !util.IsPermanent(err) || !strings.Contains(err.Error(), "initial commit error:") {
				t.Fatalf("recovery lost initial cause: %v", err)
			}
			if verification != nil && !errors.Is(err, verification) {
				t.Fatalf("recovery lost verification cause: %v", err)
			}
			if scenario != "invalid-replay-identities" && (last.RecoveryStatus != "BLOCKED" || !strings.Contains(last.RecoveryLastError, initial.Error())) {
				t.Fatalf("blocked progress lost initial cause: %#v", last)
			}
			if calls != 1 || len(store.intents) != 1 || store.saveCount != 0 || recovered != state.table {
				t.Fatal("failed recovery retried the write or changed recovery state/checkpoint/table")
			}
			for _, intent := range store.intents {
				if intent.Status != meta.IcebergCommitPrepared {
					t.Fatal("failed verification confirmed an uncertain commit")
				}
			}
			assertAutomaticData(t, cat, 0, 0)
		})
	}
}

func TestAutomaticRecoveryProofAppearsAfterAttempts(t *testing.T) {
	base := recoveryCatalog(t)
	initial, err := base.LoadTable(context.Background(), icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	verification := &verificationCatalog{Catalog: cat, hidden: initial}
	s.catalog = verification
	waits := 0
	s.recoveryWait = func(context.Context, time.Duration) error {
		waits++
		if waits == 2 {
			verification.hidden = nil
		}
		return nil
	}
	if err := s.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if waits != 2 {
		t.Fatalf("waits=%d", waits)
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoveryRestartUnfinishedUsesDurableReceipt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore(), listFailures: 1}
	s := automaticSink(t, cat, store)
	s.recoveryWait = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	if err := s.handleEvent(ctx, recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(ctx); err == nil {
		t.Fatal("interrupted recovery continued")
	}
	if len(store.intents) != 1 || store.saveCount != 0 {
		t.Fatal("restart state lost")
	}
	restarted := automaticSink(t, cat, store)
	if err := restarted.reconcileAutomatically(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if len(restarted.states["app.orders"].pending) != 0 {
		t.Fatal("accepted row replayed after restart")
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoveryWaitDeadlineRetainsLastVerificationError(t *testing.T) {
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, recoveryCatalog(t), store)
	proofErr := errors.New("matching operation not found in retained main snapshot ancestry")
	s.recoveryWait = func(context.Context, time.Duration) error { return context.DeadlineExceeded }
	var last connector.ProgressInfo
	s.progress = func(p connector.ProgressInfo) { last = p }
	err := s.runAutomaticRecovery(context.Background(), "uncertain-operation", func(context.Context) error { return proofErr })
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, proofErr) {
		t.Fatalf("deadline replaced the verification cause: %v", err)
	}
	if last.RecoveryStatus != "BLOCKED" || last.RecoveryAttempts != 1 || !strings.Contains(last.RecoveryLastError, proofErr.Error()) || !strings.Contains(last.RecoveryLastError, "context deadline exceeded") {
		t.Fatalf("blocked progress omitted the verification cause: %#v", last)
	}
	if store.saveCount != 0 || store.marks != 0 {
		t.Fatal("deadline advanced recovery metadata")
	}
}

func TestAutomaticRecoveryOwnershipLostDuringVerification(t *testing.T) {
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore(), listFailures: 1}
	s := automaticSink(t, cat, store)
	s.recoveryWait = func(context.Context, time.Duration) error {
		store.ownershipErr = meta.ErrWorkerOwnershipLost
		return nil
	}
	if err := s.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(context.Background()); !errors.Is(err, meta.ErrWorkerOwnershipLost) {
		t.Fatalf("ownership result=%v", err)
	}
	if store.marks != 0 || store.saveCount != 0 {
		t.Fatal("stale worker updated metadata")
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoveryOwnershipLostAfterCatalogProof(t *testing.T) {
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	s.catalog = &verificationCatalog{Catalog: cat, onRead: func() { store.ownershipErr = meta.ErrWorkerOwnershipLost }}
	if err := s.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(context.Background()); !errors.Is(err, meta.ErrWorkerOwnershipLost) {
		t.Fatalf("result=%v", err)
	}
	if store.marks != 0 || store.saveCount != 0 {
		t.Fatal("stale worker persisted proven result")
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoveryStopsNextBatchUntilPositiveProof(t *testing.T) {
	base := recoveryCatalog(t)
	initial, err := base.LoadTable(context.Background(), icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	s.cfg.BatchSize = 1
	verification := &verificationCatalog{Catalog: cat, hidden: initial}
	s.catalog = verification
	waiting, release := make(chan struct{}), make(chan struct{})
	s.recoveryWait = func(ctx context.Context, _ time.Duration) error {
		close(waiting)
		select {
		case <-release:
			verification.hidden = nil
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	in := make(chan model.Event, 3)
	in <- recoveryEvent(1, 0)
	in <- recoveryEvent(2, 1)
	in <- model.Event{Type: model.EventTypeCheckpoint, SourceOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}}
	close(in)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, in) }()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("recovery did not wait")
	}
	// The wait hook establishes a deterministic barrier before any next batch.
	if len(in) != 2 || store.saveCount != 0 {
		t.Fatal("next batch/checkpoint consumed during recovery")
	}
	assertAutomaticData(t, base, 1, 1)
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered pipeline did not finish")
	}
	assertAutomaticData(t, base, 2, 2)
	if store.saveCount != 1 {
		t.Fatal("normal checkpoint did not resume")
	}
}

func TestAutomaticRecoveryUnavailableCatalogExhaustsVerificationOnly(t *testing.T) {
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	verification := &verificationCatalog{Catalog: cat, readFailures: 10}
	s.catalog = verification
	if err := s.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushAll(context.Background()); err == nil {
		t.Fatal("unavailable catalog authorized continuation")
	}
	if verification.reads != 3 || store.marks != 0 || store.saveCount != 0 || len(store.intents) != 1 {
		t.Fatal("unbounded verification or unsafe progress")
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoveryStartupRetriesWithoutDoublingReplayReceipts(t *testing.T) {
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	first := recoverySink(t, cat, store)
	if err := first.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := first.flushAll(context.Background()); err == nil {
		t.Fatal("expected interrupted commit")
	}
	store.markFailures = 1
	restarted := automaticSink(t, cat, store)
	if err := restarted.reconcileAutomatically(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(context.Background(), recoveryEvent(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := restarted.handleEvent(context.Background(), model.Event{Type: model.EventTypeCheckpoint, SourceOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.flushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.saveCount != 1 || len(store.intents) != 0 {
		t.Fatal("retry duplicated receipt counters or skipped checkpoint")
	}
	assertAutomaticData(t, base, 1, 1)
}

func TestAutomaticRecoverySnapshotAppendACKAfterProof(t *testing.T) {
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	ack := make(chan error, 1)
	ev := model.Event{Type: model.EventTypeSnapshotBatch, Schema: "app", Table: "orders", Rows: []map[string]interface{}{{"id": int64(1), "status": "pending"}, {"id": int64(2), "status": "pending"}}, Ack: ack}
	if err := s.handleEvent(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if err, ok := <-ack; !ok || err != nil {
		t.Fatalf("snapshot ACK=%v ok=%v", err, ok)
	}
	assertAutomaticData(t, base, 2, 1)
	if store.saveCount != 0 || len(store.intents) != 1 {
		t.Fatal("snapshot reconciliation advanced checkpoint or discarded receipt")
	}
	for _, intent := range store.intents {
		if intent.Status != meta.IcebergCommitCommitted || intent.SnapshotEndOffset != 2 {
			t.Fatalf("receipt=%#v", intent)
		}
	}
}

func TestAutomaticRecoverySchemaCommitRestoresProvenTableHandle(t *testing.T) {
	base := recoveryCatalog(t)
	cat := &responseLossCatalog{Catalog: base, lose: true}
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	s := automaticSink(t, cat, store)
	schema := *s.states["app.orders"].sourceSchema
	schema.Columns = append(append([]model.TableColumn(nil), schema.Columns...), model.TableColumn{Name: "note", DataType: "varchar"})
	updated, err := s.syncTableSchema(context.Background(), s.states["app.orders"].table, "bronze", "orders", &schema, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if updated == nil {
		t.Fatal("recovery lost schema table handle")
	}
	if _, ok := updated.Schema().FindFieldByName("note"); !ok {
		t.Fatal("recovery returned schema before committed update")
	}
	if len(store.intents) != 0 || store.saveCount != 0 {
		t.Fatal("metadata operation retained data receipt or advanced checkpoint")
	}
	assertAutomaticData(t, base, 0, 0)
}

func TestAutomaticRecoveryStaleCheckpointFailureStopsPipeline(t *testing.T) {
	store := &automaticStore{guardTestStore: newGuardTestStore(), ownershipErr: meta.ErrWorkerOwnershipLost}
	s := automaticSink(t, recoveryCatalog(t), store)
	s.pendingOffset = &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}
	if err := s.commitPendingOffset(context.Background(), true); !errors.Is(err, meta.ErrWorkerOwnershipLost) {
		t.Fatalf("stale checkpoint result=%v", err)
	}
	if store.saveCount != 0 || s.pendingOffset == nil {
		t.Fatal("stale checkpoint advanced")
	}
}

func TestAutomaticRecoveryReportsDurableSnapshotProgress(t *testing.T) {
	store := &automaticStore{guardTestStore: newGuardTestStore()}
	store.snapshotProgress = &meta.SnapshotProgress{TableName: "app.orders", NextOffset: 40}
	s := automaticSink(t, recoveryCatalog(t), store)
	var last connector.ProgressInfo
	s.progress = func(p connector.ProgressInfo) { last = p }
	if err := s.runAutomaticRecovery(context.Background(), "operation", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if last.RecoveryCheckpoint != "snapshot:app.orders:40" {
		t.Fatalf("checkpoint=%s", last.RecoveryCheckpoint)
	}
}
