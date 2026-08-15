package iceberg

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

type nativeMaintenanceSignaler struct {
	jobID string
	cfg   config.IcebergConfig
	store *meta.IcebergMaintenanceStore

	mu               sync.RWMutex
	snapshotComplete bool
	signals          chan nativeMaintenanceSignal
}

type nativeMaintenanceSignal struct {
	target                  config.IcebergTarget
	snapshotID              int64
	addedDataFiles          int
	addedEqualityDeletes    int
	snapshotComplete        bool
	barrierSnapshotComplete *bool
}

var sharedMaintenanceSignalStore struct {
	sync.Mutex
	dsn   string
	store *meta.IcebergMaintenanceStore
}

func newNativeMaintenanceSignaler(jobID string, cfg config.IcebergConfig) (*nativeMaintenanceSignaler, error) {
	if !cfg.TableMaintenance.NativeEnabled {
		return nil, nil
	}
	dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
	if dsn == "" {
		return nil, fmt.Errorf("iceberg table_maintenance.native_enabled requires RIVUS_META_MYSQL_DSN")
	}
	store, err := sharedNativeMaintenanceSignalStore(dsn)
	if err != nil {
		return nil, err
	}
	return &nativeMaintenanceSignaler{
		jobID:   jobID,
		cfg:     cfg,
		store:   store,
		signals: make(chan nativeMaintenanceSignal, 8192),
	}, nil
}

func sharedNativeMaintenanceSignalStore(dsn string) (*meta.IcebergMaintenanceStore, error) {
	sharedMaintenanceSignalStore.Lock()
	defer sharedMaintenanceSignalStore.Unlock()
	if sharedMaintenanceSignalStore.store != nil {
		if sharedMaintenanceSignalStore.dsn != dsn {
			return nil, fmt.Errorf("native maintenance signal store DSN differs within one Rivus process")
		}
		return sharedMaintenanceSignalStore.store, nil
	}
	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.Init(ctx); err != nil {
		store.Close()
		return nil, fmt.Errorf("initialize native maintenance signal store: %w", err)
	}
	sharedMaintenanceSignalStore.dsn = dsn
	sharedMaintenanceSignalStore.store = store
	return store, nil
}

func (s *nativeMaintenanceSignaler) setInitialSnapshotComplete(complete bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.snapshotComplete = complete
	s.mu.Unlock()
}

func (s *nativeMaintenanceSignaler) setSnapshotComplete(complete bool) {
	if s == nil {
		return
	}
	s.setInitialSnapshotComplete(complete)
	s.enqueue(nativeMaintenanceSignal{barrierSnapshotComplete: &complete})
}

func (s *nativeMaintenanceSignaler) observeTable(target config.IcebergTarget, tbl *icetable.Table) {
	if s == nil || tbl == nil || tbl.CurrentSnapshot() == nil {
		return
	}
	snapshot := tbl.CurrentSnapshot()
	addedDataFiles := 0
	addedEqualityDeletes := 0
	if snapshot.Summary != nil && snapshot.Summary.Properties != nil && snapshot.Summary.Properties["rivus.job_id"] == s.jobID {
		addedDataFiles = snapshotSummaryCount(snapshot.Summary.Properties, "added-data-files")
		addedEqualityDeletes = snapshotSummaryCount(snapshot.Summary.Properties, "added-equality-delete-files")
	}
	s.mu.RLock()
	snapshotComplete := s.snapshotComplete
	s.mu.RUnlock()
	s.enqueue(nativeMaintenanceSignal{
		target:               target,
		snapshotID:           snapshot.SnapshotID,
		addedDataFiles:       addedDataFiles,
		addedEqualityDeletes: addedEqualityDeletes,
		snapshotComplete:     snapshotComplete,
	})
}

func (s *nativeMaintenanceSignaler) enqueue(signal nativeMaintenanceSignal) {
	select {
	case s.signals <- signal:
	default:
		log.Printf("[iceberg][job %s] native maintenance signal queue is full; periodic safety scan will recover", s.jobID)
	}
}

func (s *nativeMaintenanceSignaler) run(ctx context.Context) {
	if s == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case signal := <-s.signals:
			requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			s.persistSignal(requestCtx, signal)
			cancel()
		}
	}
}

func (s *nativeMaintenanceSignaler) persistSignal(ctx context.Context, signal nativeMaintenanceSignal) {
	if signal.barrierSnapshotComplete != nil {
		due := time.Now().UTC().Add(s.signalDelay())
		if err := s.store.SetSnapshotCompleteForOwner(
			ctx,
			s.jobID,
			*signal.barrierSnapshotComplete,
			s.cfg.TableMaintenance.DataFilesThreshold,
			s.cfg.TableMaintenance.EqualityDeleteFilesThreshold,
			due,
		); err != nil {
			log.Printf("[iceberg][job %s] native maintenance snapshot barrier signal failed: %v", s.jobID, err)
		}
		return
	}
	tableIdentity := canonicalMaintenanceTableKey(maintenanceCatalogName(s.cfg), signal.target.Namespace, signal.target.Table)
	now := time.Now().UTC()
	due := now.Add(s.signalDelay()).Add(deterministicJitter(tableIdentity+"|signal", s.signalDelay()/5))
	orphanDue := now.Add(s.orphanInterval()).Add(deterministicJitter(tableIdentity+"|orphan-write", s.orphanInterval()/10))
	updated, err := s.store.CoalesceSignal(
		ctx,
		tableIdentity,
		signal.snapshotID,
		signal.addedDataFiles,
		signal.addedEqualityDeletes,
		signal.snapshotComplete,
		s.cfg.TableMaintenance.DataFilesThreshold,
		s.cfg.TableMaintenance.EqualityDeleteFilesThreshold,
		due,
		orphanDue,
	)
	if err != nil {
		log.Printf("[iceberg][job %s] native maintenance signal table=%s failed: %v", s.jobID, tableIdentity, err)
		return
	}
	if !updated {
		log.Printf("[iceberg][job %s] native maintenance state not seeded yet table=%s; worker will seed it", s.jobID, tableIdentity)
	}
}

func (s *nativeMaintenanceSignaler) orphanInterval() time.Duration {
	if s == nil || s.cfg.TableMaintenance.NativeOrphanIntervalSeconds <= 0 {
		return 30 * 24 * time.Hour
	}
	return time.Duration(s.cfg.TableMaintenance.NativeOrphanIntervalSeconds) * time.Second
}

func (s *nativeMaintenanceSignaler) signalDelay() time.Duration {
	if s == nil || s.cfg.TableMaintenance.NativeSignalDelaySeconds <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(s.cfg.TableMaintenance.NativeSignalDelaySeconds) * time.Second
}
