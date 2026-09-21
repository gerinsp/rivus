package iceberg

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

func TestReservationMustRemainForSnapshotUntilDurableCompletion(t *testing.T) {
	job := meta.PersistedJob{
		Config:       &config.JobConfig{Mode: config.JobModeSnapshotOnly},
		DesiredState: meta.DesiredStateStopped,
		LastStatus:   "FAILED",
	}
	if !reservationMustRemain(job, false) {
		t.Fatal("incomplete failed snapshot must stay reserved")
	}
	if reservationMustRemain(job, true) {
		t.Fatal("durably completed snapshot must release")
	}
}

func TestReservationMustRemainForPausedStreaming(t *testing.T) {
	job := meta.PersistedJob{
		Config:       &config.JobConfig{Mode: config.JobModeLatest},
		DesiredState: meta.DesiredStateStopped,
		LastStatus:   "PAUSED",
	}
	if !reservationMustRemain(job, false) {
		t.Fatal("paused streaming job must stay reserved")
	}
}

func TestMaintenanceIdentityCatalogNameUsesPhysicalWarehouse(t *testing.T) {
	cfg := config.IcebergConfig{
		Warehouse:   "asmat",
		CatalogName: "rivus",
		TableMaintenance: config.IcebergTableMaintenanceConfig{
			CatalogName: "rivus",
		},
	}
	got, err := maintenanceIdentityCatalogName(cfg)
	if err != nil || got != "asmat" {
		t.Fatalf("catalog = %q, err = %v, want asmat", got, err)
	}
}

func TestMaintenanceIdentityCatalogNameRejectsPathWarehouse(t *testing.T) {
	cfg := config.IcebergConfig{Warehouse: "s3://bucket/warehouse", CatalogName: "rivus"}
	if got, err := maintenanceIdentityCatalogName(cfg); err == nil || got != "" {
		t.Fatalf("catalog = %q, err = %v, want safe failure", got, err)
	}
}

func TestMaintenanceReservationSelectorsApplyNamespaceOnlyOverride(t *testing.T) {
	job := maintenanceOwnershipJob("stream-orders", []string{"sales.*"})
	cfg := config.IcebergConfig{
		Warehouse: "asmat",
		Overrides: map[string]config.IcebergTarget{
			"sales.orders_*": {Namespace: "orders_stream"},
		},
	}
	got, err := maintenanceReservationSelectors(job, cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertMaintenanceReservationSelector(t, got, "asmat", "orders_stream", "orders_*")
}

func TestMaintenanceReservationSelectorsApplyTableOnlyOverride(t *testing.T) {
	job := maintenanceOwnershipJob("stream-orders", []string{"sales.orders_*"})
	cfg := config.IcebergConfig{
		Warehouse: "asmat",
		Overrides: map[string]config.IcebergTarget{
			"sales.orders_*": {Table: "current_orders"},
		},
	}
	got, err := maintenanceReservationSelectors(job, cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertMaintenanceReservationSelector(t, got, "asmat", "sales", "current_orders")
}

func TestMaintenanceReservationSelectorsFailClosedForAmbiguousWildcard(t *testing.T) {
	job := maintenanceOwnershipJob("stream-all", []string{"*.*"})
	cfg := config.IcebergConfig{
		Warehouse: "asmat",
		Overrides: map[string]config.IcebergTarget{
			"sales.orders_*": {Table: "current_orders"},
		},
	}
	got, err := maintenanceReservationSelectors(job, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("selectors = %#v, want one fail-closed catalog selector", got)
	}
	assertMaintenanceReservationSelector(t, got, "asmat", "*", "*")
}

func maintenanceOwnershipJob(id string, tables []string) *config.JobConfig {
	return &config.JobConfig{
		ID:   id,
		Mode: config.JobModeInitial,
		Source: &config.ConnectorSpec{Type: "mysql", Config: map[string]any{
			"tables": tables,
		}},
		Sink: &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{}},
	}
}

func assertMaintenanceReservationSelector(
	t *testing.T,
	selectors []meta.IcebergMaintenanceReservationSelector,
	catalog string,
	namespace string,
	table string,
) {
	t.Helper()
	for _, selector := range selectors {
		if selector.Catalog == catalog && selector.NamespacePattern == namespace && selector.TablePattern == table {
			return
		}
	}
	t.Fatalf("selector %s.%s.%s not found in %#v", catalog, namespace, table, selectors)
}
