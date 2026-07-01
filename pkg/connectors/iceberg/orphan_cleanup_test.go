package iceberg

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
)

func TestOrphanCleanupTargetsInferMySQLTargets(t *testing.T) {
	jobCfg := &config.JobConfig{
		MySQL: config.MySQLConfig{
			Tables: []string{"app.orders", "app.*"},
		},
	}
	sink := &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{
		DefaultNamespace: "lake",
		Overrides: map[string]config.IcebergTarget{
			"app.users": {Namespace: "lake", Table: "users"},
		},
	})}

	targets, err := orphanCleanupTargets(jobCfg, sink, nil)
	if err != nil {
		t.Fatalf("orphanCleanupTargets returned error: %v", err)
	}
	got := targetKeys(targets)
	want := []string{"lake.orders", "lake.users"}
	if len(got) != len(want) {
		t.Fatalf("targets=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("targets=%v want %v", got, want)
		}
	}
}

func TestOrphanCleanupTargetsExplicitTables(t *testing.T) {
	jobCfg := &config.JobConfig{}
	sink := &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{DefaultNamespace: "ignored"})}

	targets, err := orphanCleanupTargets(jobCfg, sink, []string{"lake.sales.orders", "lake.sales.orders", "lake.users"})
	if err != nil {
		t.Fatalf("orphanCleanupTargets returned error: %v", err)
	}
	got := targetKeys(targets)
	want := []string{"lake.sales.orders", "lake.users"}
	if len(got) != len(want) {
		t.Fatalf("targets=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("targets=%v want %v", got, want)
		}
	}
}

func TestOrphanCleanupTargetsRejectInvalidExplicitTable(t *testing.T) {
	_, err := orphanCleanupTargets(&config.JobConfig{}, &Sink{}, []string{"orders"})
	if err == nil {
		t.Fatal("expected invalid explicit table error")
	}
}

func targetKeys(targets []config.IcebergTarget) []string {
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		out = append(out, tableKey(target.Namespace, target.Table))
	}
	return out
}
