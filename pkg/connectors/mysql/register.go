package mysql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/model"
)

func Register(reg *connector.Registry) {
	reg.RegisterSource("mysql", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		mcfg, err := decodeMySQLConfig(cfg)
		if err != nil {
			return nil, err
		}

		inner, err := NewSource(jctx.JobID, jctx.MetaKey, mcfg, jctx.Retry, jctx.MetaStore, jctx.ReportProgress)
		if err != nil {
			return nil, err
		}
		inner.UseSnapshotBatchEvents(sinkUsesAcknowledgedSnapshotBatches(jctx.SinkType))
		inner.UseRollingSnapshotBatches(snapshotRollingEnabledForSink(jctx.SinkType, jctx.SinkConfig))
		inner.SetSinkType(jctx.SinkType)

		return &sourceAdapter{inner: inner, mode: jctx.Mode, storedMode: jctx.StoredMode, tables: inner.tableRefs()}, nil
	})
}

func snapshotRollingEnabledForSink(sinkType string, sinkConfig any) bool {
	if !strings.EqualFold(strings.TrimSpace(sinkType), "iceberg_native") {
		return false
	}
	if typed, ok := sinkConfig.(config.IcebergConfig); ok {
		return typed.SnapshotRollingEnabled == nil || *typed.SnapshotRollingEnabled
	}
	configMap, ok := sinkConfig.(map[string]any)
	if !ok {
		return true
	}
	value, exists := configMap["snapshot_rolling_enabled"]
	if !exists || value == nil {
		return true
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		return err == nil && parsed
	default:
		return false
	}
}

func sinkUsesAcknowledgedSnapshotBatches(sinkType string) bool {
	switch strings.ToLower(strings.TrimSpace(sinkType)) {
	case "doris", "iceberg_native":
		return true
	default:
		return false
	}
}

type sourceAdapter struct {
	inner      *Source
	mode       config.JobMode
	storedMode config.JobMode
	tables     []connector.TableRef
}

func (a *sourceAdapter) Run(ctx context.Context, out chan<- model.Event) error {
	return a.inner.RunFullWithStoredMode(ctx, out, a.mode, a.storedMode)
}

func (a *sourceAdapter) Tables() []connector.TableRef {
	cp := make([]connector.TableRef, len(a.tables))
	copy(cp, a.tables)
	return cp
}

func (a *sourceAdapter) FetchSchema(ctx context.Context, schema, table string) (*model.TableSchema, error) {
	return a.inner.FetchSchemaFor(ctx, schema, table)
}

func (a *sourceAdapter) CountRows(ctx context.Context, schema, table string) (int64, error) {
	return a.inner.CountRows(ctx, schema, table)
}

func (a *sourceAdapter) SkipSnapshotTables(tables []connector.TableRef) {
	a.inner.SkipSnapshotTables(tables)
}

func decodeMySQLConfig(v any) (config.MySQLConfig, error) {
	switch t := v.(type) {
	case config.MySQLConfig:
		return normalizeMySQLConfig(t), nil
	case *config.MySQLConfig:
		if t == nil {
			return config.MySQLConfig{}, fmt.Errorf("mysql config is nil")
		}
		return normalizeMySQLConfig(*t), nil
	}

	b, err := yaml.Marshal(v)
	if err != nil {
		return config.MySQLConfig{}, err
	}
	var c config.MySQLConfig
	if err := yaml.Unmarshal(b, &c); err != nil {
		return config.MySQLConfig{}, err
	}
	return normalizeMySQLConfig(c), nil
}

func normalizeMySQLConfig(c config.MySQLConfig) config.MySQLConfig {
	return config.NormalizeMySQLConfig(c)
}
