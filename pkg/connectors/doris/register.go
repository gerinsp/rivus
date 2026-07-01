package doris

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/model"
)

func Register(reg *connector.Registry) {
	reg.RegisterSource("doris", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		if jctx.Mode != config.JobModeSnapshotOnly && !(jctx.Mode == config.JobModeResume && jctx.StoredMode == config.JobModeSnapshotOnly) {
			return nil, fmt.Errorf("doris source supports snapshot-only mode only")
		}

		dcfg, err := decodeDorisSourceConfig(cfg)
		if err != nil {
			return nil, err
		}

		inner, err := NewSource(jctx.JobID, dcfg, jctx.Retry, jctx.ReportProgress)
		if err != nil {
			return nil, err
		}
		inner.SetSinkType(jctx.SinkType)

		return &sourceAdapter{inner: inner}, nil
	})

	reg.RegisterSink("doris", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		dcfg, err := decodeDorisConfig(cfg)
		if err != nil {
			return nil, err
		}

		inner, err := NewSink(jctx.JobID, jctx.MetaKey, dcfg, jctx.Retry, jctx.MetaStore)
		if err != nil {
			return nil, err
		}

		return &sinkAdapter{inner: inner}, nil
	})
}

type sourceAdapter struct {
	inner *Source
}

func (a *sourceAdapter) Run(ctx context.Context, out chan<- model.Event) error {
	return a.inner.Run(ctx, out)
}

func (a *sourceAdapter) Tables() []connector.TableRef {
	return a.inner.Tables()
}

func (a *sourceAdapter) FetchSchema(ctx context.Context, schema, table string) (*model.TableSchema, error) {
	return a.inner.FetchSchema(ctx, schema, table)
}

func (a *sourceAdapter) CountRows(ctx context.Context, schema, table string) (int64, error) {
	return a.inner.CountRows(ctx, schema, table)
}

func (a *sourceAdapter) SkipSnapshotTables(tables []connector.TableRef) {
	a.inner.SkipSnapshotTables(tables)
}

type sinkAdapter struct {
	inner *Sink
}

func (a *sinkAdapter) Run(ctx context.Context, in <-chan model.Event) error {
	return a.inner.Run(ctx, in)
}

func (a *sinkAdapter) EnsureTable(ctx context.Context, targetSchema, targetTable string, schema *model.TableSchema) error {
	return a.inner.EnsureTable(ctx, targetSchema, targetTable, schema)
}

func (a *sinkAdapter) ResolveTarget(srcSchema, srcTable string) (string, string) {
	// pastikan di Sink kamu ada resolveTarget, kalau belum, bikin sederhana:
	// return srcSchema, srcTable (atau pakai override config)
	return a.inner.resolveTarget(srcSchema, srcTable)
}

func (a *sinkAdapter) CountTargetRows(ctx context.Context, targetSchema, targetTable string) (int64, error) {
	return a.inner.CountTargetRows(ctx, targetSchema, targetTable)
}

func (a *sinkAdapter) ResetTargetTable(ctx context.Context, targetSchema, targetTable string) error {
	return a.inner.ResetTargetTable(ctx, targetSchema, targetTable)
}

func decodeDorisSourceConfig(v any) (SourceConfig, error) {
	switch t := v.(type) {
	case SourceConfig:
		return normalizeDorisSourceConfig(t), nil
	case *SourceConfig:
		if t == nil {
			return SourceConfig{}, fmt.Errorf("doris source config is nil")
		}
		return normalizeDorisSourceConfig(*t), nil
	}

	b, err := yaml.Marshal(v)
	if err != nil {
		return SourceConfig{}, err
	}
	var c SourceConfig
	if err := yaml.Unmarshal(b, &c); err != nil {
		return SourceConfig{}, err
	}
	return normalizeDorisSourceConfig(c), nil
}

func decodeDorisConfig(v any) (config.DorisConfig, error) {
	switch t := v.(type) {
	case config.DorisConfig:
		return normalizeDorisConfig(t), nil
	case *config.DorisConfig:
		if t == nil {
			return config.DorisConfig{}, fmt.Errorf("doris config is nil")
		}
		return normalizeDorisConfig(*t), nil
	}

	b, err := yaml.Marshal(v)
	if err != nil {
		return config.DorisConfig{}, err
	}
	var c config.DorisConfig
	if err := yaml.Unmarshal(b, &c); err != nil {
		return config.DorisConfig{}, err
	}
	return normalizeDorisConfig(c), nil
}

func normalizeDorisConfig(c config.DorisConfig) config.DorisConfig {
	c.HTTPHost = strings.TrimRight(strings.TrimSpace(c.HTTPHost), "/")
	c.BEHTTPHost = strings.TrimSpace(c.BEHTTPHost)
	if c.MySQLPort <= 0 {
		c.MySQLPort = 9030
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushSeconds <= 0 {
		c.FlushSeconds = 3
	}
	return c
}
