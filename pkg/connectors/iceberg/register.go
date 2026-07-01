package iceberg

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

func Register(reg *connector.Registry) {
	reg.RegisterSink("iceberg_native", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		icfg, err := decodeIcebergConfig(cfg)
		if err != nil {
			return nil, err
		}

		return NewSink(jctx.JobID, jctx.MetaKey, jctx.JobName, icfg, jctx.Retry, jctx.MetaStore, jctx.ReportProgress)
	})

}

func decodeIcebergConfig(v any) (config.IcebergConfig, error) {
	switch t := v.(type) {
	case config.IcebergConfig:
		return normalizeIcebergConfig(t), nil
	case *config.IcebergConfig:
		if t == nil {
			return config.IcebergConfig{}, fmt.Errorf("iceberg_native config is nil")
		}
		return normalizeIcebergConfig(*t), nil
	}

	b, err := yaml.Marshal(v)
	if err != nil {
		return config.IcebergConfig{}, err
	}

	var c config.IcebergConfig
	if err := yaml.Unmarshal(b, &c); err != nil {
		return config.IcebergConfig{}, err
	}

	return normalizeIcebergConfig(c), nil
}

func normalizeIcebergConfig(c config.IcebergConfig) config.IcebergConfig {
	c.CatalogURI = strings.TrimRight(strings.TrimSpace(c.CatalogURI), "/")
	c.Warehouse = strings.TrimRight(strings.TrimSpace(c.Warehouse), "/")
	c.Credential = strings.TrimSpace(c.Credential)
	c.OAuthToken = strings.TrimSpace(c.OAuthToken)
	c.Scope = strings.TrimSpace(c.Scope)
	c.Prefix = strings.Trim(strings.TrimSpace(c.Prefix), "/")
	c.DefaultNamespace = strings.TrimSpace(c.DefaultNamespace)
	c.SnapshotWriteMode = normalizeSnapshotWriteMode(c.SnapshotWriteMode)
	c.SnapshotReplaceDeleteExecutor = normalizeSnapshotReplaceDeleteExecutor(c.SnapshotReplaceDeleteExecutor)
	c.CDCDeleteExecutor = normalizeSnapshotReplaceDeleteExecutor(c.CDCDeleteExecutor)

	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.SnapshotBatchSize <= 0 {
		c.SnapshotBatchSize = 10000
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = config.ByteSize(128 * 1024 * 1024)
	}
	if c.MaxConcurrentCommits <= 0 {
		c.MaxConcurrentCommits = 2
	}
	if c.FlushSeconds <= 0 {
		c.FlushSeconds = 30
	}
	if c.DeleteConcurrency <= 0 {
		c.DeleteConcurrency = 2
	}
	if c.IdleTableEvictSeconds < 0 {
		c.IdleTableEvictSeconds = 0
	}

	if c.Overrides != nil {
		normalized := make(map[string]config.IcebergTarget, len(c.Overrides))
		for key, target := range c.Overrides {
			nk := strings.ToLower(strings.TrimSpace(key))
			if nk == "" {
				continue
			}
			target.Namespace = strings.TrimSpace(target.Namespace)
			target.Table = strings.TrimSpace(target.Table)
			normalized[nk] = target
		}
		c.Overrides = normalized
	}

	if c.PrimaryKeys != nil {
		normalized := make(map[string][]string, len(c.PrimaryKeys))
		for key, cols := range c.PrimaryKeys {
			nk := strings.ToLower(strings.TrimSpace(key))
			if nk == "" {
				continue
			}
			out := make([]string, 0, len(cols))
			for _, col := range cols {
				cc := strings.TrimSpace(col)
				if cc == "" {
					continue
				}
				out = append(out, cc)
			}
			normalized[nk] = out
		}
		c.PrimaryKeys = normalized
	}

	if c.SnapshotReplaceFilters != nil {
		normalized := make(map[string]config.IcebergSnapshotReplaceFilterConfig, len(c.SnapshotReplaceFilters))
		for key, filter := range c.SnapshotReplaceFilters {
			nk := strings.ToLower(strings.TrimSpace(key))
			filter.Column = strings.TrimSpace(filter.Column)
			filter.Op = strings.TrimSpace(filter.Op)
			filter.Value = strings.TrimSpace(filter.Value)
			if nk == "" || filter.Column == "" || filter.Op == "" || filter.Value == "" {
				continue
			}
			normalized[nk] = filter
		}
		c.SnapshotReplaceFilters = normalized
	}

	if len(c.SnapshotTruncateTables) > 0 {
		normalized := make([]string, 0, len(c.SnapshotTruncateTables))
		for _, table := range c.SnapshotTruncateTables {
			nt := strings.ToLower(strings.TrimSpace(table))
			if nt == "" {
				continue
			}
			normalized = append(normalized, nt)
		}
		c.SnapshotTruncateTables = normalized
	}

	if len(c.SnapshotTruncateExcludeTables) > 0 {
		normalized := make([]string, 0, len(c.SnapshotTruncateExcludeTables))
		for _, table := range c.SnapshotTruncateExcludeTables {
			nt := strings.ToLower(strings.TrimSpace(table))
			if nt == "" {
				continue
			}
			normalized = append(normalized, nt)
		}
		c.SnapshotTruncateExcludeTables = normalized
	}

	if c.TableProperties != nil {
		normalized := make(map[string]string, len(c.TableProperties))
		for key, value := range c.TableProperties {
			nk := strings.TrimSpace(key)
			if nk == "" {
				continue
			}
			normalized[nk] = strings.TrimSpace(value)
		}
		c.TableProperties = normalized
	}
	c.TrinoDelete.URI = strings.TrimRight(strings.TrimSpace(c.TrinoDelete.URI), "/")
	c.TrinoDelete.User = strings.TrimSpace(c.TrinoDelete.User)
	c.TrinoDelete.Password = strings.TrimSpace(c.TrinoDelete.Password)
	c.TrinoDelete.Source = strings.TrimSpace(c.TrinoDelete.Source)
	c.TrinoDelete.Catalog = strings.TrimSpace(c.TrinoDelete.Catalog)
	c.TrinoDelete.AccessToken = strings.TrimSpace(c.TrinoDelete.AccessToken)

	c.MetadataColumns = normalizeMetadataColumnsConfig(c.MetadataColumns)

	return c
}

func normalizeMetadataColumnsConfig(c config.IcebergMetadataColumnsConfig) config.IcebergMetadataColumnsConfig {
	c.CreatedAt.Name = strings.TrimSpace(c.CreatedAt.Name)
	if c.CreatedAt.Name == "" {
		c.CreatedAt.Name = "created_at"
	}
	if c.CreatedAt.SourceColumns != nil {
		normalized := make(map[string]string, len(c.CreatedAt.SourceColumns))
		for key, value := range c.CreatedAt.SourceColumns {
			nk := strings.ToLower(strings.TrimSpace(key))
			nv := strings.TrimSpace(value)
			if nk == "" || nv == "" {
				continue
			}
			normalized[nk] = nv
		}
		c.CreatedAt.SourceColumns = normalized
	}
	if len(c.CreatedAt.SourceColumns) > 0 {
		c.CreatedAt.Enabled = true
	}

	c.UpdatedAt.Name = strings.TrimSpace(c.UpdatedAt.Name)
	if c.UpdatedAt.Name == "" {
		c.UpdatedAt.Name = "updated_at"
	}

	c.ETLLoadedAt.Name = strings.TrimSpace(c.ETLLoadedAt.Name)
	if c.ETLLoadedAt.Name == "" {
		c.ETLLoadedAt.Name = "etl_loaded_at"
	}

	return c
}
