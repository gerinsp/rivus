package iceberg

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

type sourceTableSelector struct {
	SchemaPattern string
	TablePattern  string
}

func maintenanceIdentityCatalogName(cfg config.IcebergConfig) (string, error) {
	warehouse := strings.TrimSpace(cfg.Warehouse)
	if sparkCatalogNamePattern.MatchString(warehouse) {
		return warehouse, nil
	}
	return "", fmt.Errorf("physical Iceberg catalog cannot be resolved from warehouse %q", warehouse)
}

func maintenanceReservationSelectors(job *config.JobConfig, cfg config.IcebergConfig) ([]meta.IcebergMaintenanceReservationSelector, error) {
	catalog, err := maintenanceIdentityCatalogName(cfg)
	if err != nil {
		return nil, err
	}
	sources, err := normalizedStreamingSourceSelectors(job)
	if err != nil {
		return nil, err
	}
	projected := make([]meta.IcebergMaintenanceReservationSelector, 0, len(sources))
	for _, source := range sources {
		targets, safe := projectSourceSelector(source, cfg)
		if !safe {
			return []meta.IcebergMaintenanceReservationSelector{{
				Catalog: catalog, NamespacePattern: "*", TablePattern: "*",
			}}, nil
		}
		for _, target := range targets {
			target.Catalog = catalog
			projected = append(projected, target)
		}
	}
	return dedupeReservationSelectors(projected), nil
}

func normalizedStreamingSourceSelectors(job *config.JobConfig) ([]sourceTableSelector, error) {
	if job == nil {
		return nil, fmt.Errorf("job config is nil")
	}
	sourceType, sourceCfg := jobSourceSpec(job)
	if sourceType != "" && !strings.EqualFold(sourceType, "mysql") {
		return nil, fmt.Errorf("unsupported streaming source type %q", sourceType)
	}
	mysqlCfg, err := decodeMySQLConfig(sourceCfg)
	if err != nil {
		return nil, err
	}
	mysqlCfg = config.NormalizeMySQLConfig(mysqlCfg)
	seen := make(map[string]struct{}, len(mysqlCfg.Tables))
	selectors := make([]sourceTableSelector, 0, len(mysqlCfg.Tables))
	for _, raw := range mysqlCfg.Tables {
		schemaPattern, tablePattern, ok := splitMaintenanceSourceSelector(raw)
		if !ok {
			return nil, fmt.Errorf("invalid MySQL table selector %q", raw)
		}
		key := schemaPattern + "\x00" + tablePattern
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		selectors = append(selectors, sourceTableSelector{SchemaPattern: schemaPattern, TablePattern: tablePattern})
	}
	return selectors, nil
}

func projectSourceSelector(source sourceTableSelector, cfg config.IcebergConfig) ([]meta.IcebergMaintenanceReservationSelector, bool) {
	if !hasGlobMeta(source.SchemaPattern) && !hasGlobMeta(source.TablePattern) {
		namespace, tableName := (&Sink{cfg: cfg}).ResolveTarget(source.SchemaPattern, source.TablePattern)
		if namespace == "" || tableName == "" {
			return nil, false
		}
		return []meta.IcebergMaintenanceReservationSelector{{NamespacePattern: namespace, TablePattern: tableName}}, true
	}

	baseNamespace := strings.TrimSpace(cfg.DefaultNamespace)
	if baseNamespace == "" {
		baseNamespace = source.SchemaPattern
	}
	if baseNamespace == "*" && source.TablePattern == "*" {
		return nil, false
	}
	targets := []meta.IcebergMaintenanceReservationSelector{{
		NamespacePattern: baseNamespace,
		TablePattern:     source.TablePattern,
	}}

	keys := make([]string, 0, len(cfg.Overrides))
	for key := range cfg.Overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		overrideSchema, overrideTable, ok := splitMaintenanceSourceSelector(key)
		if !ok || !globPatternsMayOverlap(source.SchemaPattern, overrideSchema) || !globPatternsMayOverlap(source.TablePattern, overrideTable) {
			continue
		}
		intersectionSchema, ok := conservativePatternIntersection(source.SchemaPattern, overrideSchema)
		if !ok {
			return nil, false
		}
		intersectionTable, ok := conservativePatternIntersection(source.TablePattern, overrideTable)
		if !ok {
			return nil, false
		}
		override := cfg.Overrides[key]
		targetNamespace := strings.TrimSpace(override.Namespace)
		if targetNamespace == "" {
			targetNamespace = strings.TrimSpace(cfg.DefaultNamespace)
			if targetNamespace == "" {
				targetNamespace = intersectionSchema
			}
		}
		targetTable := strings.TrimSpace(override.Table)
		if targetTable == "" {
			targetTable = intersectionTable
		}
		if targetNamespace == "" || targetTable == "" {
			return nil, false
		}
		targets = append(targets, meta.IcebergMaintenanceReservationSelector{
			NamespacePattern: targetNamespace,
			TablePattern:     targetTable,
		})
	}
	return targets, true
}

func splitMaintenanceSourceSelector(raw string) (string, string, bool) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(raw)), ".")
	if len(parts) != 2 {
		return "", "", false
	}
	schemaPattern := strings.TrimSpace(parts[0])
	tablePattern := strings.TrimSpace(parts[1])
	if schemaPattern == "" || tablePattern == "" {
		return "", "", false
	}
	if _, err := path.Match(schemaPattern, schemaPattern); err != nil {
		return "", "", false
	}
	if _, err := path.Match(tablePattern, tablePattern); err != nil {
		return "", "", false
	}
	return schemaPattern, tablePattern, true
}

func globPatternsMayOverlap(left, right string) bool {
	if !hasGlobMeta(left) {
		matched, _ := path.Match(right, left)
		return matched
	}
	if !hasGlobMeta(right) {
		matched, _ := path.Match(left, right)
		return matched
	}
	leftPrefix := globLiteralPrefix(left)
	rightPrefix := globLiteralPrefix(right)
	return strings.HasPrefix(leftPrefix, rightPrefix) || strings.HasPrefix(rightPrefix, leftPrefix)
}

func conservativePatternIntersection(left, right string) (string, bool) {
	if left == right || right == "*" {
		return left, true
	}
	if left == "*" {
		return right, true
	}
	if !hasGlobMeta(left) {
		matched, _ := path.Match(right, left)
		return left, matched
	}
	if !hasGlobMeta(right) {
		matched, _ := path.Match(left, right)
		return right, matched
	}
	leftPrefix := globLiteralPrefix(left)
	rightPrefix := globLiteralPrefix(right)
	if strings.HasPrefix(leftPrefix, rightPrefix) {
		return left, true
	}
	if strings.HasPrefix(rightPrefix, leftPrefix) {
		return right, true
	}
	return "", false
}

func globLiteralPrefix(pattern string) string {
	if index := strings.IndexAny(pattern, "*?["); index >= 0 {
		return pattern[:index]
	}
	return pattern
}

func dedupeReservationSelectors(selectors []meta.IcebergMaintenanceReservationSelector) []meta.IcebergMaintenanceReservationSelector {
	seen := make(map[string]meta.IcebergMaintenanceReservationSelector, len(selectors))
	for _, selector := range selectors {
		selector.Catalog = strings.TrimSpace(selector.Catalog)
		selector.NamespacePattern = strings.ToLower(strings.TrimSpace(selector.NamespacePattern))
		selector.TablePattern = strings.ToLower(strings.TrimSpace(selector.TablePattern))
		if selector.Catalog == "" || selector.NamespacePattern == "" || selector.TablePattern == "" {
			continue
		}
		key := strings.ToLower(selector.Catalog) + "\x00" + selector.NamespacePattern + "\x00" + selector.TablePattern
		seen[key] = selector
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]meta.IcebergMaintenanceReservationSelector, 0, len(keys))
	for _, key := range keys {
		out = append(out, seen[key])
	}
	return out
}
