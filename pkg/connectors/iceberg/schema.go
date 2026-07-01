package iceberg

import (
	"fmt"
	"strconv"
	"strings"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
)

type ddlActionKind string

const (
	ddlActionAddColumn    ddlActionKind = "add_column"
	ddlActionDropColumn   ddlActionKind = "drop_column"
	ddlActionRenameColumn ddlActionKind = "rename_column"
	ddlActionUpdateColumn ddlActionKind = "update_column"
)

const maxIcebergDecimalPrecision = 38

type ddlAction struct {
	Kind    ddlActionKind
	OldName string
	NewName string
	Column  model.TableColumn
}

func copyTableSchema(in *model.TableSchema) *model.TableSchema {
	if in == nil {
		return nil
	}

	out := &model.TableSchema{
		SchemaName: in.SchemaName,
		TableName:  in.TableName,
		Columns:    make([]model.TableColumn, len(in.Columns)),
	}
	copy(out.Columns, in.Columns)
	return out
}

func buildIcebergSchema(sourceSchema *model.TableSchema, pkCols []string) (*iceberglib.Schema, error) {
	fields := make([]iceberglib.NestedField, 0, len(sourceSchema.Columns))
	identifierIDs := make([]int, 0, len(pkCols))
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, key := range pkCols {
		pkSet[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}

	for idx, col := range sourceSchema.Columns {
		typ, err := icebergTypeForColumn(col)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}

		fieldID := idx + 1
		fields = append(fields, iceberglib.NestedField{
			ID:       fieldID,
			Name:     col.Name,
			Type:     typ,
			Required: !col.IsNullable,
			Doc:      col.ColumnType,
		})
		if _, ok := pkSet[strings.ToLower(col.Name)]; ok && supportsIcebergIdentifierField(typ) {
			identifierIDs = append(identifierIDs, fieldID)
		}
	}

	return iceberglib.NewSchemaWithIdentifiers(1, identifierIDs, fields...), nil
}

func supportsIcebergIdentifierField(typ iceberglib.Type) bool {
	switch typ.(type) {
	case iceberglib.Float32Type, iceberglib.Float64Type:
		return false
	default:
		return true
	}
}

func syncSchema(updater *icetable.UpdateSchema, current *iceberglib.Schema, sourceSchema *model.TableSchema, pkCols []string, cfg config.IcebergConfig) (bool, error) {
	changed := false
	if len(pkCols) > 0 {
		paths := make([][]string, 0, len(pkCols))
		for _, key := range pkCols {
			sourceCol, ok := findSourceColumn(sourceSchema, key)
			if !ok {
				continue
			}
			desiredType, err := icebergTypeForColumn(sourceCol)
			if err != nil {
				return false, fmt.Errorf("column %s: %w", sourceCol.Name, err)
			}
			if !supportsIcebergIdentifierField(desiredType) {
				continue
			}
			paths = append(paths, []string{key})
		}
		if len(paths) > 0 {
			updater.SetIdentifierField(paths)
		}
	}

	for _, sourceCol := range sourceSchema.Columns {
		desiredType, err := icebergTypeForColumn(sourceCol)
		if err != nil {
			return false, fmt.Errorf("column %s: %w", sourceCol.Name, err)
		}

		field, ok := current.FindFieldByNameCaseInsensitive(sourceCol.Name)
		if !ok {
			updater.AddColumn([]string{sourceCol.Name}, desiredType, "", false, nil)
			changed = true
			continue
		}

		update := icetable.ColumnUpdate{}
		if shouldUpdateIcebergType(field.Type, desiredType, cfg.AllowUnsafeTypeChanges) {
			update.FieldType = iceberglib.Optional[iceberglib.Type]{Val: desiredType, Valid: true}
		}
		if cfg.AllowUnsafeTypeChanges && field.Required != !sourceCol.IsNullable {
			update.Required = iceberglib.Optional[bool]{Val: !sourceCol.IsNullable, Valid: true}
		}
		if update.FieldType.Valid || update.Required.Valid {
			updater.UpdateColumn([]string{field.Name}, update)
			changed = true
		}
	}

	return changed, nil
}

func shouldUpdateIcebergType(current, desired iceberglib.Type, allowUnsafe bool) bool {
	if current.Equals(desired) {
		return false
	}
	if allowUnsafe {
		return true
	}
	return !icebergTypeCanRepresent(current, desired)
}

func icebergTypeCanRepresent(current, desired iceberglib.Type) bool {
	if _, ok := current.(iceberglib.StringType); ok {
		return true
	}

	switch currentType := current.(type) {
	case iceberglib.Int64Type:
		switch desired.(type) {
		case iceberglib.Int32Type, iceberglib.Int64Type:
			return true
		}
	case iceberglib.Float64Type:
		switch desired.(type) {
		case iceberglib.Float32Type, iceberglib.Float64Type:
			return true
		}
	case iceberglib.DecimalType:
		desiredType, ok := desired.(iceberglib.DecimalType)
		return ok &&
			currentType.Scale() == desiredType.Scale() &&
			currentType.Precision() >= desiredType.Precision()
	}
	return false
}

func findSourceColumn(sourceSchema *model.TableSchema, name string) (model.TableColumn, bool) {
	if sourceSchema == nil {
		return model.TableColumn{}, false
	}
	for _, col := range sourceSchema.Columns {
		if strings.EqualFold(strings.TrimSpace(col.Name), strings.TrimSpace(name)) {
			return col, true
		}
	}
	return model.TableColumn{}, false
}

func icebergTypeForColumn(col model.TableColumn) (iceberglib.Type, error) {
	switch strings.ToLower(strings.TrimSpace(col.DataType)) {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		return iceberglib.PrimitiveTypes.Int32, nil
	case "bigint":
		return iceberglib.PrimitiveTypes.Int64, nil
	case "float":
		return iceberglib.PrimitiveTypes.Float32, nil
	case "double", "real":
		return iceberglib.PrimitiveTypes.Float64, nil
	case "decimal", "numeric":
		precision := 38
		scale := 18
		if col.NumPrec != nil && *col.NumPrec > 0 {
			precision = int(*col.NumPrec)
		}
		if col.NumScale != nil && *col.NumScale >= 0 {
			scale = int(*col.NumScale)
		}
		if precision > maxIcebergDecimalPrecision {
			precision = maxIcebergDecimalPrecision
		}
		if scale > precision {
			scale = precision
		}
		return iceberglib.DecimalTypeOf(precision, scale), nil
	case "bool", "boolean", "bit":
		return iceberglib.PrimitiveTypes.Bool, nil
	case "date":
		return iceberglib.PrimitiveTypes.Date, nil
	case "datetime", "timestamp":
		return iceberglib.PrimitiveTypes.Timestamp, nil
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return iceberglib.PrimitiveTypes.Binary, nil
	case "json", "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set", "time":
		return iceberglib.PrimitiveTypes.String, nil
	default:
		return iceberglib.PrimitiveTypes.String, nil
	}
}

func parseDDLPlan(mysqlDDL string) ([]ddlAction, bool, error) {
	ddl := strings.TrimSpace(strings.TrimSuffix(mysqlDDL, ";"))
	if ddl == "" {
		return nil, true, nil
	}

	low := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(ddl, "\n", " "), "\t", " "))
	switch {
	case strings.HasPrefix(low, "create table"):
		return nil, true, nil
	case strings.HasPrefix(low, "rename table"):
		return nil, true, nil
	case strings.HasPrefix(low, "alter table"):
		parts := strings.Fields(ddl)
		if len(parts) < 4 {
			return nil, false, fmt.Errorf("alter table too short")
		}
		actionsRaw := strings.Join(parts[3:], " ")
		partsRaw := splitTopLevelComma(actionsRaw)
		out := make([]ddlAction, 0, len(partsRaw))
		for _, part := range partsRaw {
			translated, err := translateAlterAction(part)
			if err != nil {
				return nil, false, err
			}
			out = append(out, translated...)
		}
		if len(out) == 0 {
			return nil, true, nil
		}
		return out, false, nil
	default:
		return nil, false, fmt.Errorf("unsupported DDL: %s", ddl)
	}
}

func isCreateTableDDL(ddl string) bool {
	low := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(ddl, "\n", " "), "\t", " "))
	return strings.HasPrefix(strings.TrimSpace(low), "create table")
}

func translateAlterAction(action string) ([]ddlAction, error) {
	trimmed := strings.TrimSpace(action)
	low := strings.ToLower(trimmed)

	switch {
	case strings.HasPrefix(low, "add ") || strings.HasPrefix(low, "add column "):
		act := strings.TrimSpace(trimmed)
		if strings.HasPrefix(strings.ToLower(act), "add column ") {
			act = strings.TrimSpace(act[len("add column "):])
		} else {
			act = strings.TrimSpace(act[len("add "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}
		name, rest, ok := splitFirstIdent(act)
		if !ok {
			return nil, fmt.Errorf("cannot parse add column name")
		}
		colType, notNull := parseMySQLColumnTypeAndNullability(rest)
		if colType == "" {
			return nil, fmt.Errorf("cannot parse add column type")
		}
		return []ddlAction{{
			Kind:   ddlActionAddColumn,
			Column: mysqlDDLColumn(name, colType, notNull),
		}}, nil
	case strings.HasPrefix(low, "drop ") || strings.HasPrefix(low, "drop column "):
		act := strings.TrimSpace(trimmed)
		if strings.HasPrefix(strings.ToLower(act), "drop column ") {
			act = strings.TrimSpace(act[len("drop column "):])
		} else {
			act = strings.TrimSpace(act[len("drop "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}
		name, _, ok := splitFirstIdent(act)
		if !ok {
			return nil, fmt.Errorf("cannot parse drop column name")
		}
		return []ddlAction{{Kind: ddlActionDropColumn, OldName: name}}, nil
	case strings.HasPrefix(low, "modify ") || strings.HasPrefix(low, "modify column "):
		act := strings.TrimSpace(trimmed)
		if strings.HasPrefix(strings.ToLower(act), "modify column ") {
			act = strings.TrimSpace(act[len("modify column "):])
		} else {
			act = strings.TrimSpace(act[len("modify "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}
		name, rest, ok := splitFirstIdent(act)
		if !ok {
			return nil, fmt.Errorf("cannot parse modify column name")
		}
		colType, notNull := parseMySQLColumnTypeAndNullability(rest)
		if colType == "" {
			return nil, fmt.Errorf("cannot parse modify column type")
		}
		return []ddlAction{{
			Kind:   ddlActionUpdateColumn,
			Column: mysqlDDLColumn(name, colType, notNull),
		}}, nil
	case strings.HasPrefix(low, "change ") || strings.HasPrefix(low, "change column "):
		act := strings.TrimSpace(trimmed)
		if strings.HasPrefix(strings.ToLower(act), "change column ") {
			act = strings.TrimSpace(act[len("change column "):])
		} else {
			act = strings.TrimSpace(act[len("change "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}
		oldName, rest, ok := splitFirstIdent(act)
		if !ok {
			return nil, fmt.Errorf("cannot parse change old name")
		}
		newName, rest, ok := splitFirstIdent(rest)
		if !ok {
			return nil, fmt.Errorf("cannot parse change new name")
		}
		colType, notNull := parseMySQLColumnTypeAndNullability(rest)
		if colType == "" {
			return nil, fmt.Errorf("cannot parse change column type")
		}
		actions := make([]ddlAction, 0, 2)
		if !strings.EqualFold(oldName, newName) {
			actions = append(actions, ddlAction{Kind: ddlActionRenameColumn, OldName: oldName, NewName: newName})
		}
		actions = append(actions, ddlAction{
			Kind:   ddlActionUpdateColumn,
			Column: mysqlDDLColumn(newName, colType, notNull),
		})
		return actions, nil
	case strings.HasPrefix(low, "rename column "):
		act := strings.TrimSpace(trimmed[len("rename column "):])
		oldName, rest, ok := splitFirstIdent(act)
		if !ok {
			return nil, fmt.Errorf("cannot parse rename column old name")
		}
		rest = strings.TrimSpace(rest)
		restLow := strings.ToLower(rest)
		if strings.HasPrefix(restLow, "to ") {
			rest = strings.TrimSpace(rest[len("to "):])
		}
		newName, _, ok := splitFirstIdent(rest)
		if !ok {
			return nil, fmt.Errorf("cannot parse rename column new name")
		}
		return []ddlAction{{Kind: ddlActionRenameColumn, OldName: oldName, NewName: newName}}, nil
	case strings.HasPrefix(low, "rename to ") || strings.HasPrefix(low, "rename "):
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported alter action: %s", trimmed)
	}
}

func applyDDLToSourceSchema(schema *model.TableSchema, actions []ddlAction) *model.TableSchema {
	out := copyTableSchema(schema)
	if out == nil {
		return nil
	}

	for _, action := range actions {
		switch action.Kind {
		case ddlActionAddColumn:
			out.Columns = append(out.Columns, action.Column)
		case ddlActionDropColumn:
			filtered := out.Columns[:0]
			for _, col := range out.Columns {
				if strings.EqualFold(col.Name, action.OldName) {
					continue
				}
				filtered = append(filtered, col)
			}
			out.Columns = filtered
		case ddlActionRenameColumn:
			for idx := range out.Columns {
				if strings.EqualFold(out.Columns[idx].Name, action.OldName) {
					out.Columns[idx].Name = action.NewName
					break
				}
			}
		case ddlActionUpdateColumn:
			for idx := range out.Columns {
				if strings.EqualFold(out.Columns[idx].Name, action.Column.Name) {
					action.Column.IsPK = out.Columns[idx].IsPK
					out.Columns[idx] = action.Column
					break
				}
			}
		}
	}

	return out
}

func splitTopLevelComma(s string) []string {
	out := make([]string, 0)
	start := 0
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	out = append(out, s[start:])
	return out
}

func splitFirstIdent(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", false
	}

	if strings.HasPrefix(s, "`") {
		end := strings.Index(s[1:], "`")
		if end < 0 {
			return "", "", false
		}
		name := s[1 : 1+end]
		return name, strings.TrimSpace(s[1+end+1:]), true
	}

	for i, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r':
			return s[:i], strings.TrimSpace(s[i:]), true
		}
	}

	return s, "", true
}

func parseMySQLColumnTypeAndNullability(rest string) (string, bool) {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	low := strings.ToLower(rest)
	notNull := strings.Contains(low, " not null")

	depth := 0
	for i, r := range rest {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ' ', '\t', '\n', '\r':
			if depth == 0 {
				return strings.TrimSpace(rest[:i]), notNull
			}
		}
	}

	return strings.TrimSpace(rest), notNull
}

func mysqlDDLColumn(name, columnType string, notNull bool) model.TableColumn {
	base := strings.ToLower(strings.TrimSpace(columnType))
	base = strings.ReplaceAll(base, "unsigned", "")
	base = strings.ReplaceAll(base, "zerofill", "")
	base = strings.TrimSpace(base)

	col := model.TableColumn{
		Name:       name,
		DataType:   baseTypeName(base),
		ColumnType: columnType,
		IsNullable: !notNull,
	}

	if lParen := strings.IndexByte(base, '('); lParen >= 0 {
		if rParen := strings.IndexByte(base, ')'); rParen > lParen+1 {
			args := strings.Split(base[lParen+1:rParen], ",")
			switch col.DataType {
			case "char", "varchar":
				if n, err := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64); err == nil {
					col.CharMaxLen = &n
				}
			case "decimal", "numeric":
				if len(args) >= 1 {
					if n, err := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64); err == nil {
						col.NumPrec = &n
					}
				}
				if len(args) >= 2 {
					if n, err := strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64); err == nil {
						col.NumScale = &n
					}
				}
			}
		}
	}

	return col
}

func baseTypeName(columnType string) string {
	columnType = strings.TrimSpace(columnType)
	if idx := strings.IndexByte(columnType, '('); idx >= 0 {
		columnType = columnType[:idx]
	}
	if idx := strings.IndexByte(columnType, ' '); idx >= 0 {
		columnType = columnType[:idx]
	}
	return strings.ToLower(strings.TrimSpace(columnType))
}
