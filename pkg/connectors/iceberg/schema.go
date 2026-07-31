package iceberg

import (
	"fmt"
	"strings"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/schemachange"
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
	Kind        ddlActionKind
	OldName     string
	NewName     string
	Column      model.TableColumn
	Position    model.ColumnPosition
	AfterColumn string
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
	if icebergTypeCanRepresent(current, desired) {
		return false
	}

	// Iceberg only permits a small set of safe type promotions. Do not enqueue
	// an incompatible schema update (for example, a legacy INT column whose
	// MySQL TINYINT(1) source now maps to BOOLEAN), because UpdateSchema would
	// reject the entire reconciliation. The existing Iceberg type remains the
	// writer type and can still safely encode the incoming value.
	_, err := iceberglib.PromoteType(current, desired)
	return err == nil
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
		switch desiredType := desired.(type) {
		case iceberglib.Int32Type:
			return currentType.Scale() == 0 && currentType.Precision() >= 10
		case iceberglib.Int64Type:
			return currentType.Scale() == 0 && currentType.Precision() >= 19
		case iceberglib.DecimalType:
			return currentType.Scale() == desiredType.Scale() &&
				currentType.Precision() >= desiredType.Precision()
		}
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
	dataType := strings.ToLower(strings.TrimSpace(col.DataType))
	unsigned := strings.Contains(strings.ToLower(col.ColumnType), "unsigned")

	switch dataType {
	case "tinyint":
		if !unsigned && schemachange.MySQLColumnTypeHasWidth(col.ColumnType, "tinyint", 1) {
			return iceberglib.PrimitiveTypes.Bool, nil
		}
		return iceberglib.PrimitiveTypes.Int32, nil
	case "smallint", "mediumint", "year":
		return iceberglib.PrimitiveTypes.Int32, nil
	case "int", "integer":
		if unsigned {
			return iceberglib.PrimitiveTypes.Int64, nil
		}
		return iceberglib.PrimitiveTypes.Int32, nil
	case "bigint":
		if unsigned {
			return iceberglib.DecimalTypeOf(20, 0), nil
		}
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
			// Match Flink CDC's lossless rule: MySQL supports decimal
			// precision up to 65, while Iceberg is capped at 38.
			return iceberglib.PrimitiveTypes.String, nil
		}
		if scale > precision {
			scale = precision
		}
		return iceberglib.DecimalTypeOf(precision, scale), nil
	case "bool", "boolean":
		return iceberglib.PrimitiveTypes.Bool, nil
	case "bit":
		if schemachange.MySQLColumnTypeHasWidth(col.ColumnType, "bit", 1) {
			return iceberglib.PrimitiveTypes.Bool, nil
		}
		return iceberglib.PrimitiveTypes.Binary, nil
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
	changes, err := schemachange.ParseMySQLDDL(mysqlDDL)
	if err != nil {
		return nil, false, err
	}
	return ddlActionsFromSchemaChanges(changes)
}

func ddlActionsFromSchemaChanges(changes []model.SchemaChange) ([]ddlAction, bool, error) {
	out := make([]ddlAction, 0, len(changes))
	for _, change := range changes {
		switch change.Type {
		case model.SchemaChangeAddColumn:
			if change.Column == nil {
				return nil, false, fmt.Errorf("add-column schema change has no column")
			}
			out = append(out, ddlAction{
				Kind:        ddlActionAddColumn,
				Column:      *change.Column,
				Position:    change.Position,
				AfterColumn: change.AfterColumn,
			})
		case model.SchemaChangeAlterColumnType:
			if change.Column == nil {
				return nil, false, fmt.Errorf("alter-column schema change has no column")
			}
			out = append(out, ddlAction{
				Kind:        ddlActionUpdateColumn,
				Column:      *change.Column,
				Position:    change.Position,
				AfterColumn: change.AfterColumn,
			})
		case model.SchemaChangeDropColumn:
			out = append(out, ddlAction{Kind: ddlActionDropColumn, OldName: change.OldName})
		case model.SchemaChangeRenameColumn:
			out = append(out, ddlAction{
				Kind:    ddlActionRenameColumn,
				OldName: change.OldName,
				NewName: change.NewName,
			})
		case model.SchemaChangeCreateTable,
			model.SchemaChangeDropTable,
			model.SchemaChangeTruncateTable:
			// Destructive table-level changes follow Flink's lenient behavior
			// and are not applied implicitly by the Iceberg metadata applier.
		}
	}
	return out, len(out) == 0, nil
}

func isCreateTableDDL(ddl string) bool {
	low := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(ddl, "\n", " "), "\t", " "))
	return strings.HasPrefix(strings.TrimSpace(low), "create table")
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
			moveSourceSchemaColumn(out, action.Column.Name, action.Position, action.AfterColumn)
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
			moveSourceSchemaColumn(out, action.Column.Name, action.Position, action.AfterColumn)
		}
	}

	return out
}

func moveSourceSchemaColumn(schema *model.TableSchema, name string, position model.ColumnPosition, after string) {
	if schema == nil || position == "" || position == model.ColumnPositionLast {
		return
	}
	current := -1
	for idx := range schema.Columns {
		if strings.EqualFold(schema.Columns[idx].Name, name) {
			current = idx
			break
		}
	}
	if current < 0 {
		return
	}

	column := schema.Columns[current]
	schema.Columns = append(schema.Columns[:current], schema.Columns[current+1:]...)
	target := 0
	if position == model.ColumnPositionAfter {
		target = len(schema.Columns)
		for idx := range schema.Columns {
			if strings.EqualFold(schema.Columns[idx].Name, after) {
				target = idx + 1
				break
			}
		}
	}
	schema.Columns = append(schema.Columns, model.TableColumn{})
	copy(schema.Columns[target+1:], schema.Columns[target:])
	schema.Columns[target] = column
}
