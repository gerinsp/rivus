package schemachange

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	parsermysql "github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"

	"github.com/gerinsp/rivus/pkg/model"
)

// ParseMySQLDDL converts source DDL into sink-independent schema events.
// Unsupported physical changes (indexes, constraints, partitions, options)
// intentionally produce no events because they do not change the row schema.
func ParseMySQLDDL(raw string) ([]model.SchemaChange, error) {
	ddl := strings.TrimSpace(raw)
	if ddl == "" {
		return nil, nil
	}

	stmt, err := parser.New().ParseOneStmt(ddl, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse mysql ddl: %w", err)
	}

	switch node := stmt.(type) {
	case *ast.CreateTableStmt:
		return []model.SchemaChange{{Type: model.SchemaChangeCreateTable}}, nil
	case *ast.DropTableStmt:
		return []model.SchemaChange{{Type: model.SchemaChangeDropTable}}, nil
	case *ast.TruncateTableStmt:
		return []model.SchemaChange{{Type: model.SchemaChangeTruncateTable}}, nil
	case *ast.AlterTableStmt:
		return mysqlAlterTableChanges(node)
	case *ast.RenameTableStmt:
		// Flink CDC does not expose a rename-table schema event. Ignore it
		// rather than interpreting it as a row-schema mutation.
		return []model.SchemaChange{}, nil
	default:
		return []model.SchemaChange{}, nil
	}
}

func mysqlAlterTableChanges(stmt *ast.AlterTableStmt) ([]model.SchemaChange, error) {
	changes := make([]model.SchemaChange, 0, len(stmt.Specs))
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			for _, def := range spec.NewColumns {
				column, err := mysqlColumn(def)
				if err != nil {
					return nil, err
				}
				position, after := mysqlColumnPosition(spec.Position)
				if position == "" {
					position = model.ColumnPositionLast
				}
				changes = append(changes, model.SchemaChange{
					Type:        model.SchemaChangeAddColumn,
					Column:      &column,
					Position:    position,
					AfterColumn: after,
				})
			}

		case ast.AlterTableModifyColumn:
			if len(spec.NewColumns) == 0 {
				return nil, fmt.Errorf("mysql MODIFY COLUMN has no column definition")
			}
			column, err := mysqlColumn(spec.NewColumns[0])
			if err != nil {
				return nil, err
			}
			position, after := mysqlColumnPosition(spec.Position)
			changes = append(changes, model.SchemaChange{
				Type:        model.SchemaChangeAlterColumnType,
				Column:      &column,
				Position:    position,
				AfterColumn: after,
			})

		case ast.AlterTableChangeColumn:
			if spec.OldColumnName == nil || len(spec.NewColumns) == 0 {
				return nil, fmt.Errorf("mysql CHANGE COLUMN is incomplete")
			}
			column, err := mysqlColumn(spec.NewColumns[0])
			if err != nil {
				return nil, err
			}
			oldName := spec.OldColumnName.Name.O
			if !strings.EqualFold(oldName, column.Name) {
				changes = append(changes, model.SchemaChange{
					Type:    model.SchemaChangeRenameColumn,
					OldName: oldName,
					NewName: column.Name,
				})
			}
			position, after := mysqlColumnPosition(spec.Position)
			changes = append(changes, model.SchemaChange{
				Type:        model.SchemaChangeAlterColumnType,
				Column:      &column,
				Position:    position,
				AfterColumn: after,
			})

		case ast.AlterTableDropColumn:
			if spec.OldColumnName == nil {
				return nil, fmt.Errorf("mysql DROP COLUMN has no column name")
			}
			changes = append(changes, model.SchemaChange{
				Type:    model.SchemaChangeDropColumn,
				OldName: spec.OldColumnName.Name.O,
			})

		case ast.AlterTableRenameColumn:
			if spec.OldColumnName == nil || spec.NewColumnName == nil {
				return nil, fmt.Errorf("mysql RENAME COLUMN is incomplete")
			}
			changes = append(changes, model.SchemaChange{
				Type:    model.SchemaChangeRenameColumn,
				OldName: spec.OldColumnName.Name.O,
				NewName: spec.NewColumnName.Name.O,
			})
		}
	}
	return changes, nil
}

func mysqlColumn(def *ast.ColumnDef) (model.TableColumn, error) {
	if def == nil || def.Name == nil || def.Tp == nil {
		return model.TableColumn{}, fmt.Errorf("mysql column definition is incomplete")
	}

	columnType := strings.ToLower(def.Tp.String())
	dataType := strings.ToLower(parsertypes.TypeToStr(def.Tp.GetType(), def.Tp.GetCharset()))
	column := model.TableColumn{
		Name:       def.Name.Name.O,
		DataType:   dataType,
		ColumnType: columnType,
		IsNullable: true,
	}

	switch def.Tp.GetType() {
	case parsermysql.TypeString, parsermysql.TypeVarchar, parsermysql.TypeVarString,
		parsermysql.TypeTinyBlob, parsermysql.TypeBlob, parsermysql.TypeMediumBlob, parsermysql.TypeLongBlob:
		if length := def.Tp.GetFlen(); length > 0 {
			value := int64(length)
			column.CharMaxLen = &value
		}
	case parsermysql.TypeNewDecimal:
		if precision := def.Tp.GetFlen(); precision > 0 {
			value := int64(precision)
			column.NumPrec = &value
		}
		if scale := def.Tp.GetDecimal(); scale >= 0 {
			value := int64(scale)
			column.NumScale = &value
		}
	}

	for _, option := range def.Options {
		switch option.Tp {
		case ast.ColumnOptionNotNull:
			column.IsNullable = false
		case ast.ColumnOptionNull:
			column.IsNullable = true
		case ast.ColumnOptionPrimaryKey:
			column.IsPK = true
			column.IsNullable = false
		}
	}
	return column, nil
}

func mysqlColumnPosition(position *ast.ColumnPosition) (model.ColumnPosition, string) {
	if position == nil {
		return "", ""
	}
	switch position.Tp {
	case ast.ColumnPositionFirst:
		return model.ColumnPositionFirst, ""
	case ast.ColumnPositionAfter:
		if position.RelativeColumn != nil {
			return model.ColumnPositionAfter, position.RelativeColumn.Name.O
		}
	}
	return "", ""
}

func MySQLColumnTypeHasWidth(columnType, typeName string, width int) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(columnType), ""))
	return strings.HasPrefix(normalized, fmt.Sprintf("%s(%d)", typeName, width))
}
