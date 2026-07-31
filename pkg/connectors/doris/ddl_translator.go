package doris

import (
	"fmt"
	"strings"

	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/schemachange"
)

func (s *Sink) translateMySQLDDLToDoris(mysqlDDL string, targetDB, targetTbl string) ([]string, bool, string) {
	if strings.TrimSpace(mysqlDDL) == "" {
		return nil, false, "empty ddl"
	}
	changes, err := schemachange.ParseMySQLDDL(mysqlDDL)
	if err != nil {
		return nil, false, err.Error()
	}
	return s.translateSchemaChangesToDoris(changes, targetDB, targetTbl)
}

func (s *Sink) translateSchemaChangesToDoris(changes []model.SchemaChange, db, tbl string) ([]string, bool, string) {
	out := make([]string, 0, len(changes))
	for _, change := range changes {
		switch change.Type {
		case model.SchemaChangeCreateTable:
			return nil, false, "skip CREATE TABLE from source; handled by EnsureTable()"
		case model.SchemaChangeDropTable:
			out = append(out, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", db, tbl))
		case model.SchemaChangeTruncateTable:
			out = append(out, fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", db, tbl))
		case model.SchemaChangeAddColumn:
			if change.Column == nil {
				return nil, false, "add-column schema change has no column"
			}
			stmt := fmt.Sprintf(
				"ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s NULL",
				db,
				tbl,
				change.Column.Name,
				mapMySQLColumnToDoris(*change.Column, false),
			)
			out = append(out, appendDorisColumnPosition(stmt, change))
		case model.SchemaChangeDropColumn:
			out = append(out, fmt.Sprintf(
				"ALTER TABLE `%s`.`%s` DROP COLUMN `%s`",
				db,
				tbl,
				change.OldName,
			))
		case model.SchemaChangeRenameColumn:
			out = append(out, fmt.Sprintf(
				"ALTER TABLE `%s`.`%s` RENAME COLUMN `%s` `%s`",
				db,
				tbl,
				change.OldName,
				change.NewName,
			))
		case model.SchemaChangeAlterColumnType:
			if change.Column == nil {
				return nil, false, "alter-column schema change has no column"
			}
			stmt := fmt.Sprintf(
				"ALTER TABLE `%s`.`%s` MODIFY COLUMN `%s` %s",
				db,
				tbl,
				change.Column.Name,
				mapMySQLColumnToDoris(*change.Column, false),
			)
			if change.Column.IsNullable {
				stmt += " NULL"
			} else {
				stmt += " NOT NULL"
			}
			out = append(out, appendDorisColumnPosition(stmt, change))
		}
	}
	if len(out) == 0 {
		return nil, false, "no row-schema changes"
	}
	return out, true, ""
}

func appendDorisColumnPosition(stmt string, change model.SchemaChange) string {
	switch change.Position {
	case model.ColumnPositionFirst:
		return stmt + " FIRST"
	case model.ColumnPositionAfter:
		if strings.TrimSpace(change.AfterColumn) != "" {
			return stmt + fmt.Sprintf(" AFTER `%s`", change.AfterColumn)
		}
	}
	return stmt
}
