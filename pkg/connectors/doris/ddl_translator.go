package doris

import (
	"fmt"
	"strconv"
	"strings"
)

func (s *Sink) translateMySQLDDLToDoris(mysqlDDL string, targetDB, targetTbl string) ([]string, bool, string) {
	ddl := strings.TrimSpace(mysqlDDL)
	if ddl == "" {
		return nil, false, "empty ddl"
	}
	ddl = strings.TrimSuffix(ddl, ";")

	low := strings.ToLower(ddl)
	db := targetDB
	tbl := targetTbl
	targetDBLow := strings.ToLower(db)
	targetTblLow := strings.ToLower(tbl)

	mentionsTarget := func() bool {
		d := strings.ToLower(ddl)
		if strings.Contains(d, "`"+targetDBLow+"`.`"+targetTblLow+"`") {
			return true
		}
		if strings.Contains(d, targetDBLow+"."+targetTblLow) {
			return true
		}
		if strings.Contains(d, "`"+targetTblLow+"`") {
			return true
		}
		return strings.Contains(" "+d+" ", " "+targetTblLow+" ")
	}

	// CREATE TABLE -> skip (kamu handle via EnsureTable)
	if strings.HasPrefix(low, "create table") {
		if mentionsTarget() {
			return nil, false, "skip CREATE TABLE from source; handled by EnsureTable()"
		}
		return nil, false, "not target"
	}

	if strings.HasPrefix(low, "drop table") {
		if !mentionsTarget() {
			return nil, false, "not target"
		}
		return []string{fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", db, tbl)}, true, ""
	}

	if strings.HasPrefix(low, "truncate table") {
		if !mentionsTarget() {
			return nil, false, "not target"
		}
		return []string{fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", db, tbl)}, true, ""
	}

	if !strings.HasPrefix(low, "alter table") {
		return nil, false, "ddl not supported"
	}

	if !mentionsTarget() {
		return nil, false, "not target"
	}

	// ALTER TABLE <ident> <actions...>
	parts := strings.Fields(ddl)
	if len(parts) < 4 {
		return nil, false, "alter table too short"
	}
	actionsRaw := strings.Join(parts[3:], " ")

	// split multi actions by comma at top-level (not in parentheses)
	actions := splitTopLevelComma(actionsRaw)

	var out []string
	for _, act := range actions {
		act = strings.TrimSpace(act)
		if act == "" {
			continue
		}
		stmt, ok, reason := s.translateAlterAction(act, db, tbl)
		if !ok {
			// kalau ada 1 action gagal, kita skip semua biar tidak half-applied
			return nil, false, "unsupported alter action: " + reason + " | act=" + act
		}
		out = append(out, stmt...)
	}
	if len(out) == 0 {
		return nil, false, "no translated statements"
	}
	return out, true, ""
}

// translateAlterAction mengubah 1 aksi ALTER (ADD/DROP/MODIFY/CHANGE/RENAME TO)
func (s *Sink) translateAlterAction(action string, db, tbl string) ([]string, bool, string) {
	low := strings.ToLower(strings.TrimSpace(action))

	// ADD [COLUMN] col type ...
	if strings.HasPrefix(low, "add ") || strings.HasPrefix(low, "add column ") {
		act := strings.TrimSpace(action)
		actLow := strings.ToLower(act)

		if strings.HasPrefix(actLow, "add column ") {
			act = strings.TrimSpace(act[len("add column "):])
		} else {
			act = strings.TrimSpace(act[len("add "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}

		colName, rest, ok := splitFirstIdent(act)
		if !ok {
			return nil, false, "cannot parse add column name"
		}
		colType, notNull := parseMySQLColumnTypeAndNullability(rest)
		if colType == "" {
			return nil, false, "cannot parse add column type"
		}
		dorisType := mapMySQLTypeStrToDoris(colType)

		stmt := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s", db, tbl, colName, dorisType)
		if notNull {
			stmt += " NOT NULL"
		}
		return []string{stmt}, true, ""
	}

	// DROP [COLUMN] col
	if strings.HasPrefix(low, "drop ") || strings.HasPrefix(low, "drop column ") {
		act := strings.TrimSpace(action)
		actLow := strings.ToLower(act)

		if strings.HasPrefix(actLow, "drop column ") {
			act = strings.TrimSpace(act[len("drop column "):])
		} else {
			act = strings.TrimSpace(act[len("drop "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}

		colName, _, ok := splitFirstIdent(act)
		if !ok {
			return nil, false, "cannot parse drop column name"
		}
		return []string{fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP COLUMN `%s`", db, tbl, colName)}, true, ""
	}

	// MODIFY [COLUMN] col type ...
	if strings.HasPrefix(low, "modify ") || strings.HasPrefix(low, "modify column ") {
		act := strings.TrimSpace(action)
		actLow := strings.ToLower(act)

		if strings.HasPrefix(actLow, "modify column ") {
			act = strings.TrimSpace(act[len("modify column "):])
		} else {
			act = strings.TrimSpace(act[len("modify "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}

		colName, rest, ok := splitFirstIdent(act)
		if !ok {
			return nil, false, "cannot parse modify column name"
		}
		colType, notNull := parseMySQLColumnTypeAndNullability(rest)
		if colType == "" {
			return nil, false, "cannot parse modify column type"
		}
		dorisType := mapMySQLTypeStrToDoris(colType)

		stmt := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `%s` %s", db, tbl, colName, dorisType)
		if notNull {
			stmt += " NOT NULL"
		}
		return []string{stmt}, true, ""
	}

	// CHANGE [COLUMN] old new type ...
	// Doris v3: RENAME COLUMN + MODIFY COLUMN
	if strings.HasPrefix(low, "change ") || strings.HasPrefix(low, "change column ") {
		act := strings.TrimSpace(action)
		actLow := strings.ToLower(act)

		if strings.HasPrefix(actLow, "change column ") {
			act = strings.TrimSpace(act[len("change column "):])
		} else {
			act = strings.TrimSpace(act[len("change "):])
			if strings.HasPrefix(strings.ToLower(act), "column ") {
				act = strings.TrimSpace(act[len("column "):])
			}
		}

		oldName, rest, ok := splitFirstIdent(act)
		if !ok {
			return nil, false, "cannot parse change old name"
		}
		newName, rest2, ok := splitFirstIdent(rest)
		if !ok {
			return nil, false, "cannot parse change new name"
		}
		colType, notNull := parseMySQLColumnTypeAndNullability(rest2)
		if colType == "" {
			return nil, false, "cannot parse change column type"
		}
		dorisType := mapMySQLTypeStrToDoris(colType)

		var stmts []string
		if strings.ToLower(oldName) != strings.ToLower(newName) {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE `%s`.`%s` RENAME COLUMN `%s` `%s`", db, tbl, oldName, newName))
		}
		mod := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `%s` %s", db, tbl, newName, dorisType)
		if notNull {
			mod += " NOT NULL"
		}
		stmts = append(stmts, mod)
		return stmts, true, ""
	}

	// RENAME TO new_table
	if strings.HasPrefix(low, "rename to ") {
		act := strings.TrimSpace(action[len("rename to "):])
		newTable, _, ok := splitFirstIdent(act)
		if !ok {
			return nil, false, "cannot parse rename target"
		}
		// Doris: ALTER TABLE db.old RENAME new
		return []string{fmt.Sprintf("ALTER TABLE `%s`.`%s` RENAME `%s`", db, tbl, newTable)}, true, ""
	}

	return nil, false, "unknown action"
}

func splitTopLevelComma(s string) []string {
	var out []string
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

	// backtick identifier: `col_name`
	if strings.HasPrefix(s, "`") {
		end := strings.Index(s[1:], "`")
		if end < 0 {
			return "", "", false
		}
		name := s[1 : 1+end]
		rest := strings.TrimSpace(s[1+end+1:])
		return name, rest, true
	}

	// plain identifier sampai whitespace
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

	// NOT NULL?
	notNull := strings.Contains(low, " not null")

	// type = token pertama (bisa punya "(...)" )
	// contoh: "varchar(100) not null default ''"
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
				t := strings.TrimSpace(rest[:i])
				return t, notNull
			}
		}
	}
	return strings.TrimSpace(rest), notNull
}

// mapMySQLTypeStrToDoris dipakai khusus untuk DDL translator (input-nya type string dari DDL).
// Contoh input: "varchar(100)", "longtext", "decimal(10,2)", "int", "bigint unsigned"
func mapMySQLTypeStrToDoris(mysqlType string) string {
	t := strings.ToLower(strings.TrimSpace(mysqlType))
	if t == "" {
		return "STRING"
	}

	// buang atribut yang sering muncul di MySQL
	t = strings.ReplaceAll(t, "unsigned", "")
	t = strings.ReplaceAll(t, "zerofill", "")
	t = strings.TrimSpace(t)

	// text family
	switch t {
	case "text", "tinytext", "mediumtext", "longtext":
		return "STRING"
	}

	// char/varchar with length
	if strings.HasPrefix(t, "varchar(") || strings.HasPrefix(t, "char(") {
		l := strings.IndexByte(t, '(')
		r := strings.IndexByte(t, ')')
		if l > 0 && r > l+1 {
			nStr := strings.TrimSpace(t[l+1 : r])
			if n, err := strconv.Atoi(nStr); err == nil && n > 0 {
				if n > 65533 {
					return "STRING"
				}
				return fmt.Sprintf("VARCHAR(%d)", n)
			}
		}
		return "VARCHAR(1024)"
	}

	// plain char/varchar
	if t == "varchar" || t == "char" {
		return "VARCHAR(1024)"
	}

	// decimal / numeric
	if strings.HasPrefix(t, "decimal(") || strings.HasPrefix(t, "numeric(") {
		l := strings.IndexByte(t, '(')
		r := strings.IndexByte(t, ')')
		if l > 0 && r > l+1 {
			inner := strings.TrimSpace(t[l+1 : r]) // "10,2"
			// validasi ringan
			parts := strings.Split(inner, ",")
			if len(parts) == 1 {
				if p, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil && p > 0 {
					return decimalTypeForDoris(int64(p), 0)
				}
			}
			if len(parts) == 2 {
				if p, err1 := strconv.Atoi(strings.TrimSpace(parts[0])); err1 == nil && p > 0 {
					if s, err2 := strconv.Atoi(strings.TrimSpace(parts[1])); err2 == nil && s >= 0 {
						return decimalTypeForDoris(int64(p), int64(s))
					}
				}
			}
		}
		return "DECIMAL(27,9)"
	}

	switch t {
	case "int", "bigint", "smallint", "tinyint", "mediumint":
		return "BIGINT"
	case "float", "double":
		return "DOUBLE"
	case "datetime", "timestamp":
		return "DATETIME"
	case "date":
		return "DATE"
	default:
		return "STRING"
	}
}
