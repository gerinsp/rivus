package doris

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/observability"
	"github.com/gerinsp/rivus/pkg/util"
)

type SourceConfig struct {
	HTTPHost  string `yaml:"http_host" json:"http_host"`
	Addr      string `yaml:"addr" json:"addr"`
	MySQLHost string `yaml:"mysql_host" json:"mysql_host"`
	MySQLPort int    `yaml:"mysql_port" json:"mysql_port"`

	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`

	Database string `yaml:"database" json:"database"`
	Table    string `yaml:"table" json:"table"`

	Databases  []string `yaml:"databases" json:"databases"`
	TableNames []string `yaml:"table_names" json:"table_names"`
	Tables     []string `yaml:"tables" json:"tables"`

	TableConfigs map[string]SourceTableConfig `yaml:"table_configs" json:"table_configs"`
	ChunkSize    int                          `yaml:"chunk_size" json:"chunk_size"`
}

type SourceTableConfig struct {
	Filter string `yaml:"filter" json:"filter"`
}

type Source struct {
	jobID    string
	cfg      SourceConfig
	retry    config.RetryPolicy
	db       *sql.DB
	progress connector.ProgressReporter

	skipSnapshotTables map[string]bool
}

type dorisSnapshotPlan struct {
	dbName     string
	tableName  string
	fullName   string
	columns    []string
	keyColumns []string
	filter     string
}

func NewSource(jobID string, cfg SourceConfig, retry config.RetryPolicy, progress connector.ProgressReporter) (*Source, error) {
	cfg = normalizeDorisSourceConfig(cfg)

	addr, err := dorisSourceMySQLAddr(cfg)
	if err != nil {
		return nil, err
	}

	dbForDSN := cfg.Database
	if strings.TrimSpace(dbForDSN) == "" {
		dbForDSN = "information_schema"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4,utf8&parseTime=true&interpolateParams=true&timeout=10s&readTimeout=5m&writeTimeout=5m",
		cfg.User, cfg.Password, addr, dbForDSN)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(20 * time.Minute)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("doris source mysql ping failed: %w", err)
	}

	resolveCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	expanded, err := expandDorisConfiguredTables(resolveCtx, cfg.Tables, func(ctx context.Context, dbName string) ([]string, error) {
		return listDorisBaseTables(ctx, db, dbName)
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	cfg.Tables = expanded

	return &Source{
		jobID:    jobID,
		cfg:      cfg,
		retry:    retry,
		db:       db,
		progress: progress,
	}, nil
}

func normalizeDorisSourceConfig(cfg SourceConfig) SourceConfig {
	cfg.HTTPHost = strings.TrimRight(strings.TrimSpace(cfg.HTTPHost), "/")
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	cfg.MySQLHost = strings.TrimSpace(cfg.MySQLHost)
	cfg.User = strings.TrimSpace(cfg.User)
	cfg.Database = strings.ToLower(strings.TrimSpace(cfg.Database))
	cfg.Table = strings.ToLower(strings.TrimSpace(cfg.Table))
	cfg.Databases = normalizeDorisStringSlice(cfg.Databases)
	cfg.TableNames = normalizeDorisStringSlice(cfg.TableNames)
	cfg.Tables = normalizeDorisTables(cfg)
	if cfg.MySQLPort <= 0 {
		cfg.MySQLPort = 9030
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 1000
	}
	if cfg.TableConfigs != nil {
		norm := make(map[string]SourceTableConfig, len(cfg.TableConfigs))
		for key, tableCfg := range cfg.TableConfigs {
			k := strings.ToLower(strings.TrimSpace(key))
			if k == "" {
				continue
			}
			tableCfg.Filter = normalizeDorisSnapshotFilter(tableCfg.Filter)
			norm[k] = tableCfg
		}
		cfg.TableConfigs = norm
	}
	return cfg
}

func (s *Source) SetSinkType(sinkType string) {
	for _, table := range s.cfg.Tables {
		observability.RegisterSourceTable(s.jobID, table)
		observability.SetSinkType(s.jobID, table, sinkType)
	}
}

func normalizeDorisTables(cfg SourceConfig) []string {
	out := make([]string, 0, len(cfg.Tables)+(len(cfg.Databases)*len(cfg.TableNames))+1)
	seen := make(map[string]struct{}, cap(out))
	add := func(raw string) {
		entry := strings.ToLower(strings.TrimSpace(raw))
		if entry == "" {
			return
		}
		if _, exists := seen[entry]; exists {
			return
		}
		seen[entry] = struct{}{}
		out = append(out, entry)
	}

	for _, table := range cfg.Tables {
		add(table)
	}
	for _, dbName := range cfg.Databases {
		for _, tableName := range cfg.TableNames {
			add(dbName + "." + tableName)
		}
	}
	if len(out) == 0 && cfg.Database != "" && cfg.Table != "" {
		add(cfg.Database + "." + cfg.Table)
	}
	return out
}

func normalizeDorisStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		v := strings.ToLower(strings.TrimSpace(value))
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func normalizeDorisSnapshotFilter(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, "; \t\r\n")
	if strings.HasPrefix(strings.ToLower(raw), "where ") {
		raw = strings.TrimSpace(raw[6:])
	}
	return raw
}

func dorisSourceMySQLAddr(cfg SourceConfig) (string, error) {
	if cfg.Addr != "" {
		return cfg.Addr, nil
	}

	host := cfg.MySQLHost
	if host == "" {
		host = hostFromDorisHTTPHost(cfg.HTTPHost)
	}
	if host == "" {
		return "", fmt.Errorf("doris source requires addr, mysql_host, or http_host")
	}
	if strings.Contains(host, ":") {
		return host, nil
	}
	return fmt.Sprintf("%s:%d", host, cfg.MySQLPort), nil
}

func hostFromDorisHTTPHost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.Hostname() != "" {
		return u.Hostname()
	}
	return ""
}

func expandDorisConfiguredTables(ctx context.Context, entries []string, listTables func(context.Context, string) ([]string, error)) ([]string, error) {
	out := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		dbName, tablePattern, wildcard, ok := parseDorisConfiguredTableEntry(entry)
		if !ok {
			return nil, fmt.Errorf("bad doris.tables entry: %q (expected db.table or db.*)", entry)
		}
		if !wildcard {
			full := dbName + "." + tablePattern
			if _, exists := seen[full]; exists {
				continue
			}
			seen[full] = struct{}{}
			out = append(out, full)
			continue
		}

		if tablePattern == "" {
			tablePattern = "*"
		}
		tables, err := listTables(ctx, dbName)
		if err != nil {
			return nil, fmt.Errorf("expand doris.tables wildcard %q failed: %w", dbName+"."+tablePattern, err)
		}
		matched := 0
		for _, tableName := range tables {
			if !matchDorisTableGlob(tablePattern, tableName) {
				continue
			}
			matched++
			full := dbName + "." + tableName
			if _, exists := seen[full]; exists {
				continue
			}
			seen[full] = struct{}{}
			out = append(out, full)
		}
		if matched == 0 {
			return nil, fmt.Errorf("doris.tables wildcard %q matched no tables", dbName+"."+tablePattern)
		}
	}
	return out, nil
}

func parseDorisConfiguredTableEntry(raw string) (dbName, tableName string, wildcard bool, ok bool) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) != 2 {
		return "", "", false, false
	}
	dbName = strings.ToLower(strings.TrimSpace(parts[0]))
	tableName = strings.ToLower(strings.TrimSpace(parts[1]))
	if dbName == "" || tableName == "" {
		return "", "", false, false
	}
	if tableName == "*" {
		return dbName, "", true, true
	}
	if strings.Contains(tableName, "*") {
		return dbName, tableName, true, true
	}
	return dbName, tableName, false, true
}

func matchDorisTableGlob(pattern, table string) bool {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	table = strings.ToLower(strings.TrimSpace(table))
	if pattern == "" || pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == table
	}

	expr := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", ".*") + "$"
	ok, err := regexp.MatchString(expr, table)
	return err == nil && ok
}

func listDorisBaseTables(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	const q = `
	SELECT TABLE_NAME
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = ?
	ORDER BY TABLE_NAME`

	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 16)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			out = append(out, name)
		}
	}
	return out, rows.Err()
}

func (s *Source) Run(ctx context.Context, out chan<- model.Event) error {
	return s.RunSnapshotOnly(ctx, out)
}

func (s *Source) Tables() []connector.TableRef {
	return s.tableRefs()
}

func (s *Source) tableRefs() []connector.TableRef {
	out := make([]connector.TableRef, 0, len(s.cfg.Tables))
	for _, full := range s.cfg.Tables {
		dbName, tableName, ok := splitDorisDBTable(full)
		if ok {
			out = append(out, connector.TableRef{Schema: dbName, Table: tableName})
		}
	}
	return out
}

func splitDorisDBTable(full string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(full), ".")
	if len(parts) != 2 {
		return "", "", false
	}
	dbName := strings.ToLower(strings.TrimSpace(parts[0]))
	tableName := strings.ToLower(strings.TrimSpace(parts[1]))
	return dbName, tableName, dbName != "" && tableName != ""
}

func (s *Source) RunSnapshotOnly(ctx context.Context, out chan<- model.Event) error {
	totalTables := len(s.cfg.Tables)
	s.reportProgress(connector.ProgressInfo{
		Phase:       "snapshot",
		Summary:     "Preparing Doris snapshot-only load",
		Detail:      fmt.Sprintf("Queued %d table(s) for snapshot without CDC", totalTables),
		TotalTables: totalTables,
	})

	for idx, full := range s.cfg.Tables {
		tableIndex := idx + 1
		if s.shouldSkipSnapshotTable(full) {
			s.reportProgress(connector.ProgressInfo{
				Phase:             "snapshot",
				Summary:           fmt.Sprintf("Skipping Doris snapshot table %d/%d", tableIndex, totalTables),
				Detail:            fmt.Sprintf("%s | source and target row counts match", full),
				CurrentTable:      full,
				CurrentTableIndex: tableIndex,
				CompletedTables:   tableIndex,
				TotalTables:       totalTables,
			})
			log.Printf("[doris-source][job %s] snapshot skip %s because source and target row counts match", s.jobID, full)
			continue
		}

		dbName, tableName, ok := splitDorisDBTable(full)
		if !ok {
			return fmt.Errorf("bad doris.tables entry: %q (expected db.table)", full)
		}
		if err := s.runSnapshotTable(ctx, out, dbName, tableName, tableIndex, totalTables); err != nil {
			return err
		}
	}

	s.reportProgress(connector.ProgressInfo{
		Phase:           "snapshot_complete",
		Summary:         "Doris snapshot-only load complete",
		Detail:          fmt.Sprintf("Loaded %d table(s) without CDC", totalTables),
		CompletedTables: totalTables,
		TotalTables:     totalTables,
	})
	return nil
}

func (s *Source) runSnapshotTable(ctx context.Context, out chan<- model.Event, dbName, tableName string, tableIndex, totalTables int) error {
	plan, err := s.buildSnapshotPlan(ctx, dbName, tableName)
	if err != nil {
		return err
	}

	chunk := s.cfg.ChunkSize
	if chunk <= 0 {
		chunk = 1000
	}

	log.Printf("[doris-source][job %s] snapshot start %s chunk=%d", s.jobID, plan.fullName, chunk)
	var emitted int64
	for {
		rows, err := s.fetchSnapshotChunk(ctx, plan, chunk, emitted)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		now := time.Now()
		for i, row := range rows {
			ev := model.Event{
				Type:      model.EventTypeInsert,
				Schema:    dbName,
				Table:     tableName,
				Data:      row,
				Timestamp: now,
			}
			if err := s.emitSnapshotEvent(ctx, out, ev, plan.fullName, tableIndex, totalTables, emitted+int64(i)); err != nil {
				return err
			}
		}
		emitted += int64(len(rows))
		s.reportSnapshotProgress(plan.fullName, tableIndex, totalTables, emitted)
		log.Printf("[doris-source][job %s] snapshot chunk done table=%s rows=%d total=%d", s.jobID, plan.fullName, len(rows), emitted)
	}

	log.Printf("[doris-source][job %s] snapshot finished %s total=%d", s.jobID, plan.fullName, emitted)
	s.reportProgress(connector.ProgressInfo{
		Phase:             "snapshot",
		Summary:           fmt.Sprintf("Doris snapshot table %d/%d complete", tableIndex, totalTables),
		Detail:            fmt.Sprintf("%s | %d rows emitted", plan.fullName, emitted),
		CurrentTable:      plan.fullName,
		CurrentTableIndex: tableIndex,
		CompletedTables:   tableIndex,
		TotalTables:       totalTables,
		CurrentTableRows:  emitted,
	})
	return nil
}

func (s *Source) buildSnapshotPlan(ctx context.Context, dbName, tableName string) (*dorisSnapshotPlan, error) {
	schema, err := s.FetchSchema(ctx, dbName, tableName)
	if err != nil {
		return nil, err
	}
	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("doris snapshot requires schema for %s.%s", dbName, tableName)
	}

	columns := make([]string, 0, len(schema.Columns))
	keyColumns := make([]string, 0)
	for _, col := range schema.Columns {
		columns = append(columns, col.Name)
		if col.IsPK {
			keyColumns = append(keyColumns, col.Name)
		}
	}
	orderCols := keyColumns
	if len(orderCols) == 0 {
		orderCols = columns[:1]
	}

	return &dorisSnapshotPlan{
		dbName:     dbName,
		tableName:  tableName,
		fullName:   dbName + "." + tableName,
		columns:    columns,
		keyColumns: orderCols,
		filter:     s.snapshotFilterForTable(dbName, tableName),
	}, nil
}

func (s *Source) fetchSnapshotChunk(ctx context.Context, plan *dorisSnapshotPlan, limit int, offset int64) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s",
		quoteDorisIdentifier(plan.dbName),
		quoteDorisIdentifier(plan.tableName),
	)
	if plan.filter != "" {
		query += " WHERE (" + plan.filter + ")"
	}
	query += fmt.Sprintf(" ORDER BY %s LIMIT ? OFFSET ?", quotedDorisColumnList(plan.keyColumns))

	var out []map[string]interface{}
	err := util.RetryWithBackoff(ctx, s.retry, func() error {
		rows, err := s.db.QueryContext(ctx, query, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}

		chunkRows := make([]map[string]interface{}, 0, limit)
		for rows.Next() {
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			data := make(map[string]interface{}, len(cols))
			for i, col := range cols {
				data[col] = cloneSQLValue(values[i])
			}
			chunkRows = append(chunkRows, data)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		out = chunkRows
		return nil
	})
	return out, err
}

func cloneSQLValue(value interface{}) interface{} {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...)
	default:
		return v
	}
}

func (s *Source) emitSnapshotEvent(ctx context.Context, out chan<- model.Event, ev model.Event, tableName string, tableIndex, totalTables int, rowsEmitted int64) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case out <- ev:
		return nil
	case <-timer.C:
		log.Printf("[WARN][doris-source][job %s] snapshot backpressure table=%s rows_emitted=%d waiting_for_sink=true", s.jobID, tableName, rowsEmitted)
		s.reportProgress(connector.ProgressInfo{
			Phase:             "snapshot",
			Summary:           fmt.Sprintf("Waiting for sink flush on Doris table %d/%d", tableIndex, totalTables),
			Detail:            fmt.Sprintf("%s | %d rows emitted, sink is slower than snapshot reader", tableName, rowsEmitted),
			CurrentTable:      tableName,
			CurrentTableIndex: tableIndex,
			CompletedTables:   tableIndex - 1,
			TotalTables:       totalTables,
			CurrentTableRows:  rowsEmitted,
		})
		select {
		case out <- ev:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Source) reportSnapshotProgress(tableName string, tableIndex, totalTables int, rows int64) {
	s.reportProgress(connector.ProgressInfo{
		Phase:             "snapshot",
		Summary:           fmt.Sprintf("Loading Doris snapshot table %d/%d", tableIndex, totalTables),
		Detail:            fmt.Sprintf("%s | %d rows emitted", tableName, rows),
		CurrentTable:      tableName,
		CurrentTableIndex: tableIndex,
		CompletedTables:   tableIndex - 1,
		TotalTables:       totalTables,
		CurrentTableRows:  rows,
	})
}

func (s *Source) reportProgress(info connector.ProgressInfo) {
	if s.progress != nil {
		s.progress(info)
	}
}

func (s *Source) FetchSchema(ctx context.Context, dbName, tableName string) (*model.TableSchema, error) {
	query := fmt.Sprintf("DESC %s.%s", quoteDorisIdentifier(dbName), quoteDorisIdentifier(tableName))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []model.TableColumn
	for rows.Next() {
		values, err := scanNullableStrings(rows)
		if err != nil {
			return nil, err
		}
		if len(values) < 2 {
			return nil, fmt.Errorf("unexpected DESC result for %s.%s: got %d columns", dbName, tableName, len(values))
		}
		field := values[0]
		typeText := values[1]
		nullText := ""
		keyText := ""
		if len(values) > 2 {
			nullText = values[2]
		}
		if len(values) > 3 {
			keyText = values[3]
		}
		if strings.TrimSpace(field) == "" {
			continue
		}
		cols = append(cols, dorisColumnFromDesc(field, typeText, nullText, keyText))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &model.TableSchema{
		SchemaName: dbName,
		TableName:  tableName,
		Columns:    cols,
	}, nil
}

func scanNullableStrings(rows *sql.Rows) ([]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]sql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	out := make([]string, len(values))
	for i, value := range values {
		if value.Valid {
			out[i] = value.String
		}
	}
	return out, nil
}

func dorisColumnFromDesc(field, typeText, nullText, keyText string) model.TableColumn {
	dataType, charLen, precision, scale := normalizeDorisType(typeText)
	return model.TableColumn{
		Name:       strings.TrimSpace(field),
		DataType:   dataType,
		ColumnType: strings.ToLower(strings.TrimSpace(typeText)),
		CharMaxLen: charLen,
		NumPrec:    precision,
		NumScale:   scale,
		IsNullable: dorisNullability(nullText),
		IsPK:       dorisKeyFlag(keyText),
	}
}

func normalizeDorisType(typeText string) (string, *int64, *int64, *int64) {
	raw := strings.ToLower(strings.TrimSpace(typeText))
	base := raw
	if idx := strings.IndexByte(base, '('); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}

	var charLen *int64
	var precision *int64
	var scale *int64

	switch base {
	case "varchar", "char":
		if vals := parseTypeParams(raw); len(vals) > 0 && vals[0] > 0 {
			charLen = int64Ptr(int64(vals[0]))
		}
		return base, charLen, nil, nil
	case "string":
		return "text", nil, nil, nil
	case "boolean", "bool":
		return "boolean", nil, nil, nil
	case "tinyint", "smallint", "int", "integer", "bigint", "float", "double", "date":
		if base == "integer" {
			return "int", nil, nil, nil
		}
		return base, nil, nil, nil
	case "largeint":
		precision = int64Ptr(38)
		scale = int64Ptr(0)
		return "decimal", nil, precision, scale
	case "decimal", "decimalv2", "decimalv3", "numeric":
		vals := parseTypeParams(raw)
		if len(vals) > 0 && vals[0] > 0 {
			precision = int64Ptr(int64(vals[0]))
		}
		if len(vals) > 1 && vals[1] >= 0 {
			scale = int64Ptr(int64(vals[1]))
		}
		return "decimal", nil, precision, scale
	case "datetime", "datetimev2", "timestamp":
		return "datetime", nil, nil, nil
	case "datev2":
		return "date", nil, nil, nil
	default:
		return "text", nil, nil, nil
	}
}

func parseTypeParams(typeText string) []int {
	left := strings.IndexByte(typeText, '(')
	right := strings.LastIndexByte(typeText, ')')
	if left < 0 || right <= left+1 {
		return nil
	}
	parts := strings.Split(typeText[left+1:right], ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return out
		}
		out = append(out, n)
	}
	return out
}

func int64Ptr(v int64) *int64 {
	return &v
}

func dorisNullability(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "no", "false", "not null":
		return false
	default:
		return true
	}
}

func dorisKeyFlag(raw string) bool {
	key := strings.ToLower(strings.TrimSpace(raw))
	return key == "pri" || key == "key" || key == "true" || key == "yes" || strings.Contains(key, "key")
}

func quotedDorisColumnList(cols []string) string {
	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted = append(quoted, quoteDorisIdentifier(col))
	}
	return strings.Join(quoted, ", ")
}

func (s *Source) CountRows(ctx context.Context, dbName, tableName string) (int64, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s",
		quoteDorisIdentifier(dbName),
		quoteDorisIdentifier(tableName),
	)
	if filter := s.snapshotFilterForTable(dbName, tableName); filter != "" {
		query += " WHERE (" + filter + ")"
	}

	var count int64
	if err := s.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Source) snapshotFilterForTable(dbName, tableName string) string {
	if len(s.cfg.TableConfigs) == 0 {
		return ""
	}

	full := strings.ToLower(strings.TrimSpace(dbName + "." + tableName))
	if tableCfg, ok := s.cfg.TableConfigs[full]; ok {
		return strings.TrimSpace(tableCfg.Filter)
	}
	short := strings.ToLower(strings.TrimSpace(tableName))
	if tableCfg, ok := s.cfg.TableConfigs[short]; ok {
		return strings.TrimSpace(tableCfg.Filter)
	}
	return ""
}

func (s *Source) SkipSnapshotTables(tables []connector.TableRef) {
	if len(tables) == 0 {
		s.skipSnapshotTables = nil
		return
	}
	s.skipSnapshotTables = make(map[string]bool, len(tables))
	for _, table := range tables {
		key := strings.ToLower(strings.TrimSpace(table.Schema + "." + table.Table))
		if key != "." && key != "" {
			s.skipSnapshotTables[key] = true
		}
	}
}

func (s *Source) shouldSkipSnapshotTable(fullName string) bool {
	if len(s.skipSnapshotTables) == 0 {
		return false
	}
	return s.skipSnapshotTables[strings.ToLower(strings.TrimSpace(fullName))]
}
