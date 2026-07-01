package doris

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/observability"
	"github.com/gerinsp/rivus/pkg/util"
)

// =======================
// Airflow-compatible CSV
// =======================

// FIELD_SEP di file: ASCII 0x1F (Unit Separator)
const fieldSep = "\x1f"

// Representasi aman untuk HTTP header Doris
const headerFieldSep = "\\x1f"

// Escape char yang "dibersihkan" dari konten (Airflow pakai ^ untuk escape file,
// tapi di Go kita pilih: jangan pakai quoting/escaping sama sekali, jadi konten dibersihkan).
const escapeChar = "^"

const (
	streamLoadBufferSize     = 64 * 1024
	retainedBatchCapLimit    = 1024
	dorisMaxVarcharLen       = 65533
	dorisMaxDecimalPrecision = 38
)

// =======================

var dorisColumnNamePattern = regexp.MustCompile(`^[.a-zA-Z0-9_+\-/?@#$%^&*"\s,:]{1,256}$`)

type columnBinding struct {
	Source string
	Target string
}

type Sink struct {
	jobID      string
	stateKey   string
	offsetSto  meta.OffsetStore
	cfg        config.DorisConfig
	retry      config.RetryPolicy
	httpClient *http.Client
	sqlDB      *sql.DB

	mu             sync.RWMutex
	columns        map[string][]string        // key "db.table" -> target cols order
	columnBindings map[string][]columnBinding // key "db.table" -> source->target cols order

	maxLen   map[string]map[string]int  // "db.table" -> col -> max chars (varchar/char)
	isString map[string]map[string]bool // "db.table" -> col -> string-ish
}

type loadStatus struct {
	State  string
	Reason string
	Raw    map[string]string // opsional untuk debug
}

type dorisFlushCounter struct {
	events  int
	rows    int
	deletes int
}

func NewSink(jobID, stateKey string, cfg config.DorisConfig, retry config.RetryPolicy, offsetSto meta.OffsetStore) (*Sink, error) {
	httpClient := &http.Client{Timeout: 60 * time.Second}

	// MySQL FE host buat Exec DDL (biasanya 9030)
	host := strings.TrimPrefix(cfg.HTTPHost, "http://")
	host = strings.TrimPrefix(host, "https://")
	if i := strings.Index(host, "/"); i >= 0 {
		host = host[:i]
	}
	if i := strings.Index(host, ":"); i >= 0 {
		host = host[:i]
	}

	mysqlPort := cfg.MySQLPort
	if mysqlPort <= 0 {
		mysqlPort = 9030
	}
	mysqlFE := fmt.Sprintf("%s:%d", host, mysqlPort)

	// Doris MySQL protocol needs a database in DSN; pick a reasonable default.
	dbForDSN := cfg.DefaultDatabase
	if dbForDSN == "" {
		dbForDSN = cfg.Database
	}
	if dbForDSN == "" {
		dbForDSN = "information_schema"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4,utf8&parseTime=true&timeout=10s&readTimeout=30s&writeTimeout=30s",
		cfg.User, cfg.Password, mysqlFE, dbForDSN)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("doris mysql(9030) ping failed: %w", err)
	}

	return &Sink{
		jobID:          jobID,
		stateKey:       stateKey,
		offsetSto:      offsetSto,
		cfg:            cfg,
		retry:          retry,
		httpClient:     httpClient,
		sqlDB:          db,
		columns:        make(map[string][]string),
		columnBindings: make(map[string][]columnBinding),
		maxLen:         make(map[string]map[string]int),
		isString:       make(map[string]map[string]bool),
	}, nil
}

func (s *Sink) checkpointKey() string {
	if strings.TrimSpace(s.stateKey) != "" {
		return s.stateKey
	}
	return s.jobID
}

func keyCompatibleStringType(c model.TableColumn) string {
	if c.CharMaxLen != nil && *c.CharMaxLen > 0 {
		n := *c.CharMaxLen
		if n > dorisMaxVarcharLen {
			n = dorisMaxVarcharLen
		}
		return fmt.Sprintf("VARCHAR(%d)", n)
	}
	return "VARCHAR(1024)"
}

func normalizedDecimalPrecisionScale(precision, scale int64) (int64, int64) {
	if precision <= 0 {
		return 27, 9
	}
	if scale < 0 {
		scale = 0
	}
	if scale > precision {
		scale = precision
	}
	return precision, scale
}

func oversizedDecimalFallbackType(precision, scale int64) string {
	length := precision + 1 // sign
	if scale > 0 {
		length++ // decimal point
	}
	if length <= 0 {
		length = 1024
	}
	if length > dorisMaxVarcharLen {
		length = dorisMaxVarcharLen
	}
	return fmt.Sprintf("VARCHAR(%d)", length)
}

func decimalTypeForDoris(precision, scale int64) string {
	precision, scale = normalizedDecimalPrecisionScale(precision, scale)
	if precision > dorisMaxDecimalPrecision {
		return oversizedDecimalFallbackType(precision, scale)
	}
	return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
}

func mapMySQLColumnToDoris(c model.TableColumn, keyColumn bool) string {
	t := strings.ToLower(strings.TrimSpace(c.DataType))

	switch t {
	// integer family
	case "int", "bigint", "smallint", "tinyint", "mediumint":
		return "BIGINT"

	// float/double
	case "float", "double":
		if keyColumn {
			return "DECIMAL(27,9)"
		}
		return "DOUBLE"

	// decimal/numeric: ikut precision/scale kalau ada
	case "decimal", "numeric":
		if c.NumPrec != nil {
			scale := int64(0)
			if c.NumScale != nil {
				scale = *c.NumScale
			}
			return decimalTypeForDoris(*c.NumPrec, scale)
		}
		// fallback aman
		return "DECIMAL(27,9)"

	case "datetime", "timestamp":
		return "DATETIME"
	case "date":
		return "DATE"

	// char/varchar: ikut length
	case "char", "varchar":
		if c.CharMaxLen != nil && *c.CharMaxLen > 0 {
			// Doris VARCHAR ada limit max; key column tidak boleh STRING.
			if *c.CharMaxLen > dorisMaxVarcharLen {
				if keyColumn {
					return keyCompatibleStringType(c)
				}
				return "STRING"
			}
			return fmt.Sprintf("VARCHAR(%d)", *c.CharMaxLen)
		}
		// fallback
		return "VARCHAR(1024)"

	// text family: paling tepat STRING
	case "text", "tinytext", "mediumtext", "longtext":
		if keyColumn {
			return keyCompatibleStringType(c)
		}
		return "STRING"

	default:
		// Doris key column tidak boleh STRING, jadi fallback key dipaksa ke VARCHAR.
		if keyColumn {
			return keyCompatibleStringType(c)
		}
		return "STRING"
	}
}

func (s *Sink) EnsureTable(ctx context.Context, targetDB, targetTable string, schema *model.TableSchema) error {
	if len(schema.Columns) == 0 {
		return fmt.Errorf("empty schema for %s.%s", schema.SchemaName, schema.TableName)
	}
	sourceKey := dorisTableKey(schema.SchemaName, schema.TableName)
	targetKey := dorisTableKey(targetDB, targetTable)
	if sourceKey != "" {
		observability.RegisterSourceTable(s.jobID, sourceKey)
		observability.SetSinkType(s.jobID, sourceKey, "doris")
		observability.SetTargetTable(s.jobID, sourceKey, targetKey)
	}

	// pk/non-pk index
	pkIdx := make([]int, 0)
	nonPkIdx := make([]int, 0)
	for i := range schema.Columns {
		if schema.Columns[i].IsPK {
			pkIdx = append(pkIdx, i)
		} else {
			nonPkIdx = append(nonPkIdx, i)
		}
	}
	if len(pkIdx) == 0 {
		pkIdx = []int{0}
		tmp := nonPkIdx[:0]
		for _, i := range nonPkIdx {
			if i != 0 {
				tmp = append(tmp, i)
			}
		}
		nonPkIdx = tmp
	}

	orderedIdx := make([]int, 0, len(schema.Columns))
	orderedIdx = append(orderedIdx, pkIdx...)
	orderedIdx = append(orderedIdx, nonPkIdx...)

	key := strings.ToLower(targetDB + "." + targetTable)

	// init caches ONCE
	s.mu.Lock()
	if s.maxLen[key] == nil {
		s.maxLen[key] = make(map[string]int)
	}
	if s.isString[key] == nil {
		s.isString[key] = make(map[string]bool)
	}
	s.mu.Unlock()

	colsDef := make([]string, 0, len(orderedIdx))
	colsOrder := make([]string, 0, len(orderedIdx))
	bindings := make([]columnBinding, 0, len(orderedIdx))
	keyIndexes := make(map[int]struct{}, len(pkIdx))
	usedTargetNames := make(map[string]int, len(orderedIdx))
	for _, idx := range pkIdx {
		keyIndexes[idx] = struct{}{}
	}
	for _, i := range orderedIdx {
		c := schema.Columns[i]
		targetColName := sanitizeDorisColumnName(c.Name, len(bindings), usedTargetNames)
		_, keyColumn := keyIndexes[i]
		dorisType := mapMySQLColumnToDoris(c, keyColumn)

		// string-ish + maxLen from dorisType
		dtU := strings.ToUpper(strings.TrimSpace(dorisType))
		isStr := dtU == "STRING" || strings.HasPrefix(dtU, "VARCHAR(") || strings.HasPrefix(dtU, "CHAR(")

		ml := 0
		if strings.HasPrefix(dtU, "VARCHAR(") || strings.HasPrefix(dtU, "CHAR(") {
			l := strings.IndexByte(dtU, '(')
			r := strings.IndexByte(dtU, ')')
			if l > 0 && r > l+1 {
				if n, err := strconv.Atoi(strings.TrimSpace(dtU[l+1 : r])); err == nil && n > 0 {
					ml = n
				}
			}
		}

		s.mu.Lock()
		s.isString[key][targetColName] = isStr
		if ml > 0 {
			s.maxLen[key][targetColName] = ml
		}
		s.mu.Unlock()

		if targetColName != c.Name {
			log.Printf("[doris] sanitize source column %q -> target column %q for %s.%s", c.Name, targetColName, targetDB, targetTable)
		}

		col := fmt.Sprintf("`%s` %s", targetColName, dorisType)
		if !c.IsNullable {
			col += " NOT NULL"
		}
		colsDef = append(colsDef, col)
		colsOrder = append(colsOrder, targetColName)
		bindings = append(bindings, columnBinding{Source: c.Name, Target: targetColName})
	}

	// UNIQUE KEY prefix
	pks := make([]string, 0, len(pkIdx))
	for i := 0; i < len(pkIdx); i++ {
		pks = append(pks, fmt.Sprintf("`%s`", bindings[i].Target))
	}

	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
		%s
		)
		UNIQUE KEY(%s)
		DISTRIBUTED BY HASH(%s) BUCKETS 10
		PROPERTIES (
		"replication_num" = "1",
		"enable_unique_key_merge_on_write" = "true"
		);`,
		targetDB,
		targetTable,
		strings.Join(colsDef, ",\n  "),
		strings.Join(pks, ","),
		pks[0],
	)

	log.Printf("[doris] ensure table: %s", ddl)

	s.mu.Lock()
	s.columns[key] = colsOrder
	s.columnBindings[key] = bindings
	s.mu.Unlock()

	return util.RetryWithBackoff(ctx, s.retry, func() error {
		_, err := s.sqlDB.ExecContext(ctx, ddl)
		return err
	})
}

func (s *Sink) CountTargetRows(ctx context.Context, targetDB, targetTable string) (int64, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s",
		quoteDorisIdentifier(targetDB),
		quoteDorisIdentifier(targetTable),
	)

	var count int64
	if err := s.sqlDB.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Sink) ResetTargetTable(ctx context.Context, targetDB, targetTable string) error {
	stmt := fmt.Sprintf(
		"TRUNCATE TABLE %s.%s",
		quoteDorisIdentifier(targetDB),
		quoteDorisIdentifier(targetTable),
	)
	return util.RetryWithBackoff(ctx, s.retry, func() error {
		_, err := s.sqlDB.ExecContext(ctx, stmt)
		return err
	})
}

func (s *Sink) applyDDL(ctx context.Context, targetDB, targetTable, mysqlDDL string) error {
	stmts, ok, reason := s.translateMySQLDDLToDoris(mysqlDDL, targetDB, targetTable)
	if !ok {
		log.Printf("[doris] skip DDL: %s | reason=%s", mysqlDDL, reason)
		return nil
	}

	log.Printf("[doris] apply translated DDL (%d stmt): %v", len(stmts), stmts)

	return util.RetryWithBackoff(ctx, s.retry, func() error {
		for _, st := range stmts {
			st = strings.TrimSpace(st)
			if st == "" {
				continue
			}
			if _, err := s.sqlDB.ExecContext(ctx, st); err != nil {
				return fmt.Errorf("exec ddl failed: %w | stmt=%s", err, st)
			}
		}
		return nil
	})
}

type streamLoadResp struct {
	Status             string `json:"Status"`
	Message            string `json:"Message"`
	Label              string `json:"Label"`
	NumberTotalRows    int64  `json:"NumberTotalRows"`
	NumberLoadedRows   int64  `json:"NumberLoadedRows"`
	NumberFilteredRows int64  `json:"NumberFilteredRows"`
	ErrorURL           string `json:"ErrorURL,omitempty"`
}

func sanitizeCell(v string) string {
	if v == "" {
		return v
	}

	v = strings.ReplaceAll(v, "\r\n", " ")
	v = strings.ReplaceAll(v, "\n", " ")
	v = strings.ReplaceAll(v, "\r", " ")

	v = strings.ReplaceAll(v, fieldSep, " ")
	v = strings.ReplaceAll(v, `"`, `'`)
	v = strings.ReplaceAll(v, escapeChar, " ")

	return v
}

func quoteDorisIdentifier(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "`", "``")
	return "`" + name + "`"
}

func sanitizeDorisColumnName(name string, ordinal int, used map[string]int) string {
	name = strings.TrimSpace(name)

	base := name
	if name == "" || !dorisColumnNamePattern.MatchString(name) {
		var b strings.Builder
		b.Grow(len(name))
		lastUnderscore := false
		for _, r := range name {
			switch {
			case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_':
				b.WriteRune(r)
				lastUnderscore = false
			default:
				if !lastUnderscore {
					b.WriteByte('_')
					lastUnderscore = true
				}
			}
		}
		base = strings.Trim(b.String(), "_")
	}
	if base == "" {
		base = fmt.Sprintf("col_%d", ordinal+1)
	}
	if len(base) > 256 {
		base = strings.TrimRight(base[:256], "_")
		if base == "" {
			base = fmt.Sprintf("col_%d", ordinal+1)
		}
	}
	if used == nil {
		return base
	}

	candidate := base
	for suffix := 2; ; suffix++ {
		key := strings.ToLower(candidate)
		if _, exists := used[key]; !exists {
			used[key] = 1
			return candidate
		}

		next := fmt.Sprintf("%s_%d", base, suffix)
		if len(next) > 256 {
			maxBaseLen := 256 - len(fmt.Sprintf("_%d", suffix))
			if maxBaseLen < 1 {
				maxBaseLen = 1
			}
			trimmed := base
			if len(trimmed) > maxBaseLen {
				trimmed = strings.TrimRight(trimmed[:maxBaseLen], "_")
				if trimmed == "" {
					trimmed = "col"
				}
			}
			next = fmt.Sprintf("%s_%d", trimmed, suffix)
		}
		candidate = next
	}
}

func buildColumnsHeader(cols []string) string {
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		quoted = append(quoted, quoteDorisIdentifier(c))
	}
	return strings.Join(quoted, ",")
}

func parseRedirectHostOverride(raw string) (string, int) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0
	}

	candidate := raw
	if !strings.Contains(candidate, "://") {
		candidate = "//" + candidate
	}

	u, err := url.Parse(candidate)
	if err == nil && u.Host != "" {
		host := strings.TrimSpace(u.Hostname())
		if host == "" {
			return "", 0
		}
		if port := strings.TrimSpace(u.Port()); port != "" {
			if n, convErr := strconv.Atoi(port); convErr == nil && n > 0 {
				return host, n
			}
		}
		return host, 0
	}

	return raw, 0
}

func joinHostPort(host string, port int) string {
	if port > 0 {
		return net.JoinHostPort(host, strconv.Itoa(port))
	}
	return host
}

func rewriteStreamLoadRedirectURL(rawURL *url.URL, cfg config.DorisConfig) string {
	if rawURL == nil {
		return ""
	}

	overrideHost, overrideHostPort := parseRedirectHostOverride(cfg.BEHTTPHost)
	if overrideHost == "" && cfg.BEHTTPPort <= 0 {
		return rawURL.String()
	}

	host := rawURL.Hostname()
	if overrideHost != "" {
		host = overrideHost
	} else if feHost, _ := parseRedirectHostOverride(cfg.HTTPHost); feHost != "" {
		host = feHost
	}

	port := 0
	if rawPort := strings.TrimSpace(rawURL.Port()); rawPort != "" {
		if n, err := strconv.Atoi(rawPort); err == nil && n > 0 {
			port = n
		}
	}
	if overrideHostPort > 0 {
		port = overrideHostPort
	}
	if cfg.BEHTTPPort > 0 {
		port = cfg.BEHTTPPort
	}

	cloned := *rawURL
	cloned.Host = joinHostPort(host, port)
	return cloned.String()
}

func (s *Sink) getColumnRuntimeMeta(targetKey string, cols []string) ([]bool, []int) {
	s.mu.RLock()
	stringMap := s.isString[targetKey]
	maxLenMap := s.maxLen[targetKey]
	s.mu.RUnlock()

	isString := make([]bool, len(cols))
	maxLen := make([]int, len(cols))
	for i, c := range cols {
		if stringMap != nil {
			isString[i] = stringMap[c]
		}
		if maxLenMap != nil {
			maxLen[i] = maxLenMap[c]
		}
	}
	return isString, maxLen
}

func (s *Sink) writeBatchPayload(
	w io.Writer,
	bindings []columnBinding,
	isString []bool,
	maxLen []int,
	batch []model.Event,
) error {
	bw := bufio.NewWriterSize(w, streamLoadBufferSize)

	for _, ev := range batch {
		for i, binding := range bindings {
			if i > 0 {
				if err := bw.WriteByte(fieldSep[0]); err != nil {
					return err
				}
			}

			val, ok := normalizeDorisValue(ev.Data[binding.Source])
			if !ok {
				if _, err := bw.WriteString("\\N"); err != nil {
					return err
				}
				continue
			}

			if val != "" {
				if isString[i] {
					val = sanitizeCell(val)
					val = normalizeToASCII(val)
					if ml := maxLen[i]; ml > 0 {
						val = truncateChars(val, ml) // ASCII-only => aman byte==char
					}
				} else {
					val = sanitizeCell(val)
				}

				if _, err := bw.WriteString(val); err != nil {
					return err
				}
			}
		}

		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}

	return bw.Flush()
}

func resetBatch(batch []model.Event) []model.Event {
	if len(batch) == 0 {
		if cap(batch) > retainedBatchCapLimit {
			return nil
		}
		return batch[:0]
	}

	if cap(batch) > retainedBatchCapLimit {
		return nil
	}

	clear(batch)
	return batch[:0]
}

func eventTraceID(ev model.Event) string {
	if strings.TrimSpace(ev.TraceID) == "" {
		return "-"
	}
	return ev.TraceID
}

func eventSourceOffsetText(ev model.Event) string {
	if ev.SourceOffset == nil || ev.SourceOffset.BinlogPos == 0 {
		return "-"
	}
	file := strings.TrimSpace(ev.SourceOffset.BinlogFile)
	if file == "" {
		file = "unknown-binlog"
	}
	return fmt.Sprintf("%s:%d", file, ev.SourceOffset.BinlogPos)
}

func batchTraceSummary(batch []model.Event) string {
	if len(batch) == 0 {
		return "-"
	}

	traces := make([]string, 0, 5)
	for _, ev := range batch {
		if strings.TrimSpace(ev.TraceID) == "" {
			continue
		}
		traces = append(traces, ev.TraceID)
		if len(traces) >= 5 {
			break
		}
	}
	if len(traces) == 0 {
		return "-"
	}
	out := strings.Join(traces, ",")
	if len(batch) > len(traces) {
		out += ",..."
	}
	return out
}

func (s *Sink) sendBatch(ctx context.Context, targetDB, targetTable string, batch []model.Event) error {
	if len(batch) == 0 {
		return nil
	}

	bindings := s.getColumnBindingsForTarget(ctx, targetDB, targetTable, batch[0])
	cols := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		cols = append(cols, binding.Target)
	}
	targetKey := strings.ToLower(targetDB + "." + targetTable)
	isString, maxLen := s.getColumnRuntimeMeta(targetKey, cols)
	url := fmt.Sprintf("%s/api/%s/%s/_stream_load", s.cfg.HTTPHost, targetDB, targetTable)
	label := fmt.Sprintf("gosync_%d", time.Now().UnixNano())
	columnsHeader := buildColumnsHeader(cols)
	var payload bytes.Buffer
	if err := s.writeBatchPayload(&payload, bindings, isString, maxLen, batch); err != nil {
		return err
	}
	bodyBytes := payload.Bytes()
	traces := batchTraceSummary(batch)

	log.Printf("[doris][job %s] stream_load start label=%s target=%s.%s rows=%d traces=%s",
		s.jobID, label, targetDB, targetTable, len(batch), traces)

	start := time.Now()
	err := util.RetryWithBackoff(ctx, s.retry, func() error {
		targetURL := url
		var resp *http.Response
		var body []byte
		var err error

		for redirects := 0; redirects < 3; redirects++ {
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(bodyBytes))
			if reqErr != nil {
				return reqErr
			}

			// Handle Doris FE -> BE redirects ourselves so auth/header/body stay intact.
			req.GetBody = nil
			req.SetBasicAuth(s.cfg.User, s.cfg.Password)
			req.Header.Set("label", label)
			req.Header.Set("format", "csv")
			req.Header.Set("columns", columnsHeader)
			req.Header.Set("strict_mode", "true")
			req.Header.Set("column_separator", headerFieldSep)
			req.Header.Set("null_value", "\\N")
			req.Header.Set("Expect", "100-continue")
			req.Header.Set("Content-Type", "text/csv")

			resp, err = s.httpClient.Do(req)
			if err != nil {
				return err
			}

			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode != http.StatusTemporaryRedirect && resp.StatusCode != http.StatusPermanentRedirect {
				break
			}

			nextURL, locErr := resp.Location()
			if locErr != nil {
				return fmt.Errorf("doris stream load redirect without valid Location header: %w", locErr)
			}
			rewrittenURL := rewriteStreamLoadRedirectURL(nextURL, s.cfg)
			if rewrittenURL != nextURL.String() {
				log.Printf("[doris] rewrite stream load redirect %s -> %s", nextURL.String(), rewrittenURL)
			}
			targetURL = rewrittenURL
		}

		if resp == nil {
			return fmt.Errorf("doris stream load did not produce a response")
		}
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
			return fmt.Errorf("doris stream load too many redirects for %s", targetTable)
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err := fmt.Errorf("doris stream load http=%s body=%s", resp.Status, string(body))
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return util.Permanent(err)
			}
			return err
		}

		var r streamLoadResp
		if err := json.Unmarshal(body, &r); err != nil {
			return fmt.Errorf("doris stream load bad json body=%s", string(body))
		}

		if strings.ToLower(strings.TrimSpace(r.Status)) != "success" {
			// Option B: kalau label sudah ada, jangan fail.
			// Poll status label tersebut sampai FINISHED/CANCELLED.
			if isLabelAlreadyExists(r.Status, r.Message) {
				log.Printf("[doris] label already exists, polling label=%s (db=%s)", label, targetDB)
				return s.waitLoadLabelDone(ctx, targetDB, label)
			}

			return util.Permanent(fmt.Errorf(
				"doris stream load not success: status=%s msg=%s total=%d loaded=%d filtered=%d label=%s error_url=%s body=%s",
				r.Status, r.Message, r.NumberTotalRows, r.NumberLoadedRows, r.NumberFilteredRows, r.Label, r.ErrorURL, string(body),
			))
		}

		if r.NumberFilteredRows > 0 {
			log.Printf("[doris][job %s] stream_load success filtered=%d total=%d label=%s target=%s.%s traces=%s msg=%s",
				s.jobID, r.NumberFilteredRows, r.NumberTotalRows, r.Label, targetDB, targetTable, traces, r.Message)
		} else {
			log.Printf("[doris][job %s] stream_load success label=%s target=%s.%s total=%d loaded=%d traces=%s",
				s.jobID, r.Label, targetDB, targetTable, r.NumberTotalRows, r.NumberLoadedRows, traces)
		}

		return nil
	})
	if err != nil {
		return err
	}
	s.recordDorisSinkFlush(batch, dorisTableKey(targetDB, targetTable), time.Since(start))
	return nil
}

func (s *Sink) recordDorisSinkFlush(batch []model.Event, targetTable string, duration time.Duration) {
	bySource := make(map[string]dorisFlushCounter)
	for _, ev := range batch {
		sourceTable := dorisTableKey(ev.Schema, ev.Table)
		if sourceTable == "" {
			continue
		}
		counter := bySource[sourceTable]
		counter.events++
		counter.rows++
		if ev.Type == model.EventTypeDelete {
			counter.deletes++
		}
		bySource[sourceTable] = counter
	}

	for sourceTable, counter := range bySource {
		observability.RegisterSourceTable(s.jobID, sourceTable)
		observability.SetSinkType(s.jobID, sourceTable, "doris")
		observability.SetTargetTable(s.jobID, sourceTable, targetTable)
		observability.RecordSinkFlush(s.jobID, sourceTable, targetTable, "stream", "stream_load", counter.events, counter.rows, counter.deletes, duration)
	}
}

func (s *Sink) Run(ctx context.Context, in <-chan model.Event) error {
	batches := make(map[string][]model.Event) // key "db.table"
	var pendingOffset *model.SourceOffset
	pendingOffsetTraceID := ""
	flushTicker := time.NewTicker(time.Duration(s.cfg.FlushSeconds) * time.Second)
	defer flushTicker.Stop()

	rememberOffset := func(off *model.SourceOffset, traceID string) {
		if !off.Valid() {
			return
		}
		cp := *off
		pendingOffset = &cp
		pendingOffsetTraceID = traceID
	}

	allBatchesEmpty := func() bool {
		for _, b := range batches {
			if len(b) > 0 {
				return false
			}
		}
		return true
	}

	commitPendingOffset := func(commitCtx context.Context) error {
		if pendingOffset == nil || s.offsetSto == nil || !allBatchesEmpty() {
			return nil
		}

		saveCtx, cancel := context.WithTimeout(commitCtx, 2*time.Second)
		defer cancel()

		if err := connector.SaveSourceOffset(saveCtx, s.offsetSto, s.checkpointKey(), pendingOffset); err != nil {
			log.Printf("[doris][job %s] save offset error pos=%s:%d: %v", s.jobID, pendingOffset.BinlogFile, pendingOffset.BinlogPos, err)
			return nil
		}
		log.Printf("[doris][job %s] committed offset trace_id=%s pos=%s:%d", s.jobID, pendingOffsetTraceID, pendingOffset.BinlogFile, pendingOffset.BinlogPos)
		pendingOffset = nil
		pendingOffsetTraceID = ""
		return nil
	}

	flushAll := func(flushCtx context.Context) error {
		for key, b := range batches {
			if len(b) == 0 {
				continue
			}
			db, tbl, ok := splitDBTable(key)
			if !ok {
				continue
			}
			if err := s.sendBatch(flushCtx, db, tbl, b); err != nil {
				return err
			}
			log.Printf("[doris][job %s] flushed target=%s rows=%d traces=%s", s.jobID, key, len(b), batchTraceSummary(b))
			batches[key] = resetBatch(b)
		}
		return commitPendingOffset(flushCtx)
	}

	handleEvent := func(procCtx context.Context, ev model.Event) error {
		if ev.Type == model.EventTypeCheckpoint {
			log.Printf("[doris][job %s] checkpoint received trace_id=%s pos=%s batches_empty=%t",
				s.jobID, eventTraceID(ev), eventSourceOffsetText(ev), allBatchesEmpty())
			rememberOffset(ev.SourceOffset, eventTraceID(ev))
			return flushAll(procCtx)
		}

		targetDB, targetTable := s.resolveTarget(ev.Schema, ev.Table)
		targetKey := strings.ToLower(targetDB + "." + targetTable)

		if ev.Type == model.EventTypeDDL {
			if err := flushAll(procCtx); err != nil {
				return err
			}
			if err := s.applyDDL(procCtx, targetDB, targetTable, ev.DDL); err != nil {
				log.Printf("[doris] DDL error: %v", err)
				return err
			}
			return nil
		}

		b := batches[targetKey]
		b = append(b, ev)
		batches[targetKey] = b
		log.Printf("[doris][job %s] buffered trace_id=%s action=%s source=%s.%s target=%s batch_rows=%d source_pos=%s",
			s.jobID, eventTraceID(ev), ev.Type, ev.Schema, ev.Table, targetKey, len(b), eventSourceOffsetText(ev))

		if len(b) >= s.cfg.BatchSize {
			db, tbl, _ := splitDBTable(targetKey)
			if err := s.sendBatch(procCtx, db, tbl, b); err != nil {
				log.Printf("[doris] flush error: %v", err)
				return err
			}
			log.Printf("[doris][job %s] flushed target=%s rows=%d traces=%s", s.jobID, targetKey, len(b), batchTraceSummary(b))
			batches[targetKey] = resetBatch(b)
			if err := commitPendingOffset(procCtx); err != nil {
				return err
			}
		}
		return nil
	}

	drainAfterCancel := func() error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		log.Printf("[doris] context cancelled, draining buffered events before stop")
		for {
			select {
			case <-shutdownCtx.Done():
				return shutdownCtx.Err()
			case ev, ok := <-in:
				if !ok {
					return flushAll(shutdownCtx)
				}
				if err := handleEvent(shutdownCtx, ev); err != nil {
					return err
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			if err := drainAfterCancel(); err != nil {
				log.Printf("[doris] drain on cancel error: %v", err)
			}
			return ctx.Err()

		case <-flushTicker.C:
			if err := flushAll(ctx); err != nil {
				log.Printf("[doris] flush error: %v", err)
				return err
			}

		case ev, ok := <-in:
			if !ok {
				if err := flushAll(ctx); err != nil {
					return err
				}
				return nil
			}
			if err := handleEvent(ctx, ev); err != nil {
				return err
			}
		}
	}
}

func (s *Sink) resolveTarget(srcDB, srcTable string) (string, string) {
	srcDB = strings.TrimSpace(srcDB)
	srcTable = strings.TrimSpace(srcTable)
	key := strings.ToLower(srcDB + "." + srcTable)

	if s.cfg.Overrides != nil {
		if t, ok := s.cfg.Overrides[key]; ok {
			db := t.Database
			if db == "" {
				db = srcDB
			}
			tbl := t.Table
			if tbl == "" {
				tbl = srcTable
			}
			return db, tbl
		}

		if t, ok := s.cfg.Overrides[strings.ToLower(srcDB+".*")]; ok {
			db := t.Database
			if db == "" {
				db = srcDB
			}
			tbl := t.Table
			if tbl == "" || tbl == "*" {
				tbl = srcTable
			}
			return db, tbl
		}
	}

	if s.cfg.DefaultDatabase != "" {
		return s.cfg.DefaultDatabase, srcTable
	}

	return srcDB, srcTable
}

func splitDBTable(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", false
	}
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	db := strings.TrimSpace(parts[0])
	tbl := strings.TrimSpace(parts[1])
	if db == "" || tbl == "" {
		return "", "", false
	}
	return db, tbl, true
}

func dorisTableKey(schema, table string) string {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if schema == "" {
		return strings.ToLower(table)
	}
	if table == "" {
		return strings.ToLower(schema)
	}
	return strings.ToLower(schema + "." + table)
}

func (s *Sink) getColumnBindingsForTarget(ctx context.Context, targetDB, targetTable string, ev model.Event) []columnBinding {
	key := strings.ToLower(targetDB + "." + targetTable)

	// 1) cache hit (paling ideal: dari EnsureTable / schema)
	s.mu.RLock()
	if bindings, ok := s.columnBindings[key]; ok && len(bindings) > 0 {
		out := make([]columnBinding, len(bindings))
		copy(out, bindings)
		s.mu.RUnlock()
		return out
	}
	s.mu.RUnlock()

	// 2) coba ambil dari Doris (DESC) lalu cache
	if cols, err := s.fetchColumnsFromDoris(ctx, targetDB, targetTable); err == nil && len(cols) > 0 {
		bindings := make([]columnBinding, 0, len(cols))
		for _, col := range cols {
			bindings = append(bindings, columnBinding{Source: col, Target: col})
		}
		s.mu.Lock()
		s.columns[key] = cols
		s.columnBindings[key] = bindings
		s.mu.Unlock()

		out := make([]columnBinding, len(bindings))
		copy(out, bindings)
		return out
	}

	// 3) last resort: deterministik dari event (stabil utk event yang sama)
	// NOTE: ini hanya supaya jalan, tapi tidak seaman "DESC table".
	cols := make([]string, 0, len(ev.Data))
	for k := range ev.Data {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	usedTargetNames := make(map[string]int, len(cols))
	bindings := make([]columnBinding, 0, len(cols))
	for i, col := range cols {
		targetColName := sanitizeDorisColumnName(col, i, usedTargetNames)
		bindings = append(bindings, columnBinding{Source: col, Target: targetColName})
	}
	return bindings
}

func (s *Sink) fetchColumnsFromDoris(ctx context.Context, db, table string) ([]string, error) {
	// Doris MySQL protocol: "DESC db.table" atau "SHOW COLUMNS FROM db.table"
	q := fmt.Sprintf("DESC `%s`.`%s`", db, table)

	rows, err := s.sqlDB.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make([]string, 0, 64)

	// DESC di MySQL biasanya: Field, Type, Null, Key, Default, Extra
	// Doris mirip, jadi kita scan minimal Field saja, sisanya buang.
	for rows.Next() {
		var field string
		var t, nullStr, key, def, extra sql.NullString
		if err := rows.Scan(&field, &t, &nullStr, &key, &def, &extra); err != nil {
			// beberapa versi bisa beda jumlah kolom -> fallback scan fleksibel
			// coba scan cuma 1 kolom (field) kalau driver mengizinkan? biasanya tidak.
			return nil, err
		}
		if field != "" {
			cols = append(cols, field)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func normalizeDorisValue(v any) (string, bool) {
	if v == nil {
		return "", false
	}

	var s string
	switch x := v.(type) {
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprintf("%v", x)
	}
	s = strings.TrimSpace(s)

	// zero DATETIME / DATE dari MySQL.
	// MySQL bisa menyimpan "0000-00-00" / "0000-00-00 00:00:00" sebagai "zero date".
	// Doris tidak menerima nilai ini untuk kolom NOT NULL, jadi kita map ke tanggal default yang valid.
	if s == "0000-00-00" {
		return "1970-01-01", true
	}
	if s == "0000-00-00 00:00:00" {
		return "1970-01-01 00:00:00", true
	}

	return s, true
}

// ---- Stream Load label status polling (Option B) ----

func isLabelAlreadyExists(status, msg string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	m := strings.ToLower(msg)

	// Doris biasanya: Status="Label Already Exists"
	// Message mengandung: "[LABEL_ALREADY_EXISTS]"
	if strings.Contains(s, "label already exists") {
		return true
	}
	if strings.Contains(m, "label_already_exists") || strings.Contains(m, "label already exists") {
		return true
	}
	return false
}

// Doris SHOW LOAD output bisa beda versi.
// Kita scan pakai columns dan ambil kolom "State" (atau "state") secara dinamis.
func (s *Sink) getLoadStateByLabel(ctx context.Context, db, label string) (state string, found bool, err error) {
	// Aman pakai backtick untuk db
	q := fmt.Sprintf("SHOW LOAD FROM `%s` WHERE Label = '%s'", db, strings.ReplaceAll(label, "'", "''"))

	rows, err := s.sqlDB.QueryContext(ctx, q)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", false, err
	}

	stateIdx := -1
	for i, c := range cols {
		if strings.EqualFold(c, "State") {
			stateIdx = i
			break
		}
	}
	if stateIdx < 0 {
		// fallback: beberapa versi pakai "state"
		for i, c := range cols {
			if strings.EqualFold(c, "state") {
				stateIdx = i
				break
			}
		}
	}
	if stateIdx < 0 {
		return "", false, fmt.Errorf("SHOW LOAD missing State column; cols=%v", cols)
	}

	// Scan 1 row pertama saja (label biasanya unik)
	for rows.Next() {
		vals := make([]any, len(cols))
		bufs := make([]sql.RawBytes, len(cols))
		for i := range vals {
			vals[i] = &bufs[i]
		}

		if err := rows.Scan(vals...); err != nil {
			return "", false, err
		}

		st := strings.TrimSpace(string(bufs[stateIdx]))
		if st == "" {
			return "", true, nil
		}
		return st, true, nil
	}

	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return "", false, nil
}

// Tunggu sampai label selesai.
// - FINISHED => sukses
// - CANCELLED => error
// - lainnya => polling sampai timeout ctx atau maxWait
func (s *Sink) waitLoadLabelDone(ctx context.Context, db, label string) error {
	const maxWait = 5 * time.Minute

	deadline := time.Now().Add(maxWait)
	sleep := 1 * time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting doris load label=%s", label)
		}

		ls, found, err := s.getLoadStatusByLabel(ctx, db, label)
		if err != nil {
			log.Printf("[doris] wait label=%s: show load error: %v", label, err)
			time.Sleep(sleep)
			if sleep < 5*time.Second {
				sleep += 500 * time.Millisecond
			}
			continue
		}
		if !found {
			time.Sleep(sleep)
			continue
		}

		up := strings.ToUpper(strings.TrimSpace(ls.State))
		switch up {
		case "FINISHED":
			log.Printf("[doris] label=%s finished (treat as success)", label)
			return nil
		case "CANCELLED":
			// kalau reason kosong, tetap kasih minimal info raw keys biar gampang debug
			if strings.TrimSpace(ls.Reason) != "" {
				return fmt.Errorf("doris load cancelled label=%s reason=%s", label, ls.Reason)
			}
			return fmt.Errorf("doris load cancelled label=%s (no reason column found)", label)
		default:
			time.Sleep(sleep)
			if sleep < 5*time.Second {
				sleep += 500 * time.Millisecond
			}
		}
	}
}

func (s *Sink) getLoadStatusByLabel(ctx context.Context, db, label string) (st loadStatus, found bool, err error) {
	q := fmt.Sprintf("SHOW LOAD FROM `%s` WHERE Label = '%s'", db, strings.ReplaceAll(label, "'", "''"))

	rows, err := s.sqlDB.QueryContext(ctx, q)
	if err != nil {
		return loadStatus{}, false, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return loadStatus{}, false, err
	}

	// helper cari index kolom by possible names
	findIdx := func(names ...string) int {
		for _, want := range names {
			for i, c := range cols {
				if strings.EqualFold(c, want) {
					return i
				}
			}
		}
		return -1
	}

	stateIdx := findIdx("State", "state")
	// Doris berbeda versi: kadang Reason / ErrorMsg / Message / Msg / FailMsg / ErrorMessage
	reasonIdx := findIdx("Reason", "reason", "ErrorMsg", "errormsg", "Message", "message", "Msg", "msg", "FailMsg", "failmsg", "ErrorMessage", "errormessage")

	if stateIdx < 0 {
		return loadStatus{}, false, fmt.Errorf("SHOW LOAD missing State column; cols=%v", cols)
	}

	for rows.Next() {
		vals := make([]any, len(cols))
		bufs := make([]sql.RawBytes, len(cols))
		for i := range vals {
			vals[i] = &bufs[i]
		}

		if err := rows.Scan(vals...); err != nil {
			return loadStatus{}, false, err
		}

		raw := make(map[string]string, len(cols))
		for i, c := range cols {
			raw[c] = strings.TrimSpace(string(bufs[i]))
		}

		state := strings.TrimSpace(string(bufs[stateIdx]))
		reason := ""
		if reasonIdx >= 0 {
			reason = strings.TrimSpace(string(bufs[reasonIdx]))
		}

		return loadStatus{
			State:  state,
			Reason: reason,
			Raw:    raw,
		}, true, nil
	}

	if err := rows.Err(); err != nil {
		return loadStatus{}, false, err
	}
	return loadStatus{}, false, nil
}
