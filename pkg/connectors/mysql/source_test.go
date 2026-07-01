package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/util"
)

func TestParseConfiguredTableEntrySupportsWildcardDatabasePatterns(t *testing.T) {
	dbName, tableName, wildcard, ok := parseConfiguredTableEntry("App.*")
	if !ok {
		t.Fatal("expected wildcard entry to parse")
	}
	if dbName != "app" {
		t.Fatalf("dbName = %q, want %q", dbName, "app")
	}
	if tableName != "" {
		t.Fatalf("tableName = %q, want empty", tableName)
	}
	if !wildcard {
		t.Fatal("expected wildcard flag to be true")
	}
}

func TestParseConfiguredTableEntrySupportsTableGlobPatterns(t *testing.T) {
	dbName, tablePattern, wildcard, ok := parseConfiguredTableEntry("App.tbl_reservasi_backup_*")
	if !ok {
		t.Fatal("expected table glob entry to parse")
	}
	if dbName != "app" {
		t.Fatalf("dbName = %q, want %q", dbName, "app")
	}
	if tablePattern != "tbl_reservasi_backup_*" {
		t.Fatalf("tablePattern = %q, want %q", tablePattern, "tbl_reservasi_backup_*")
	}
	if !wildcard {
		t.Fatal("expected wildcard flag to be true")
	}
}

func TestExpandConfiguredTablesExpandsWildcardAndDeduplicates(t *testing.T) {
	got, err := expandConfiguredTables(context.Background(), []string{
		"app.*",
		"app.orders",
		"logs.audit",
	}, func(ctx context.Context, dbName string) ([]string, error) {
		if dbName != "app" {
			t.Fatalf("listTables called with dbName=%q, want %q", dbName, "app")
		}
		return []string{"customers", "orders"}, nil
	})
	if err != nil {
		t.Fatalf("expandConfiguredTables returned error: %v", err)
	}

	want := []string{"app.customers", "app.orders", "logs.audit"}
	if got, wantLen := len(got), len(want); got != wantLen {
		t.Fatalf("length = %d, want %d (%#v)", got, wantLen, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tables[%d] = %q, want %q (all=%#v)", i, got[i], want[i], got)
		}
	}
}

func TestExpandConfiguredTablesPreservesDiscoveredTableCase(t *testing.T) {
	got, err := expandConfiguredTables(context.Background(), []string{
		"scraping_operator.*",
	}, func(ctx context.Context, dbName string) ([]string, error) {
		if dbName != "scraping_operator" {
			t.Fatalf("listTables called with dbName=%q, want %q", dbName, "scraping_operator")
		}
		return []string{"pageData", "orders"}, nil
	})
	if err != nil {
		t.Fatalf("expandConfiguredTables returned error: %v", err)
	}

	want := []string{"scraping_operator.pageData", "scraping_operator.orders"}
	if got, wantLen := len(got), len(want); got != wantLen {
		t.Fatalf("length = %d, want %d (%#v)", got, wantLen, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tables[%d] = %q, want %q (all=%#v)", i, got[i], want[i], got)
		}
	}
}

func TestExpandConfiguredTablesExpandsTableGlobInOrder(t *testing.T) {
	got, err := expandConfiguredTables(context.Background(), []string{
		"app.tbl_reservasi_backup_*",
	}, func(ctx context.Context, dbName string) ([]string, error) {
		if dbName != "app" {
			t.Fatalf("listTables called with dbName=%q, want %q", dbName, "app")
		}
		return []string{
			"tbl_reservasi",
			"tbl_reservasi_backup_202501",
			"tbl_reservasi_backup_202502",
		}, nil
	})
	if err != nil {
		t.Fatalf("expandConfiguredTables returned error: %v", err)
	}

	want := []string{
		"app.tbl_reservasi_backup_202501",
		"app.tbl_reservasi_backup_202502",
	}
	if got, wantLen := len(got), len(want); got != wantLen {
		t.Fatalf("length = %d, want %d (%#v)", got, wantLen, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tables[%d] = %q, want %q (all=%#v)", i, got[i], want[i], got)
		}
	}
}

func TestExpandConfiguredTablesErrorsWhenWildcardMatchesNothing(t *testing.T) {
	_, err := expandConfiguredTables(context.Background(), []string{"app.*"}, func(ctx context.Context, dbName string) ([]string, error) {
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error when wildcard matches nothing")
	}
	if !strings.Contains(err.Error(), "matched no tables") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFilterConfiguredTablesToBaseTablesSkipsViews(t *testing.T) {
	got, skipped, err := filterConfiguredTablesToBaseTables(context.Background(), []string{
		"app.orders",
		"app.orders_view",
	}, func(ctx context.Context, dbName, tableName string) (string, error) {
		switch tableName {
		case "orders":
			return "BASE TABLE", nil
		case "orders_view":
			return "VIEW", nil
		default:
			return "", fmt.Errorf("unexpected table %s.%s", dbName, tableName)
		}
	})
	if err != nil {
		t.Fatalf("filterConfiguredTablesToBaseTables returned error: %v", err)
	}
	if len(got) != 1 || got[0] != "app.orders" {
		t.Fatalf("got tables = %#v, want [app.orders]", got)
	}
	if len(skipped) != 1 || skipped[0] != "app.orders_view" {
		t.Fatalf("skipped = %#v, want [app.orders_view]", skipped)
	}
}

func TestFilterConfiguredTablesToBaseTablesErrorsWhenNothingLeft(t *testing.T) {
	_, skipped, err := filterConfiguredTablesToBaseTables(context.Background(), []string{
		"app.orders_view",
	}, func(ctx context.Context, dbName, tableName string) (string, error) {
		return "VIEW", nil
	})
	if err == nil {
		t.Fatal("expected error when no base table remains")
	}
	if len(skipped) != 1 || skipped[0] != "app.orders_view" {
		t.Fatalf("skipped = %#v, want [app.orders_view]", skipped)
	}
	if !strings.Contains(err.Error(), "no base tables") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClassifyBinlogStartErrorMarksError1236Permanent(t *testing.T) {
	start := &gomysql.Position{Name: "mysql-bin.000123", Pos: 456}
	inner := &gomysql.MyError{
		Code:    1236,
		State:   "HY000",
		Message: "Could not find first log file name in binary log index file",
	}

	got := classifyBinlogStartError(fmt.Errorf("start sync replication failed: %w", inner), start)

	if !util.IsPermanent(got) {
		t.Fatalf("expected permanent error, got %T %v", got, got)
	}
	if !strings.Contains(got.Error(), "mysql-bin.000123:456") {
		t.Fatalf("expected checkpoint in message, got %q", got.Error())
	}
	if !strings.Contains(got.Error(), "mode=initial") {
		t.Fatalf("expected remediation in message, got %q", got.Error())
	}
}

func TestClassifyBinlogStartErrorIncludesDiagnostics(t *testing.T) {
	start := &gomysql.Position{Name: "mysql-bin.000123", Pos: 456}
	inner := &gomysql.MyError{
		Code:    1236,
		State:   "HY000",
		Message: "Could not find first log file name in binary log index file",
	}

	got := classifyBinlogStartErrorWithDiagnostics(
		fmt.Errorf("start sync replication failed: %w", inner),
		start,
		"Saved checkpoint row key=rivus/v1/test pos=mysql-bin.000123:456 updated_at=2026-06-04T15:10:37+07:00. MySQL binary logs currently available earliest=mysql-bin.000130 latest=mysql-bin.000140 count=11.",
	)

	if !util.IsPermanent(got) {
		t.Fatalf("expected permanent error, got %T %v", got, got)
	}
	for _, want := range []string{
		"Saved checkpoint row key=rivus/v1/test",
		"earliest=mysql-bin.000130",
		"latest=mysql-bin.000140",
	} {
		if !strings.Contains(got.Error(), want) {
			t.Fatalf("expected %q in message, got %q", want, got.Error())
		}
	}
}

func TestClassifyBinlogStartErrorLeavesOtherErrorsUntouched(t *testing.T) {
	inner := errors.New("temporary network blip")

	got := classifyBinlogStartError(inner, nil)

	if got != inner {
		t.Fatalf("expected original error, got %T %v", got, got)
	}
}

func TestGetMasterPosUsesBinaryLogStatus(t *testing.T) {
	var queries []string
	responses := []masterPosRow{
		fakeMasterPosRow{file: "binlog.000007", pos: 1307},
	}

	got, err := getMasterPosWithQuery(context.Background(), fakeMasterPosQuery(&queries, &responses))
	if err != nil {
		t.Fatalf("getMasterPosWithQuery returned error: %v", err)
	}

	if got.Name != "binlog.000007" || got.Pos != 1307 {
		t.Fatalf("position = %s:%d, want binlog.000007:1307", got.Name, got.Pos)
	}
	if len(queries) != 1 || queries[0] != showBinaryLogStatusQuery {
		t.Fatalf("queries = %#v, want only %q", queries, showBinaryLogStatusQuery)
	}
}

func TestGetMasterPosFallsBackToMasterStatusWhenBinaryLogStatusUnsupported(t *testing.T) {
	var queries []string
	responses := []masterPosRow{
		fakeMasterPosRow{err: &mysqldriver.MySQLError{
			Number:  1064,
			Message: "You have an error in your SQL syntax near 'BINARY LOG STATUS'",
		}},
		fakeMasterPosRow{file: "mysql-bin.000123", pos: 456},
	}

	got, err := getMasterPosWithQuery(context.Background(), fakeMasterPosQuery(&queries, &responses))
	if err != nil {
		t.Fatalf("getMasterPosWithQuery returned error: %v", err)
	}

	if got.Name != "mysql-bin.000123" || got.Pos != 456 {
		t.Fatalf("position = %s:%d, want mysql-bin.000123:456", got.Name, got.Pos)
	}
	wantQueries := []string{showBinaryLogStatusQuery, showMasterStatusQuery}
	if fmt.Sprint(queries) != fmt.Sprint(wantQueries) {
		t.Fatalf("queries = %#v, want %#v", queries, wantQueries)
	}
}

func TestGetMasterPosDoesNotFallbackOnBinaryLogPrivilegeError(t *testing.T) {
	var queries []string
	responses := []masterPosRow{
		fakeMasterPosRow{err: &mysqldriver.MySQLError{
			Number:  1227,
			Message: "Access denied; you need (at least one of) the REPLICATION CLIENT privilege(s) for this operation",
		}},
		fakeMasterPosRow{file: "mysql-bin.000123", pos: 456},
	}

	_, err := getMasterPosWithQuery(context.Background(), fakeMasterPosQuery(&queries, &responses))
	if err == nil {
		t.Fatal("expected privilege error")
	}
	if len(queries) != 1 || queries[0] != showBinaryLogStatusQuery {
		t.Fatalf("queries = %#v, want only %q", queries, showBinaryLogStatusQuery)
	}
}

func TestGetMasterPosNoRowsExplainsMissingCoordinates(t *testing.T) {
	var queries []string
	responses := []masterPosRow{
		fakeMasterPosRow{err: sql.ErrNoRows},
	}

	_, err := getMasterPosWithQuery(context.Background(), fakeMasterPosQuery(&queries, &responses))
	if err == nil {
		t.Fatal("expected no rows error")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows to be wrapped, got %T %v", err, err)
	}
	if !strings.Contains(err.Error(), "binary log coordinates are unavailable") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestCDCHandlerOnPosSyncedEmitsCheckpoint(t *testing.T) {
	out := make(chan model.Event, 1)
	handler := &cdcHandler{
		jobID: "job-1",
		out:   out,
		ctx:   context.Background(),
	}

	err := handler.OnPosSynced(nil, gomysql.Position{Name: "mysql-bin.000123", Pos: 456}, nil, true)
	if err != nil {
		t.Fatalf("OnPosSynced returned error: %v", err)
	}

	select {
	case ev := <-out:
		if ev.Type != model.EventTypeCheckpoint {
			t.Fatalf("event type = %s, want %s", ev.Type, model.EventTypeCheckpoint)
		}
		if ev.TraceID != "mysql-bin.000123:456:checkpoint" {
			t.Fatalf("trace id = %q, want mysql-bin.000123:456:checkpoint", ev.TraceID)
		}
		if ev.SourceOffset == nil {
			t.Fatal("expected source offset")
		}
		if ev.SourceOffset.BinlogFile != "mysql-bin.000123" || ev.SourceOffset.BinlogPos != 456 {
			t.Fatalf("source offset = %#v, want mysql-bin.000123:456", ev.SourceOffset)
		}
	default:
		t.Fatal("expected checkpoint event")
	}
}

func TestCDCHandlerOnRowEmitsTraceIDAndSourceOffset(t *testing.T) {
	out := make(chan model.Event, 1)
	handler := &cdcHandler{
		jobID:             "job-1",
		allowed:           map[string]bool{"app.orders": true},
		currentBinlogFile: "mysql-bin.000123",
		out:               out,
		ctx:               context.Background(),
	}

	err := handler.OnRow(&canal.RowsEvent{
		Table: &schema.Table{
			Schema: "app",
			Name:   "orders",
			Columns: []schema.TableColumn{
				{Name: "id"},
				{Name: "islunas"},
			},
			PKColumns: []int{0},
		},
		Action: canal.UpdateAction,
		Rows: [][]interface{}{
			{int64(9376214), int64(0)},
			{int64(9376214), int64(1)},
		},
		Header: &replication.EventHeader{Timestamp: 1777977226, LogPos: 226030061},
	})
	if err != nil {
		t.Fatalf("OnRow returned error: %v", err)
	}

	select {
	case ev := <-out:
		if ev.TraceID != "mysql-bin.000123:226030061:update:0" {
			t.Fatalf("trace id = %q, want mysql-bin.000123:226030061:update:0", ev.TraceID)
		}
		if ev.SourceOffset == nil {
			t.Fatal("expected source offset")
		}
		if ev.SourceOffset.BinlogFile != "mysql-bin.000123" || ev.SourceOffset.BinlogPos != 226030061 {
			t.Fatalf("source offset = %#v, want mysql-bin.000123:226030061", ev.SourceOffset)
		}
		if ev.Origin != model.EventOriginCDC {
			t.Fatalf("origin = %s, want %s", ev.Origin, model.EventOriginCDC)
		}
		if got, want := ev.Timestamp, time.Unix(1777977226, 0).UTC(); !got.Equal(want) {
			t.Fatalf("timestamp = %s, want %s", got, want)
		}
	default:
		t.Fatal("expected row event")
	}
}

func TestBuildSnapshotCursorPredicateUsesLexicographicKeyset(t *testing.T) {
	cursor := &snapshotCursor{
		Columns: []string{"tenant_id", "id"},
		Values: []snapshotCursorValue{
			{Kind: "int64", Int64: 7},
			{Kind: "int64", Int64: 99},
		},
	}

	gotSQL, gotArgs, err := buildSnapshotCursorPredicate([]string{"tenant_id", "id"}, cursor)
	if err != nil {
		t.Fatalf("buildSnapshotCursorPredicate returned error: %v", err)
	}

	wantSQL := "(`tenant_id` > ?) OR (`tenant_id` = ? AND `id` > ?)"
	if gotSQL != wantSQL {
		t.Fatalf("predicate = %q, want %q", gotSQL, wantSQL)
	}
	if got, want := len(gotArgs), 3; got != want {
		t.Fatalf("args length = %d, want %d", got, want)
	}
	if gotArgs[0] != int64(7) || gotArgs[1] != int64(7) || gotArgs[2] != int64(99) {
		t.Fatalf("unexpected args: %#v", gotArgs)
	}
}

func TestSnapshotPlanBuildSnapshotQueryUsesKeysetCursor(t *testing.T) {
	cursor := &snapshotCursor{
		Columns: []string{"id"},
		Values: []snapshotCursorValue{
			{Kind: "int64", Int64: 42},
		},
	}
	plan := &snapshotPlan{
		dbName:      "app",
		tableName:   "orders",
		orderCols:   []string{"id"},
		keyCols:     []string{"id"},
		useKeyset:   true,
		quotedOrder: "`id`",
	}

	gotSQL, gotArgs, err := plan.buildSnapshotQuery(500, 123, cursor)
	if err != nil {
		t.Fatalf("buildSnapshotQuery returned error: %v", err)
	}

	wantSQL := "SELECT * FROM `app`.`orders` WHERE (`id` > ?) ORDER BY `id` LIMIT ?"
	if gotSQL != wantSQL {
		t.Fatalf("query = %q, want %q", gotSQL, wantSQL)
	}
	if got, want := len(gotArgs), 2; got != want {
		t.Fatalf("args length = %d, want %d", got, want)
	}
	if gotArgs[0] != int64(42) || gotArgs[1] != 500 {
		t.Fatalf("unexpected args: %#v", gotArgs)
	}
}

func TestSnapshotPlanBuildSnapshotQueryUsesOffsetFallback(t *testing.T) {
	plan := &snapshotPlan{
		dbName:      "app",
		tableName:   "logs",
		orderCols:   []string{"id", "created_at"},
		useKeyset:   false,
		quotedOrder: "`id`, `created_at`",
	}

	gotSQL, gotArgs, err := plan.buildSnapshotQuery(250, 900, nil)
	if err != nil {
		t.Fatalf("buildSnapshotQuery returned error: %v", err)
	}

	wantSQL := "SELECT * FROM `app`.`logs` ORDER BY `id`, `created_at` LIMIT ? OFFSET ?"
	if gotSQL != wantSQL {
		t.Fatalf("query = %q, want %q", gotSQL, wantSQL)
	}
	if got, want := len(gotArgs), 2; got != want {
		t.Fatalf("args length = %d, want %d", got, want)
	}
	if gotArgs[0] != 250 || gotArgs[1] != int64(900) {
		t.Fatalf("unexpected args: %#v", gotArgs)
	}
}

func TestSnapshotPlanBuildSnapshotQueryAppliesConfiguredFilter(t *testing.T) {
	plan := &snapshotPlan{
		dbName:      "app",
		tableName:   "orders",
		selectList:  "*",
		filter:      "`TglBerangkat` > '2025-01-01'",
		orderCols:   []string{"id"},
		keyCols:     []string{"id"},
		useKeyset:   true,
		quotedOrder: "`id`",
	}

	gotSQL, gotArgs, err := plan.buildSnapshotQuery(500, 0, nil)
	if err != nil {
		t.Fatalf("buildSnapshotQuery returned error: %v", err)
	}

	wantSQL := "SELECT * FROM `app`.`orders` WHERE (`TglBerangkat` > '2025-01-01') ORDER BY `id` LIMIT ?"
	if gotSQL != wantSQL {
		t.Fatalf("query = %q, want %q", gotSQL, wantSQL)
	}
	if got, want := len(gotArgs), 1; got != want {
		t.Fatalf("args length = %d, want %d", got, want)
	}
	if gotArgs[0] != 500 {
		t.Fatalf("unexpected args: %#v", gotArgs)
	}
}

func TestSnapshotPlanBuildSnapshotQueryCombinesFilterWithKeysetCursor(t *testing.T) {
	cursor := &snapshotCursor{
		Columns: []string{"id"},
		Values: []snapshotCursorValue{
			{Kind: "int64", Int64: 42},
		},
	}
	plan := &snapshotPlan{
		dbName:      "app",
		tableName:   "orders",
		selectList:  "*",
		filter:      "status = 'PAID'",
		orderCols:   []string{"id"},
		keyCols:     []string{"id"},
		useKeyset:   true,
		quotedOrder: "`id`",
	}

	gotSQL, gotArgs, err := plan.buildSnapshotQuery(500, 123, cursor)
	if err != nil {
		t.Fatalf("buildSnapshotQuery returned error: %v", err)
	}

	wantSQL := "SELECT * FROM `app`.`orders` WHERE (status = 'PAID') AND ((`id` > ?)) ORDER BY `id` LIMIT ?"
	if gotSQL != wantSQL {
		t.Fatalf("query = %q, want %q", gotSQL, wantSQL)
	}
	if got, want := len(gotArgs), 2; got != want {
		t.Fatalf("args length = %d, want %d", got, want)
	}
	if gotArgs[0] != int64(42) || gotArgs[1] != 500 {
		t.Fatalf("unexpected args: %#v", gotArgs)
	}
}

func TestSnapshotPlanBuildSnapshotQueryUsesCompositeSnapshotKeyColumns(t *testing.T) {
	cursor := &snapshotCursor{
		Columns: []string{"created_at", "kode"},
		Values: []snapshotCursorValue{
			{Kind: "time", Time: time.Date(2026, 4, 1, 10, 30, 0, 0, time.UTC).Format(time.RFC3339Nano)},
			{Kind: "string", String: "RSV-001"},
		},
	}
	plan := &snapshotPlan{
		dbName:      "source_orders",
		tableName:   "reservasi",
		filter:      "`created_at` >= '2026-04-01 00:00:00'",
		orderCols:   []string{"created_at", "kode"},
		keyCols:     []string{"created_at", "kode"},
		useKeyset:   true,
		quotedOrder: "`created_at`, `kode`",
	}

	gotSQL, gotArgs, err := plan.buildSnapshotQuery(10000, 10000, cursor)
	if err != nil {
		t.Fatalf("buildSnapshotQuery returned error: %v", err)
	}

	wantSQL := "SELECT * FROM `source_orders`.`reservasi` WHERE (`created_at` >= '2026-04-01 00:00:00') AND (((`created_at` > ?) OR (`created_at` = ? AND `kode` > ?))) ORDER BY `created_at`, `kode` LIMIT ?"
	if gotSQL != wantSQL {
		t.Fatalf("query = %q, want %q", gotSQL, wantSQL)
	}
	if got, want := len(gotArgs), 4; got != want {
		t.Fatalf("args length = %d, want %d", got, want)
	}
	if gotArgs[3] != 10000 {
		t.Fatalf("limit arg = %#v, want 10000", gotArgs[3])
	}
}

func TestSnapshotPlanBuildSnapshotQueryAppliesExtraSelectColumns(t *testing.T) {
	plan := &snapshotPlan{
		dbName:      "app",
		tableName:   "orders",
		selectList:  "`orders`.*, (SELECT MAX(created_at) FROM `app`.`events`) AS `event_created_at`",
		orderCols:   []string{"id"},
		keyCols:     []string{"id"},
		useKeyset:   true,
		quotedOrder: "`id`",
	}

	gotSQL, _, err := plan.buildSnapshotQuery(500, 0, nil)
	if err != nil {
		t.Fatalf("buildSnapshotQuery returned error: %v", err)
	}

	wantSQL := "SELECT `orders`.*, (SELECT MAX(created_at) FROM `app`.`events`) AS `event_created_at` FROM `app`.`orders` ORDER BY `id` LIMIT ?"
	if gotSQL != wantSQL {
		t.Fatalf("query = %q, want %q", gotSQL, wantSQL)
	}
}

func TestSourceSnapshotFilterForTablePrefersFullNameBeforeShortName(t *testing.T) {
	src := &Source{
		cfg: config.MySQLConfig{
			TableConfigs: map[string]config.MySQLTableConfig{
				"orders": {
					Filter: "status = 'SHORT'",
				},
				"app.orders": {
					Filter: "status = 'FULL'",
				},
			},
		},
	}

	got := src.snapshotFilterForTable("App", "Orders")
	if got != "status = 'FULL'" {
		t.Fatalf("snapshotFilterForTable = %q, want %q", got, "status = 'FULL'")
	}
}

func TestSourceSnapshotFilterForTableSupportsWildcardConfig(t *testing.T) {
	src := &Source{
		cfg: config.MySQLConfig{
			TableConfigs: map[string]config.MySQLTableConfig{
				"source_*.tbl_reservasi": {
					Filter: "`WaktuPesan` >= '2026-04-01 00:00:00'",
				},
				"source_alpha.tbl_reservasi": {
					Filter: "`WaktuPesan` >= '2026-05-01 00:00:00'",
				},
			},
		},
	}

	got := src.snapshotFilterForTable("source_beta", "tbl_reservasi")
	if got != "`WaktuPesan` >= '2026-04-01 00:00:00'" {
		t.Fatalf("wildcard snapshotFilterForTable = %q", got)
	}

	got = src.snapshotFilterForTable("source_alpha", "tbl_reservasi")
	if got != "`WaktuPesan` >= '2026-05-01 00:00:00'" {
		t.Fatalf("exact snapshotFilterForTable = %q", got)
	}
}

func TestSourceSnapshotFilterForTableRendersSchemaPlaceholder(t *testing.T) {
	src := &Source{
		cfg: config.MySQLConfig{
			TableConfigs: map[string]config.MySQLTableConfig{
				"source_*.tbl_biaya_op": {
					Filter: "EXISTS (SELECT 1 FROM `{{schema}}`.`tbl_penjadwalan_kendaraan` AS tpk WHERE tpk.`NomorManifest` = `tbl_biaya_op`.`NomorSPJ`)",
				},
			},
		},
	}

	got := src.snapshotFilterForTable("source_alpha", "tbl_biaya_op")
	want := "EXISTS (SELECT 1 FROM `source_alpha`.`tbl_penjadwalan_kendaraan` AS tpk WHERE tpk.`NomorManifest` = `tbl_biaya_op`.`NomorSPJ`)"
	if got != want {
		t.Fatalf("snapshotFilterForTable = %q, want %q", got, want)
	}
}

func TestSourceSnapshotKeyColumnsForTableResolvesConfiguredColumns(t *testing.T) {
	src := &Source{
		cfg: config.MySQLConfig{
			TableConfigs: map[string]config.MySQLTableConfig{
				"app.orders": {
					SnapshotKeyColumns: []string{" created_at ", "ID"},
				},
			},
		},
	}
	schema := &model.TableSchema{
		SchemaName: "app",
		TableName:  "orders",
		Columns: []model.TableColumn{
			{Name: "created_at"},
			{Name: "id"},
			{Name: "status"},
		},
	}

	cols, ok, err := src.snapshotKeyColumnsForTable("app", "orders", schema)
	if err != nil {
		t.Fatalf("snapshotKeyColumnsForTable returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected configured snapshot key columns")
	}
	want := []string{"created_at", "id"}
	if len(cols) != len(want) {
		t.Fatalf("cols = %#v, want %#v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("cols[%d] = %q, want %q", i, cols[i], want[i])
		}
	}
}

func TestSnapshotCursorRoundTripKeepsTypedValues(t *testing.T) {
	at := time.Date(2026, 4, 23, 10, 11, 12, 13000000, time.FixedZone("WIB", 7*3600))
	cursor := &snapshotCursor{
		Columns: []string{"id", "created_at", "code"},
		Values: []snapshotCursorValue{
			{Kind: "int64", Int64: 88},
			{Kind: "time", Time: at.Format(time.RFC3339Nano)},
			{Kind: "string", String: "A-1"},
		},
	}

	raw, err := encodeSnapshotCursor(cursor)
	if err != nil {
		t.Fatalf("encodeSnapshotCursor returned error: %v", err)
	}

	var decodedJSON map[string]any
	if err := json.Unmarshal([]byte(raw), &decodedJSON); err != nil {
		t.Fatalf("encoded cursor is not valid JSON: %v", err)
	}

	decoded, err := decodeSnapshotCursor(raw)
	if err != nil {
		t.Fatalf("decodeSnapshotCursor returned error: %v", err)
	}
	if !decoded.matchesColumns([]string{"id", "created_at", "code"}) {
		t.Fatalf("decoded cursor columns do not match")
	}

	args, err := decoded.queryArgs()
	if err != nil {
		t.Fatalf("queryArgs returned error: %v", err)
	}
	if got, want := len(args), 3; got != want {
		t.Fatalf("args length = %d, want %d", got, want)
	}
	if args[0] != int64(88) {
		t.Fatalf("args[0] = %#v, want 88", args[0])
	}
	gotTime, ok := args[1].(time.Time)
	if !ok {
		t.Fatalf("args[1] type = %T, want time.Time", args[1])
	}
	if !gotTime.Equal(at) {
		t.Fatalf("args[1] = %v, want %v", gotTime, at)
	}
	if args[2] != "A-1" {
		t.Fatalf("args[2] = %#v, want %q", args[2], "A-1")
	}
}

func TestRestoreSnapshotCursorRestartsWhenLegacyProgressLacksCursor(t *testing.T) {
	src := &Source{jobID: "job-1"}
	plan := &snapshotPlan{
		fullName:  "app.orders",
		keyCols:   []string{"id"},
		useKeyset: true,
	}

	cursor, rows, raw := src.restoreSnapshotCursor(plan, snapshotResumeState{
		rowsEmitted: 55,
	})

	if cursor != nil {
		t.Fatalf("expected nil cursor, got %#v", cursor)
	}
	if rows != 0 {
		t.Fatalf("rows = %d, want 0", rows)
	}
	if raw != "" {
		t.Fatalf("raw cursor = %q, want empty", raw)
	}
}

func TestResumeSnapshotOnlyProgressUsesSavedSnapshotProgress(t *testing.T) {
	store := &testOffsetStore{
		progress: &meta.SnapshotProgress{
			JobID:      "job-1",
			TableName:  "app.orders",
			NextOffset: 20,
		},
	}
	var reports []connector.ProgressInfo
	src := &Source{
		jobID:     "job-1",
		stateKey:  "rivus/v1/state-key",
		cfg:       config.MySQLConfig{Tables: []string{"app.customers", "app.orders"}},
		offsetSto: store,
		retry:     config.RetryPolicy{MaxAttempts: 1},
		progress: func(info connector.ProgressInfo) {
			reports = append(reports, info)
		},
	}

	resumeCalled := false
	resumed, err := src.resumeSnapshotOnlyProgress(context.Background(), nil, func(ctx context.Context, out chan<- model.Event) error {
		resumeCalled = true
		return nil
	})

	if err != nil {
		t.Fatalf("resumeSnapshotOnlyProgress returned error: %v", err)
	}
	if !resumed {
		t.Fatal("expected saved progress to trigger snapshot-only resume")
	}
	if !resumeCalled {
		t.Fatal("expected snapshot resume function to be called")
	}
	if store.clearSnapshotProgressCalls != 1 {
		t.Fatalf("ClearSnapshotProgress calls = %d, want 1", store.clearSnapshotProgressCalls)
	}
	if store.getSnapshotProgressJobID != "rivus/v1/state-key" || store.clearSnapshotProgressJobID != "rivus/v1/state-key" {
		t.Fatalf("snapshot state keys = get %q clear %q, want internal checkpoint key", store.getSnapshotProgressJobID, store.clearSnapshotProgressJobID)
	}
	if len(reports) < 2 {
		t.Fatalf("progress reports = %d, want at least 2", len(reports))
	}
	if got, want := reports[0].Summary, "Resuming snapshot-only load"; got != want {
		t.Fatalf("first progress summary = %q, want %q", got, want)
	}
	if got, want := reports[len(reports)-1].Summary, "Snapshot-only resume complete"; got != want {
		t.Fatalf("last progress summary = %q, want %q", got, want)
	}
}

func TestRunInitialSnapshotAllSkipsConfiguredTables(t *testing.T) {
	var reports []connector.ProgressInfo
	src := &Source{
		jobID: "job-1",
		cfg: config.MySQLConfig{
			Tables: []string{"app.matched", "app.done"},
		},
		progress: func(info connector.ProgressInfo) {
			reports = append(reports, info)
		},
	}
	src.SkipSnapshotTables([]connector.TableRef{
		{Schema: "app", Table: "matched"},
		{Schema: "app", Table: "done"},
	})

	if err := src.RunInitialSnapshotAll(context.Background(), make(chan model.Event)); err != nil {
		t.Fatalf("RunInitialSnapshotAll returned error: %v", err)
	}
	if got, want := len(reports), 2; got != want {
		t.Fatalf("progress reports = %d, want %d", got, want)
	}
	if got, want := reports[0].Summary, "Skipping snapshot table 1/2"; got != want {
		t.Fatalf("first progress summary = %q, want %q", got, want)
	}
	if got, want := reports[1].Summary, "Skipping snapshot table 2/2"; got != want {
		t.Fatalf("second progress summary = %q, want %q", got, want)
	}
}

func TestResumeOffsetMigratesLegacyVisibleJobKey(t *testing.T) {
	store := &testOffsetStore{
		offsets: map[string]*meta.Offset{
			"visible-job": {BinlogFile: "mysql-bin.000007", BinlogPos: 81},
		},
	}
	src := &Source{
		jobID:     "visible-job",
		stateKey:  "rivus/v1/checkpoint-key",
		offsetSto: store,
	}

	off, err := src.resumeOffset(context.Background())
	if err != nil {
		t.Fatalf("resumeOffset returned error: %v", err)
	}
	if off == nil || off.BinlogFile != "mysql-bin.000007" || off.BinlogPos != 81 {
		t.Fatalf("resume offset = %#v, want legacy position", off)
	}
	if store.savedOffsetJobID != "rivus/v1/checkpoint-key" {
		t.Fatalf("migrated offset key = %q, want internal checkpoint key", store.savedOffsetJobID)
	}
	if store.deletedJobID != "visible-job" {
		t.Fatalf("deleted legacy offset key = %q, want visible job key", store.deletedJobID)
	}
}

func TestResetInitialCheckpointClearsInternalAndLegacyState(t *testing.T) {
	store := &testOffsetStore{
		offsets: map[string]*meta.Offset{
			"visible-job":             {BinlogFile: "mysql-bin.000349", BinlogPos: 433582112},
			"rivus/v1/checkpoint-key": {BinlogFile: "mysql-bin.000349", BinlogPos: 433582112},
		},
	}
	src := &Source{
		jobID:     "visible-job",
		stateKey:  "rivus/v1/checkpoint-key",
		offsetSto: store,
	}

	if err := src.resetInitialCheckpoint(context.Background()); err != nil {
		t.Fatalf("resetInitialCheckpoint returned error: %v", err)
	}
	if store.offsets["visible-job"] != nil {
		t.Fatal("expected legacy visible job checkpoint to be cleared")
	}
	if store.offsets["rivus/v1/checkpoint-key"] != nil {
		t.Fatal("expected internal checkpoint key to be cleared")
	}
	if got, want := store.deletedJobIDs, []string{"visible-job", "rivus/v1/checkpoint-key"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deleted job IDs = %#v, want %#v", got, want)
	}
}

func TestDecodeSnapshotCursorRejectsEmptyPayload(t *testing.T) {
	_, err := decodeSnapshotCursor(`{"columns":["id"],"values":[]}`)
	if err == nil {
		t.Fatal("expected error for empty snapshot cursor")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShouldResumeSnapshotProgressUsesSavedTableProgress(t *testing.T) {
	store := &testOffsetStore{
		progress: &meta.SnapshotProgress{
			TableName:  "app.orders",
			NextOffset: 10000,
		},
	}
	src := &Source{
		jobID:     "visible-job",
		stateKey:  "rivus/v1/checkpoint-key",
		offsetSto: store,
	}

	resume, err := src.shouldResumeSnapshotProgress(context.Background())
	if err != nil {
		t.Fatalf("shouldResumeSnapshotProgress returned error: %v", err)
	}
	if !resume {
		t.Fatal("expected saved progress to trigger resume")
	}
	if store.getSnapshotProgressJobID != "rivus/v1/checkpoint-key" {
		t.Fatalf("progress key = %q, want internal checkpoint key", store.getSnapshotProgressJobID)
	}
}

type testOffsetStore struct {
	offset                     *meta.Offset
	snapshot                   *meta.SnapshotState
	progress                   *meta.SnapshotProgress
	offsets                    map[string]*meta.Offset
	savedOffsetJobID           string
	deletedJobID               string
	deletedJobIDs              []string
	clearSnapshotProgressCalls int
	getSnapshotProgressJobID   string
	clearSnapshotProgressJobID string
}

func (s *testOffsetStore) GetOffset(_ context.Context, jobID string) (*meta.Offset, error) {
	if s.offsets != nil {
		return s.offsets[jobID], nil
	}
	return s.offset, nil
}

func (s *testOffsetStore) SaveOffset(_ context.Context, jobID string, offset meta.Offset) error {
	s.savedOffsetJobID = jobID
	s.offset = &offset
	if s.offsets != nil {
		s.offsets[jobID] = &offset
	}
	return nil
}

func (s *testOffsetStore) GetSnapshotState(context.Context, string) (*meta.SnapshotState, error) {
	return s.snapshot, nil
}

func (s *testOffsetStore) SaveSnapshotStart(_ context.Context, jobID string, start meta.Offset) error {
	s.snapshot = &meta.SnapshotState{
		JobID:     jobID,
		StartFile: start.BinlogFile,
		StartPos:  start.BinlogPos,
	}
	return nil
}

func (s *testOffsetStore) MarkSnapshotDone(context.Context, string) error {
	if s.snapshot != nil {
		s.snapshot.Done = true
	}
	return nil
}

func (s *testOffsetStore) GetSnapshotProgress(_ context.Context, jobID string) (*meta.SnapshotProgress, error) {
	s.getSnapshotProgressJobID = jobID
	return s.progress, nil
}

func (s *testOffsetStore) SaveSnapshotProgress(_ context.Context, jobID string, tableName string, nextOffset int64, cursorJSON string) error {
	s.progress = &meta.SnapshotProgress{
		JobID:      jobID,
		TableName:  tableName,
		NextOffset: nextOffset,
		CursorJSON: cursorJSON,
	}
	return nil
}

func (s *testOffsetStore) ClearSnapshotProgress(_ context.Context, jobID string) error {
	s.clearSnapshotProgressCalls++
	s.clearSnapshotProgressJobID = jobID
	s.progress = nil
	return nil
}

func (s *testOffsetStore) DeleteJobState(_ context.Context, jobID string) error {
	s.deletedJobID = jobID
	s.deletedJobIDs = append(s.deletedJobIDs, jobID)
	s.offset = nil
	s.snapshot = nil
	s.progress = nil
	if s.offsets != nil {
		delete(s.offsets, jobID)
	}
	return nil
}

type fakeMasterPosRow struct {
	file string
	pos  uint32
	err  error
}

func (r fakeMasterPosRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 5 {
		return fmt.Errorf("dest length = %d, want 5", len(dest))
	}

	file, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("dest[0] type = %T, want *string", dest[0])
	}
	pos, ok := dest[1].(*uint32)
	if !ok {
		return fmt.Errorf("dest[1] type = %T, want *uint32", dest[1])
	}

	*file = r.file
	*pos = r.pos
	for _, dest := range dest[2:] {
		nullString, ok := dest.(*sql.NullString)
		if !ok {
			return fmt.Errorf("dest type = %T, want *sql.NullString", dest)
		}
		*nullString = sql.NullString{}
	}
	return nil
}

func fakeMasterPosQuery(queries *[]string, responses *[]masterPosRow) masterPosQueryFunc {
	return func(_ context.Context, query string) masterPosRow {
		*queries = append(*queries, query)
		if len(*responses) == 0 {
			return fakeMasterPosRow{err: fmt.Errorf("unexpected query %q", query)}
		}
		row := (*responses)[0]
		*responses = (*responses)[1:]
		return row
	}
}
