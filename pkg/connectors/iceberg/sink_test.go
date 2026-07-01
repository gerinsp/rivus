package iceberg

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	icerest "github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type emptyUnauthorizedRESTError struct{}

func (emptyUnauthorizedRESTError) Error() string { return ": " }
func (emptyUnauthorizedRESTError) Unwrap() error { return icerest.ErrUnauthorized }

type fakeCDCEqualityDeleter struct {
	calls      int
	gotState   *tableState
	gotKeys    []map[string]interface{}
	gotDeletes []pendingDelete
	gotPKCols  []string
	updated    *table.Table
	err        error
}

func (f *fakeCDCEqualityDeleter) DeleteEquality(ctx context.Context, state *tableState, keys []map[string]interface{}, deleteRows []pendingDelete, pkCols []string) (*table.Table, error) {
	f.calls++
	f.gotState = state
	f.gotKeys = keys
	f.gotDeletes = deleteRows
	f.gotPKCols = pkCols
	return f.updated, f.err
}

func TestCatalogInitializationErrorReportsUnauthorizedEmptyResponse(t *testing.T) {
	err := catalogInitializationError("http://catalog.example.test", "scraping", emptyUnauthorizedRESTError{})

	message := err.Error()
	for _, want := range []string{
		"initialize iceberg REST catalog",
		`warehouse="scraping"`,
		"unauthorized response with an empty body",
		"credential/oauth_token, scope, and catalog permissions",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("error = %q, want substring %q", message, want)
		}
	}
}

func TestStateOperationErrorIdentifiesCatalogAndTarget(t *testing.T) {
	sink := &Sink{cfg: config.IcebergConfig{Warehouse: "scraping"}}
	state := &tableState{
		sourceKey:       "scraping_operator.schedule_scraping",
		targetNamespace: "scraping_operator",
		targetTable:     "schedule_scraping",
	}

	err := sink.stateOperationError("overwrite", state, emptyUnauthorizedRESTError{})
	message := err.Error()
	for _, want := range []string{
		`overwrite source="scraping_operator.schedule_scraping"`,
		`target="scraping_operator.schedule_scraping"`,
		`warehouse="scraping"`,
		"catalog permissions",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("error = %q, want substring %q", message, want)
		}
	}
}

func TestCatalogRESTConfigUsesRunnerAppEnv(t *testing.T) {
	clearCatalogEnv(t)
	t.Setenv("ICEBERG_REST_URI", "http://gravitino:9001/iceberg")
	t.Setenv("ICEBERG_CATALOG_NAME", "raw")

	if got, want := catalogRESTURI(config.IcebergConfig{}), "http://gravitino:9001/iceberg"; got != want {
		t.Fatalf("catalogRESTURI = %q, want %q", got, want)
	}

	warehouse, err := catalogWarehouse(config.IcebergConfig{})
	if err != nil {
		t.Fatalf("catalogWarehouse returned error: %v", err)
	}
	if warehouse != "raw" {
		t.Fatalf("warehouse = %q, want raw", warehouse)
	}
}

func TestCatalogWarehouseTemplateMapsCatalogName(t *testing.T) {
	clearCatalogEnv(t)
	t.Setenv("ICEBERG_CATALOG_NAME", "analytics")
	t.Setenv("ICEBERG_WAREHOUSE_TEMPLATE", "{catalog}")

	warehouse, err := catalogWarehouse(config.IcebergConfig{})
	if err != nil {
		t.Fatalf("catalogWarehouse returned error: %v", err)
	}
	if warehouse != "analytics" {
		t.Fatalf("warehouse = %q, want analytics", warehouse)
	}

	_, err = catalogWarehouse(config.IcebergConfig{
		CatalogName:       "raw",
		WarehouseTemplate: "s3://warehouse/{unknown}",
	})
	if err == nil || !strings.Contains(err.Error(), "only supports the {catalog} placeholder") {
		t.Fatalf("catalogWarehouse error = %v, want unsupported placeholder error", err)
	}
}

func TestCatalogS3PropertiesUseGenericS3Env(t *testing.T) {
	clearCatalogEnv(t)
	t.Setenv("ICEBERG_S3_ENDPOINT", "https://s3.example.test")
	t.Setenv("ICEBERG_S3_PATH_STYLE", "true")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")

	props, err := catalogS3Properties(config.IcebergConfig{})
	if err != nil {
		t.Fatalf("catalogS3Properties returned error: %v", err)
	}
	for key, want := range map[string]string{
		"s3.endpoint":                 "https://s3.example.test",
		"s3.region":                   "us-east-1",
		"client.region":               "us-east-1",
		"s3.access-key-id":            "access",
		"s3.secret-access-key":        "secret",
		"s3.force-virtual-addressing": "false",
	} {
		if got := props[key]; got != want {
			t.Fatalf("props[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestCatalogRESTHeadersUseExplicitOrBasicAuth(t *testing.T) {
	clearCatalogEnv(t)
	t.Setenv("ICEBERG_REST_AUTH_HEADER", "Bearer token")
	if got := catalogRESTHeaders(config.IcebergConfig{})["Authorization"]; got != "Bearer token" {
		t.Fatalf("explicit auth header = %q, want Bearer token", got)
	}

	clearCatalogEnv(t)
	t.Setenv("ICEBERG_REST_BASIC_USERNAME", "admin")
	t.Setenv("ICEBERG_REST_BASIC_PASSWORD", "secret")
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:secret"))
	if got := catalogRESTHeaders(config.IcebergConfig{})["Authorization"]; got != want {
		t.Fatalf("basic auth header = %q, want %q", got, want)
	}
}

func clearCatalogEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"ICEBERG_REST_URI",
		"ICEBERG_CATALOG_URI",
		"ICEBERG_CATALOG_NAME",
		"ICEBERG_WAREHOUSE",
		"ICEBERG_WAREHOUSE_TEMPLATE",
		"ICEBERG_REST_AUTH_HEADER",
		"ICEBERG_REST_BASIC_USERNAME",
		"ICEBERG_REST_BASIC_PASSWORD",
		"GRAVITINO_SIMPLE_AUTH_USER",
		"GRAVITINO_SIMPLE_AUTH_PASSWORD",
		"ICEBERG_S3_REGION",
		"ICEBERG_S3_ENDPOINT",
		"ICEBERG_S3_PATH_STYLE",
		"ICEBERG_S3_FORCE_PATH_STYLE",
		"ICEBERG_S3_ACCESS_KEY_ID",
		"ICEBERG_S3_SECRET_ACCESS_KEY",
		"ICEBERG_S3_SESSION_TOKEN",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_S3_ENDPOINT",
		"AWS_ENDPOINT_URL_S3",
		"AWS_S3_PATH_STYLE",
		"AWS_S3_FORCE_PATH_STYLE",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
	} {
		t.Setenv(key, "")
	}
}

func TestSkipSnapshotTableWithoutPrimaryKeyUsesConfigAndOverrides(t *testing.T) {
	schema := &model.TableSchema{
		SchemaName: "app",
		TableName:  "without_pk",
		Columns: []model.TableColumn{
			{Name: "code", DataType: "varchar"},
		},
	}

	disabled := &Sink{cfg: config.IcebergConfig{}}
	if disabled.SkipSnapshotTableWithoutPrimaryKey("app", "without_pk", schema) {
		t.Fatal("expected skip to be disabled by default")
	}

	enabled := &Sink{cfg: config.IcebergConfig{SkipSnapshotTablesWithoutPK: true}}
	if !enabled.SkipSnapshotTableWithoutPrimaryKey("app", "without_pk", schema) {
		t.Fatal("expected no-PK schema to be skipped when config is enabled")
	}

	withOverride := &Sink{cfg: config.IcebergConfig{
		SkipSnapshotTablesWithoutPK: true,
		PrimaryKeys: map[string][]string{
			"app.without_pk": {"code"},
		},
	}}
	if withOverride.SkipSnapshotTableWithoutPrimaryKey("app", "without_pk", schema) {
		t.Fatal("expected configured primary_keys to prevent skip")
	}
}

func TestShouldFlushStateUsesPendingBatchAge(t *testing.T) {
	now := time.Date(2026, 6, 6, 1, 30, 0, 0, time.UTC)
	sink := &Sink{cfg: config.IcebergConfig{FlushSeconds: 300}}
	state := &tableState{
		pending:        []model.Event{{TraceID: "event-1"}},
		firstPendingAt: now.Add(-301 * time.Second),
		lastEventAt:    now.Add(-5 * time.Second),
	}

	if !sink.shouldFlushStateLocked(state, now) {
		t.Fatal("expected flush when pending batch age exceeds flush_seconds")
	}
}

func TestShouldFlushStateWaitsBeforeIdleOrAgeLimit(t *testing.T) {
	now := time.Date(2026, 6, 6, 1, 30, 0, 0, time.UTC)
	sink := &Sink{cfg: config.IcebergConfig{FlushSeconds: 300}}
	state := &tableState{
		pending:        []model.Event{{TraceID: "event-1"}},
		firstPendingAt: now.Add(-120 * time.Second),
		lastEventAt:    now.Add(-5 * time.Second),
	}

	if sink.shouldFlushStateLocked(state, now) {
		t.Fatal("did not expect flush before idle or pending age exceeds flush_seconds")
	}
}

func TestIcebergTypeForColumnClampsDecimalPrecision(t *testing.T) {
	precision := int64(65)
	scale := int64(30)
	typ, err := icebergTypeForColumn(model.TableColumn{
		Name:     "harga",
		DataType: "decimal",
		NumPrec:  &precision,
		NumScale: &scale,
	})
	if err != nil {
		t.Fatalf("icebergTypeForColumn returned error: %v", err)
	}

	dec, ok := typ.(iceberglib.DecimalType)
	if !ok {
		t.Fatalf("type = %T, want iceberg.DecimalType", typ)
	}
	if got, want := dec.Precision(), maxIcebergDecimalPrecision; got != want {
		t.Fatalf("precision = %d, want %d", got, want)
	}
	if got, want := dec.Scale(), 30; got != want {
		t.Fatalf("scale = %d, want %d", got, want)
	}
}

func TestShouldUpdateIcebergTypeSkipsExistingWiderType(t *testing.T) {
	if shouldUpdateIcebergType(iceberglib.PrimitiveTypes.Int64, iceberglib.PrimitiveTypes.Int32, false) {
		t.Fatal("existing long should be kept when source narrows to int")
	}
	if shouldUpdateIcebergType(iceberglib.DecimalTypeOf(20, 2), iceberglib.DecimalTypeOf(10, 2), false) {
		t.Fatal("existing wider decimal should be kept when source narrows precision")
	}
	if shouldUpdateIcebergType(iceberglib.PrimitiveTypes.String, iceberglib.PrimitiveTypes.Int32, false) {
		t.Fatal("existing string should be kept when source is a narrower scalar type")
	}
	if !shouldUpdateIcebergType(iceberglib.PrimitiveTypes.Int32, iceberglib.PrimitiveTypes.Int64, false) {
		t.Fatal("existing int should be promoted when source widens to long")
	}
	if !shouldUpdateIcebergType(iceberglib.PrimitiveTypes.Int64, iceberglib.PrimitiveTypes.Int32, true) {
		t.Fatal("unsafe type changes should preserve explicit narrowing requests")
	}
}

func TestBuildIcebergSchemaSkipsFloatIdentifierField(t *testing.T) {
	schema, err := buildIcebergSchema(&model.TableSchema{
		SchemaName: "source_alpha",
		TableName:  "tbl_md_harga_sewa_kendaraan",
		Columns: []model.TableColumn{
			{
				Name:       "HargaSewa",
				DataType:   "double",
				ColumnType: "double",
				IsPK:       true,
			},
		},
	}, []string{"HargaSewa"})
	if err != nil {
		t.Fatalf("buildIcebergSchema returned error: %v", err)
	}
	if got := schema.IdentifierFieldIDs; len(got) != 0 {
		t.Fatalf("identifier field ids = %#v, want none for double pk", got)
	}

	field, ok := schema.FindFieldByNameCaseInsensitive("HargaSewa")
	if !ok {
		t.Fatal("HargaSewa field not found")
	}
	if _, ok := field.Type.(iceberglib.Float64Type); !ok {
		t.Fatalf("HargaSewa type = %T, want iceberg.Float64Type", field.Type)
	}
}

func TestLiteralForDateValueAcceptsRFC3339Timestamp(t *testing.T) {
	lit, err := literalForValue(iceberglib.PrimitiveTypes.Date, "2026-03-01T00:00:00Z")
	if err != nil {
		t.Fatalf("literalForValue returned error: %v", err)
	}
	if lit == nil {
		t.Fatal("literalForValue returned nil literal")
	}
}

func TestLiteralForTimestampValueAcceptsMySQLDateTime(t *testing.T) {
	lit, err := literalForValue(iceberglib.PrimitiveTypes.Timestamp, "2026-04-01 00:00:00")
	if err != nil {
		t.Fatalf("literalForValue returned error: %v", err)
	}
	if lit == nil {
		t.Fatal("literalForValue returned nil literal")
	}
}

func TestTrinoLiteralForTimestampValueAcceptsMySQLDateTime(t *testing.T) {
	lit, err := trinoLiteralForValue(iceberglib.PrimitiveTypes.Timestamp, "2026-04-01 00:00:00")
	if err != nil {
		t.Fatalf("trinoLiteralForValue returned error: %v", err)
	}
	if got, want := lit, "TIMESTAMP '2026-04-01 00:00:00'"; got != want {
		t.Fatalf("literal = %q, want %q", got, want)
	}
}

func TestSnapshotReplaceFilterTrinoSQLBuildsTimestampDelete(t *testing.T) {
	schema := iceberglib.NewSchema(1,
		iceberglib.NestedField{ID: 1, Name: "kode", Type: iceberglib.PrimitiveTypes.String, Required: true},
		iceberglib.NestedField{ID: 2, Name: "created_at", Type: iceberglib.PrimitiveTypes.Timestamp},
	)
	meta, err := table.NewMetadata(schema, iceberglib.UnpartitionedSpec, table.UnsortedSortOrder, "s3://warehouse-generic/orders_bronze/reservasi", iceberglib.Properties{})
	if err != nil {
		t.Fatalf("NewMetadata returned error: %v", err)
	}

	sink := &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{
		Warehouse: "generic",
		SnapshotReplaceFilters: map[string]config.IcebergSnapshotReplaceFilterConfig{
			"source_orders.reservasi": {
				Column: "created_at",
				Op:     ">=",
				Value:  "2026-04-01 00:00:00",
			},
		},
	})}
	state := &tableState{
		sourceKey:       "source_orders.reservasi",
		targetNamespace: "orders_bronze",
		targetTable:     "reservasi",
		sourceSchema:    &model.TableSchema{SchemaName: "source_orders", TableName: "reservasi"},
		table:           table.New(table.Identifier{"generic", "orders_bronze", "reservasi"}, meta, "", nil, nil),
	}

	query, description, err := sink.snapshotReplaceFilterTrinoSQL(state)
	if err != nil {
		t.Fatalf("snapshotReplaceFilterTrinoSQL returned error: %v", err)
	}
	if got, want := description, "created_at >= 2026-04-01 00:00:00"; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	want := `DELETE FROM "generic"."orders_bronze"."reservasi" WHERE "created_at" >= TIMESTAMP '2026-04-01 00:00:00'`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestKeyDeleteTrinoSQLBuildsCompositePredicate(t *testing.T) {
	schema := iceberglib.NewSchema(1,
		iceberglib.NestedField{ID: 1, Name: "kode", Type: iceberglib.PrimitiveTypes.String, Required: true},
		iceberglib.NestedField{ID: 2, Name: "seq", Type: iceberglib.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := table.NewMetadata(schema, iceberglib.UnpartitionedSpec, table.UnsortedSortOrder, "s3://warehouse-generic/aragon_bronze/tbl_biaya_op", iceberglib.Properties{})
	if err != nil {
		t.Fatalf("NewMetadata returned error: %v", err)
	}

	sink := &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{
		Warehouse: "generic",
	})}
	state := &tableState{
		sourceKey:       "source_epsilon.tbl_biaya_op",
		targetNamespace: "aragon_bronze",
		targetTable:     "tbl_biaya_op",
		table:           table.New(table.Identifier{"generic", "aragon_bronze", "tbl_biaya_op"}, meta, "", nil, nil),
	}

	query, err := sink.keyDeleteTrinoSQL(state, []map[string]interface{}{
		{"kode": "A'01", "seq": int64(7)},
		{"kode": "B02", "seq": int64(8)},
	})
	if err != nil {
		t.Fatalf("keyDeleteTrinoSQL returned error: %v", err)
	}
	want := `DELETE FROM "generic"."aragon_bronze"."tbl_biaya_op" WHERE ("kode" = 'A''01' AND "seq" = 7) OR ("kode" = 'B02' AND "seq" = 8)`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestBuildRecordReaderConvertsMySQLZeroDateToNull(t *testing.T) {
	schema := iceberglib.NewSchema(1,
		iceberglib.NestedField{ID: 1, Name: "id", Type: iceberglib.PrimitiveTypes.Int64, Required: true},
		iceberglib.NestedField{ID: 2, Name: "UmurInfant", Type: iceberglib.PrimitiveTypes.Date, Required: false},
	)
	reader, release, err := buildRecordReader(schema, []map[string]interface{}{
		{"id": int64(1), "UmurInfant": "0000-00-00"},
	})
	if err != nil {
		t.Fatalf("buildRecordReader returned error: %v", err)
	}
	defer release()

	if !reader.Next() {
		t.Fatal("reader produced no record")
	}
	rec := reader.Record()
	if rec.Column(1).NullN() != 1 {
		t.Fatalf("UmurInfant null count = %d, want 1", rec.Column(1).NullN())
	}
}

func TestSplitRowsByLimitsHonorsRowLimit(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	}
	batches := splitRowsByLimits(rows, 2, 0)
	if got, want := len(batches), 2; got != want {
		t.Fatalf("batch count = %d, want %d", got, want)
	}
	if got, want := len(batches[0]), 2; got != want {
		t.Fatalf("first batch length = %d, want %d", got, want)
	}
	if got, want := len(batches[1]), 1; got != want {
		t.Fatalf("second batch length = %d, want %d", got, want)
	}
}

func TestPendingBytesLimitReached(t *testing.T) {
	sink := &Sink{cfg: config.IcebergConfig{MaxBatchBytes: config.ByteSize(100)}}
	state := &tableState{pendingBytes: 100}
	if !sink.pendingBytesLimitReached(state) {
		t.Fatal("pending byte limit should be reached")
	}
}

func TestNormalizeIcebergConfigDefaultsSnapshotWriteMode(t *testing.T) {
	cfg := normalizeIcebergConfig(config.IcebergConfig{})
	if got, want := cfg.SnapshotWriteMode, snapshotWriteModeOverwrite; got != want {
		t.Fatalf("SnapshotWriteMode = %q, want %q", got, want)
	}
	if got, want := cfg.SnapshotReplaceDeleteExecutor, snapshotReplaceDeleteExecutorNative; got != want {
		t.Fatalf("SnapshotReplaceDeleteExecutor = %q, want %q", got, want)
	}
	if got, want := cfg.CDCDeleteExecutor, snapshotReplaceDeleteExecutorNative; got != want {
		t.Fatalf("CDCDeleteExecutor = %q, want %q", got, want)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{SnapshotWriteMode: " Delete-Append "})
	if got, want := cfg.SnapshotWriteMode, snapshotWriteModeDeleteAppend; got != want {
		t.Fatalf("SnapshotWriteMode = %q, want %q", got, want)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{SnapshotReplaceDeleteExecutor: " Trino "})
	if got, want := cfg.SnapshotReplaceDeleteExecutor, snapshotReplaceDeleteExecutorTrino; got != want {
		t.Fatalf("SnapshotReplaceDeleteExecutor = %q, want %q", got, want)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{CDCDeleteExecutor: " Trino "})
	if got, want := cfg.CDCDeleteExecutor, snapshotReplaceDeleteExecutorTrino; got != want {
		t.Fatalf("CDCDeleteExecutor = %q, want %q", got, want)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{SnapshotWriteMode: " Auto "})
	if got, want := cfg.SnapshotWriteMode, snapshotWriteModeAuto; got != want {
		t.Fatalf("SnapshotWriteMode = %q, want %q", got, want)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{SnapshotWriteMode: " Truncate-Append "})
	if got, want := cfg.SnapshotWriteMode, snapshotWriteModeTruncateAppend; got != want {
		t.Fatalf("SnapshotWriteMode = %q, want %q", got, want)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{
		SnapshotTruncateTables:        []string{" APP.TBL_PELANGGAN ", " app.* "},
		SnapshotTruncateExcludeTables: []string{" APP.TBL_RESERVASI "},
		SnapshotReplaceFilters: map[string]config.IcebergSnapshotReplaceFilterConfig{
			" APP.TBL_RESERVASI ": {
				Column: " created_at ",
				Op:     " >= ",
				Value:  " 2026-04-01 00:00:00 ",
			},
		},
	})
	filter, ok := cfg.SnapshotReplaceFilters["app.tbl_reservasi"]
	if !ok {
		t.Fatalf("normalized snapshot replace filter missing")
	}
	if got, want := filter.Column, "created_at"; got != want {
		t.Fatalf("replace filter column = %q, want %q", got, want)
	}
	if got, want := filter.Op, ">="; got != want {
		t.Fatalf("replace filter op = %q, want %q", got, want)
	}
	if got, want := filter.Value, "2026-04-01 00:00:00"; got != want {
		t.Fatalf("replace filter value = %q, want %q", got, want)
	}
	if got, want := cfg.SnapshotTruncateTables, []string{"app.tbl_pelanggan", "app.*"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("SnapshotTruncateTables = %#v, want %#v", got, want)
	}
	if got, want := cfg.SnapshotTruncateExcludeTables, []string{"app.tbl_reservasi"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("SnapshotTruncateExcludeTables = %#v, want %#v", got, want)
	}
}

func TestNewSinkRequiresTrinoDeleteURIForTrinoExecutor(t *testing.T) {
	_, err := NewSink("job", "state", "job", config.IcebergConfig{
		SnapshotReplaceDeleteExecutor: snapshotReplaceDeleteExecutorTrino,
	}, config.RetryPolicy{}, nil, nil)
	if err == nil {
		t.Fatal("expected trino delete executor without uri to fail")
	}
	if !strings.Contains(err.Error(), "requires trino_delete.uri") {
		t.Fatalf("error = %q, want trino_delete.uri guidance", err.Error())
	}

	_, err = NewSink("job", "state", "job", config.IcebergConfig{
		CDCDeleteExecutor: snapshotReplaceDeleteExecutorTrino,
	}, config.RetryPolicy{}, nil, nil)
	if err == nil {
		t.Fatal("expected cdc trino delete executor without uri to fail")
	}
	if !strings.Contains(err.Error(), "requires trino_delete.uri") {
		t.Fatalf("error = %q, want trino_delete.uri guidance", err.Error())
	}
}

func TestCDCKeyDeleteExecutorUsesExplicitEqualityOnly(t *testing.T) {
	cfg := normalizeIcebergConfig(config.IcebergConfig{})
	if got := cdcKeyDeleteExecutor(cfg); got != "" {
		t.Fatalf("default CDC key delete executor = %q, want disabled native equality", got)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{
		CDCDeleteExecutor: cdcDeleteExecutorEquality,
	})
	if got, want := cdcKeyDeleteExecutor(cfg), cdcDeleteExecutorEquality; got != want {
		t.Fatalf("explicit equality CDC key delete executor = %q, want %q", got, want)
	}

	cfg = normalizeIcebergConfig(config.IcebergConfig{
		CDCDeleteExecutor: snapshotReplaceDeleteExecutorTrino,
		TrinoDelete: config.IcebergTrinoDeleteConfig{
			URI: "http://trino:8080",
		},
	})
	if got, want := cdcKeyDeleteExecutor(cfg), snapshotReplaceDeleteExecutorTrino; got != want {
		t.Fatalf("explicit trino CDC key delete executor = %q, want %q", got, want)
	}
}

func TestCDCKeyDeleteExecutorFallsBackToTrinoURI(t *testing.T) {
	cfg := normalizeIcebergConfig(config.IcebergConfig{
		TrinoDelete: config.IcebergTrinoDeleteConfig{
			URI: "http://trino:8080",
		},
	})

	if got, want := cdcKeyDeleteExecutor(cfg), snapshotReplaceDeleteExecutorTrino; got != want {
		t.Fatalf("CDC key delete executor with trino URI = %q, want %q", got, want)
	}
}

func TestApplyKeyDeleteWithEqualityDeletesUsesConfiguredDeleter(t *testing.T) {
	deleter := &fakeCDCEqualityDeleter{}
	sink := &Sink{
		jobID:           "job-1",
		cfg:             config.IcebergConfig{MaxConcurrentCommits: 1},
		equalityDeleter: deleter,
		states:          make(map[string]*tableState),
	}
	state := &tableState{
		sourceKey:       "app.orders",
		targetNamespace: "bronze",
		targetTable:     "orders",
	}
	sink.states[state.sourceKey] = state
	key := map[string]interface{}{"id": int64(10)}
	deleteRow := pendingDelete{key: key, row: map[string]interface{}{"id": int64(10)}}
	batch := &reducedBatch{
		deleteKeys:  []map[string]interface{}{key},
		deleteRows:  []pendingDelete{deleteRow},
		pkCols:      []string{"id"},
		deleteCount: 1,
	}

	if err := sink.applyKeyDeleteWithEqualityDeletes(context.Background(), state, batch, "delete-equality"); err != nil {
		t.Fatalf("applyKeyDeleteWithEqualityDeletes returned error: %v", err)
	}

	if got, want := deleter.calls, 1; got != want {
		t.Fatalf("deleter calls = %d, want %d", got, want)
	}
	if deleter.gotState != state {
		t.Fatal("deleter received different table state")
	}
	if got, want := len(deleter.gotKeys), 1; got != want {
		t.Fatalf("deleter keys length = %d, want %d", got, want)
	}
	if got := deleter.gotKeys[0]["id"]; got != int64(10) {
		t.Fatalf("deleter key id = %v, want 10", got)
	}
	if got, want := len(deleter.gotDeletes), 1; got != want {
		t.Fatalf("deleter delete rows length = %d, want %d", got, want)
	}
	if got, want := deleter.gotPKCols, []string{"id"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("deleter pk cols = %v, want %v", got, want)
	}
}

func TestSetTrinoHeadersUsesBasicAuthPassword(t *testing.T) {
	sink := &Sink{cfg: config.IcebergConfig{
		Warehouse: "generic",
		TrinoDelete: config.IcebergTrinoDeleteConfig{
			User:     "rivus",
			Password: "secret",
		},
	}}
	req, err := http.NewRequest(http.MethodGet, "http://trino:8080/v1/statement", nil)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}

	sink.setTrinoHeaders(req, "orders_bronze")

	gotUser, gotPassword, ok := req.BasicAuth()
	if !ok {
		t.Fatal("expected basic auth header")
	}
	if gotUser != "rivus" || gotPassword != "secret" {
		t.Fatalf("basic auth = %q/%q, want rivus/secret", gotUser, gotPassword)
	}
	if got, want := req.Header.Get("X-Trino-Catalog"), "generic"; got != want {
		t.Fatalf("X-Trino-Catalog = %q, want %q", got, want)
	}
	if got, want := req.Header.Get("X-Trino-Schema"), "orders_bronze"; got != want {
		t.Fatalf("X-Trino-Schema = %q, want %q", got, want)
	}
}

func TestTrinoAuthModePrefersBearerOverPassword(t *testing.T) {
	sink := &Sink{cfg: config.IcebergConfig{}}
	if got, want := sink.trinoAuthMode(), "none"; got != want {
		t.Fatalf("trinoAuthMode = %q, want %q", got, want)
	}

	sink.cfg.TrinoDelete.Password = "secret"
	if got, want := sink.trinoAuthMode(), "basic"; got != want {
		t.Fatalf("trinoAuthMode = %q, want %q", got, want)
	}

	sink.cfg.TrinoDelete.AccessToken = "token"
	if got, want := sink.trinoAuthMode(), "bearer"; got != want {
		t.Fatalf("trinoAuthMode = %q, want %q", got, want)
	}
}

func TestValidateSnapshotTruncateConfigRejectsWildcardByDefault(t *testing.T) {
	err := validateSnapshotTruncateConfig(normalizeIcebergConfig(config.IcebergConfig{
		SnapshotTruncateTables: []string{"app.*"},
	}))
	if err == nil {
		t.Fatal("expected wildcard snapshot truncate config to be rejected by default")
	}
	if !strings.Contains(err.Error(), "snapshot_truncate_allow_patterns") {
		t.Fatalf("error = %q, want snapshot_truncate_allow_patterns guidance", err.Error())
	}

	err = validateSnapshotTruncateConfig(normalizeIcebergConfig(config.IcebergConfig{
		SnapshotTruncateTables:        []string{"app.*"},
		SnapshotTruncateAllowPatterns: true,
	}))
	if err != nil {
		t.Fatalf("wildcard truncate with explicit allow returned error: %v", err)
	}
}

func TestSnapshotWriteModeAutoSelectsByTargetState(t *testing.T) {
	sink := &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{SnapshotWriteMode: snapshotWriteModeAuto})}
	if got, want := sink.snapshotWriteModeForState(true), snapshotWriteModeAppend; got != want {
		t.Fatalf("empty target selected mode = %q, want %q", got, want)
	}
	if got, want := sink.snapshotWriteModeForState(false), snapshotWriteModeDeleteAppend; got != want {
		t.Fatalf("existing target selected mode = %q, want %q", got, want)
	}

	sink = &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{
		SnapshotWriteMode: snapshotWriteModeAuto,
		SnapshotReplaceFilters: map[string]config.IcebergSnapshotReplaceFilterConfig{
			"app.tbl_reservasi": {Column: "created_at", Op: ">=", Value: "2026-04-01 00:00:00"},
		},
	})}
	state := &tableState{
		sourceKey:          "app.tbl_reservasi",
		snapshotAppendSafe: false,
		sourceSchema:       &model.TableSchema{SchemaName: "app", TableName: "tbl_reservasi"},
	}
	if got, want := sink.snapshotWriteModeForTableState(state), snapshotWriteModeReplaceFilterAppend; got != want {
		t.Fatalf("existing filtered target selected mode = %q, want %q", got, want)
	}

	sink = &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{
		SnapshotWriteMode:             snapshotWriteModeAuto,
		SnapshotTruncateTables:        []string{"app.*"},
		SnapshotTruncateExcludeTables: []string{"app.tbl_biaya_op"},
		SnapshotTruncateAllowPatterns: true,
		SnapshotReplaceFilters: map[string]config.IcebergSnapshotReplaceFilterConfig{
			"app.tbl_reservasi": {Column: "created_at", Op: ">=", Value: "2026-04-01 00:00:00"},
		},
	})}
	state = &tableState{
		sourceKey:          "app.tbl_reservasi",
		snapshotAppendSafe: false,
		sourceSchema:       &model.TableSchema{SchemaName: "app", TableName: "tbl_reservasi"},
	}
	if got, want := sink.snapshotWriteModeForTableState(state), snapshotWriteModeReplaceFilterAppend; got != want {
		t.Fatalf("filtered table selected mode = %q, want %q", got, want)
	}

	state = &tableState{
		sourceKey:          "app.tbl_pelanggan",
		snapshotAppendSafe: false,
		sourceSchema:       &model.TableSchema{SchemaName: "app", TableName: "tbl_pelanggan"},
	}
	if got, want := sink.snapshotWriteModeForTableState(state), snapshotWriteModeTruncateAppend; got != want {
		t.Fatalf("unfiltered wildcard truncate table selected mode = %q, want %q", got, want)
	}

	state = &tableState{
		sourceKey:          "app.tbl_biaya_op",
		snapshotAppendSafe: false,
		sourceSchema:       &model.TableSchema{SchemaName: "app", TableName: "tbl_biaya_op"},
	}
	if got, want := sink.snapshotWriteModeForTableState(state), snapshotWriteModeDeleteAppend; got != want {
		t.Fatalf("truncate excluded table selected mode = %q, want %q", got, want)
	}
}

func TestSnapshotAutoTargetCountSkippedForExplicitModes(t *testing.T) {
	sink := &Sink{cfg: normalizeIcebergConfig(config.IcebergConfig{
		SnapshotWriteMode:             snapshotWriteModeAuto,
		SnapshotTruncateTables:        []string{"app.*"},
		SnapshotTruncateExcludeTables: []string{"app.tbl_biaya_op"},
		SnapshotTruncateAllowPatterns: true,
		SnapshotReplaceFilters: map[string]config.IcebergSnapshotReplaceFilterConfig{
			"app.tbl_reservasi": {Column: "created_at", Op: ">=", Value: "2026-04-01 00:00:00"},
		},
	})}

	replaceState := &tableState{
		sourceKey:    "app.tbl_reservasi",
		sourceSchema: &model.TableSchema{SchemaName: "app", TableName: "tbl_reservasi"},
	}
	if sink.shouldCountSnapshotAutoTarget(replaceState) {
		t.Fatal("replace-filter table should not need target count")
	}

	truncateState := &tableState{
		sourceKey:    "app.tbl_member",
		sourceSchema: &model.TableSchema{SchemaName: "app", TableName: "tbl_member"},
	}
	if sink.shouldCountSnapshotAutoTarget(truncateState) {
		t.Fatal("truncate table should not need target count")
	}

	fallbackState := &tableState{
		sourceKey:    "app.tbl_biaya_op",
		sourceSchema: &model.TableSchema{SchemaName: "app", TableName: "tbl_biaya_op"},
	}
	if !sink.shouldCountSnapshotAutoTarget(fallbackState) {
		t.Fatal("auto fallback table should still count target rows")
	}
}

func TestReduceEventsCollapsesToLatestRow(t *testing.T) {
	events := []model.Event{
		{
			Type:   model.EventTypeInsert,
			Schema: "db1",
			Table:  "orders",
			Data: map[string]interface{}{
				"id":     int64(10),
				"status": "NEW",
			},
			Timestamp: time.Now(),
		},
		{
			Type:   model.EventTypeUpdate,
			Schema: "db1",
			Table:  "orders",
			OldData: map[string]interface{}{
				"id":     int64(10),
				"status": "NEW",
			},
			Data: map[string]interface{}{
				"id":     int64(10),
				"status": "PAID",
			},
			Timestamp: time.Now(),
		},
		{
			Type:   model.EventTypeDelete,
			Schema: "db1",
			Table:  "orders",
			Data: map[string]interface{}{
				"id": int64(20),
			},
			Timestamp: time.Now(),
		},
	}

	rows, deletes, err := reduceEvents(events, []string{"id"})
	if err != nil {
		t.Fatalf("reduceEvents returned error: %v", err)
	}

	if got, want := len(rows), 1; got != want {
		t.Fatalf("rows length = %d, want %d", got, want)
	}
	if got, want := rows[0]["status"], "PAID"; got != want {
		t.Fatalf("rows[0].status = %v, want %v", got, want)
	}
	if got, want := len(deletes), 2; got != want {
		t.Fatalf("deletes length = %d, want %d", got, want)
	}
}

func TestReduceEventsInsertUsesOverwriteKeys(t *testing.T) {
	events := []model.Event{
		{
			Type:   model.EventTypeInsert,
			Schema: "db1",
			Table:  "customers",
			Data: map[string]interface{}{
				"id":   int64(1),
				"name": "Alice",
			},
			Timestamp: time.Now(),
		},
		{
			Type:   model.EventTypeInsert,
			Schema: "db1",
			Table:  "customers",
			Data: map[string]interface{}{
				"id":   int64(2),
				"name": "Bob",
			},
			Timestamp: time.Now(),
		},
	}

	rows, deletes, err := reduceEvents(events, []string{"id"})
	if err != nil {
		t.Fatalf("reduceEvents returned error: %v", err)
	}

	if got, want := len(rows), 2; got != want {
		t.Fatalf("rows length = %d, want %d", got, want)
	}
	if got, want := len(deletes), 2; got != want {
		t.Fatalf("deletes length = %d, want %d", got, want)
	}
	if got, want := deletes[0]["id"], int64(1); got != want {
		t.Fatalf("deletes[0].id = %v, want %v", got, want)
	}
	if got, want := deletes[1]["id"], int64(2); got != want {
		t.Fatalf("deletes[1].id = %v, want %v", got, want)
	}
}

func TestReduceEventsDetailedPreservesDeleteRowsForPartitionedEquality(t *testing.T) {
	oldEventDate := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	newEventDate := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	events := []model.Event{
		{
			Type:   model.EventTypeUpdate,
			Schema: "db1",
			Table:  "orders",
			OldData: map[string]interface{}{
				"id":         int64(10),
				"event_date": oldEventDate,
			},
			Data: map[string]interface{}{
				"id":         int64(10),
				"event_date": newEventDate,
			},
			Timestamp: time.Now(),
		},
	}

	_, deletes, err := reduceEventsDetailed(events, []string{"id"})
	if err != nil {
		t.Fatalf("reduceEventsDetailed returned error: %v", err)
	}
	if got, want := len(deletes), 1; got != want {
		t.Fatalf("deletes length = %d, want %d", got, want)
	}
	if got := deletes[0].row["event_date"]; got != oldEventDate {
		t.Fatalf("delete row event_date = %v, want old value %v", got, oldEventDate)
	}
	if got := deletes[0].partitionRows[0]["event_date"]; got != oldEventDate {
		t.Fatalf("delete partition row event_date = %v, want old value %v", got, oldEventDate)
	}
}

func TestReduceEventsDetailedKeepsEveryPartitionMoveForSameKey(t *testing.T) {
	firstEventDate := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	secondEventDate := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	thirdEventDate := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	events := []model.Event{
		{
			Type:   model.EventTypeUpdate,
			Schema: "db1",
			Table:  "orders",
			OldData: map[string]interface{}{
				"id":         int64(10),
				"event_date": firstEventDate,
			},
			Data: map[string]interface{}{
				"id":         int64(10),
				"event_date": secondEventDate,
			},
			Timestamp: time.Now(),
		},
		{
			Type:   model.EventTypeUpdate,
			Schema: "db1",
			Table:  "orders",
			OldData: map[string]interface{}{
				"id":         int64(10),
				"event_date": secondEventDate,
			},
			Data: map[string]interface{}{
				"id":         int64(10),
				"event_date": thirdEventDate,
			},
			Timestamp: time.Now(),
		},
	}

	rows, deletes, err := reduceEventsDetailed(events, []string{"id"})
	if err != nil {
		t.Fatalf("reduceEventsDetailed returned error: %v", err)
	}
	if got, want := len(rows), 1; got != want {
		t.Fatalf("rows length = %d, want %d", got, want)
	}
	if got := rows[0].row["event_date"]; got != thirdEventDate {
		t.Fatalf("final row event_date = %v, want %v", got, thirdEventDate)
	}
	if got, want := len(deletes), 1; got != want {
		t.Fatalf("deletes length = %d, want %d", got, want)
	}
	if got, want := len(deletes[0].partitionRows), 2; got != want {
		t.Fatalf("partition row count = %d, want %d", got, want)
	}
	if got := deletes[0].partitionRows[0]["event_date"]; got != firstEventDate {
		t.Fatalf("first partition row event_date = %v, want %v", got, firstEventDate)
	}
	if got := deletes[0].partitionRows[1]["event_date"]; got != secondEventDate {
		t.Fatalf("second partition row event_date = %v, want %v", got, secondEventDate)
	}
}

func TestEqualityDeletePartitionDataDayTransform(t *testing.T) {
	schema := iceberglib.NewSchema(1,
		iceberglib.NestedField{ID: 1, Name: "id", Type: iceberglib.PrimitiveTypes.Int64, Required: true},
		iceberglib.NestedField{ID: 2, Name: "event_date", Type: iceberglib.PrimitiveTypes.Date},
	)
	spec := iceberglib.NewPartitionSpecID(1, iceberglib.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "event_date_day",
		Transform: iceberglib.DayTransform{},
	})
	eventDate := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	partitionData, err := equalityDeletePartitionData(schema, spec, map[string]interface{}{
		"id":         int64(10),
		"event_date": eventDate,
	})
	if err != nil {
		t.Fatalf("equalityDeletePartitionData returned error: %v", err)
	}

	expectedDays := int32(eventDate.Unix() / int64((24 * time.Hour).Seconds()))
	if got, want := partitionData[1000], expectedDays; got != want {
		t.Fatalf("partition day = %v (%T), want %v", got, got, want)
	}
}

func TestPartitionDeleteGroupsUsesEveryPartitionMoveForSameKey(t *testing.T) {
	schema := iceberglib.NewSchema(1,
		iceberglib.NestedField{ID: 1, Name: "id", Type: iceberglib.PrimitiveTypes.Int64, Required: true},
		iceberglib.NestedField{ID: 2, Name: "event_date", Type: iceberglib.PrimitiveTypes.Date},
	)
	spec := iceberglib.NewPartitionSpecID(1, iceberglib.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "event_date_day",
		Transform: iceberglib.DayTransform{},
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, "s3://warehouse-test/test_bronze/orders", iceberglib.Properties{})
	if err != nil {
		t.Fatalf("NewMetadata returned error: %v", err)
	}
	tbl := table.New(table.Identifier{"ds", "test_bronze", "orders"}, meta, "", nil, nil)

	firstEventDate := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	secondEventDate := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	groups, err := partitionDeleteGroups(tbl, []pendingDelete{
		{
			key: map[string]interface{}{"id": int64(10)},
			partitionRows: []map[string]interface{}{
				{"id": int64(10), "event_date": firstEventDate},
				{"id": int64(10), "event_date": secondEventDate},
			},
		},
	})
	if err != nil {
		t.Fatalf("partitionDeleteGroups returned error: %v", err)
	}
	if got, want := len(groups), 2; got != want {
		t.Fatalf("group count = %d, want %d", got, want)
	}
	for _, group := range groups {
		if got, want := len(group.keys), 1; got != want {
			t.Fatalf("group key count = %d, want %d", got, want)
		}
		if got, want := group.keys[0]["id"], int64(10); got != want {
			t.Fatalf("group key id = %v, want %v", got, want)
		}
	}
}

func TestResolveTargetSupportsTableGlobOverride(t *testing.T) {
	sink := &Sink{
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			DefaultNamespace: "raw",
			Overrides: map[string]config.IcebergTarget{
				"app.tbl_reservasi_backup_*": {
					Table: "tbl_reservasi",
				},
			},
		}),
	}

	namespace, tableName := sink.ResolveTarget("app", "tbl_reservasi_backup_202501")
	if namespace != "raw" {
		t.Fatalf("namespace = %q, want %q", namespace, "raw")
	}
	if tableName != "tbl_reservasi" {
		t.Fatalf("table = %q, want %q", tableName, "tbl_reservasi")
	}
}

func TestResolveTargetSupportsBareTableOverride(t *testing.T) {
	sink := &Sink{
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			DefaultNamespace: "raw",
			Overrides: map[string]config.IcebergTarget{
				"tbl_reservasi_backup": {
					Table: "tbl_reservasi",
				},
			},
		}),
	}

	namespace, tableName := sink.ResolveTarget("source_alpha", "tbl_reservasi_backup")
	if namespace != "raw" {
		t.Fatalf("namespace = %q, want %q", namespace, "raw")
	}
	if tableName != "tbl_reservasi" {
		t.Fatalf("table = %q, want %q", tableName, "tbl_reservasi")
	}

	_, tableName = sink.ResolveTarget("source_beta", "tbl_reservasi_backup")
	if tableName != "tbl_reservasi" {
		t.Fatalf("table = %q, want %q", tableName, "tbl_reservasi")
	}
}

func TestResolveTargetExactOverrideWinsOverGlob(t *testing.T) {
	sink := &Sink{
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			DefaultNamespace: "raw",
			Overrides: map[string]config.IcebergTarget{
				"app.tbl_reservasi_backup_*": {
					Table: "tbl_reservasi",
				},
				"app.tbl_reservasi_backup_202501": {
					Table: "tbl_reservasi_january",
				},
			},
		}),
	}

	_, tableName := sink.ResolveTarget("app", "tbl_reservasi_backup_202501")
	if tableName != "tbl_reservasi_january" {
		t.Fatalf("table = %q, want %q", tableName, "tbl_reservasi_january")
	}
}

func TestResolveTargetPrefersMostSpecificGlob(t *testing.T) {
	sink := &Sink{
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			DefaultNamespace: "raw",
			Overrides: map[string]config.IcebergTarget{
				"app.tbl_*": {
					Table: "generic_table",
				},
				"app.tbl_reservasi_backup_*": {
					Table: "tbl_reservasi",
				},
			},
		}),
	}

	_, tableName := sink.ResolveTarget("app", "tbl_reservasi_backup_202501")
	if tableName != "tbl_reservasi" {
		t.Fatalf("table = %q, want %q", tableName, "tbl_reservasi")
	}
}

func TestMetadataConfigEnablesCreatedAtSourceMappings(t *testing.T) {
	cfg := normalizeIcebergConfig(config.IcebergConfig{
		MetadataColumns: config.IcebergMetadataColumnsConfig{
			CreatedAt: config.IcebergCreatedAtColumnConfig{
				SourceColumns: map[string]string{
					"APP.TBL_RESERVASI": " waktu_pesan ",
				},
			},
		},
	})

	if !cfg.MetadataColumns.CreatedAt.Enabled {
		t.Fatalf("created_at metadata should be enabled when source mappings are configured")
	}
	if got, want := cfg.MetadataColumns.CreatedAt.Name, "created_at"; got != want {
		t.Fatalf("created_at name = %q, want %q", got, want)
	}
	if got, want := cfg.MetadataColumns.CreatedAt.SourceColumns["app.tbl_reservasi"], "waktu_pesan"; got != want {
		t.Fatalf("created_at source column = %q, want %q", got, want)
	}
}

func TestAugmentSchemaForMetadataAddsNullableTimestampColumns(t *testing.T) {
	sink := &Sink{
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			MetadataColumns: config.IcebergMetadataColumnsConfig{
				CreatedAt: config.IcebergCreatedAtColumnConfig{
					SourceColumns: map[string]string{"app.tbl_reservasi": "waktu_pesan"},
				},
				UpdatedAt:   config.IcebergTimestampColumnConfig{Enabled: true},
				ETLLoadedAt: config.IcebergTimestampColumnConfig{Enabled: true},
			},
		}),
	}

	schema := sink.augmentSchemaForMetadata(&model.TableSchema{
		SchemaName: "app",
		TableName:  "tbl_reservasi",
		Columns: []model.TableColumn{
			{Name: "id", DataType: "bigint", IsPK: true},
			{Name: "waktu_pesan", DataType: "datetime"},
		},
	})

	for _, name := range []string{"created_at", "updated_at", "etl_loaded_at"} {
		if !tableSchemaHasColumn(schema, name) {
			t.Fatalf("expected metadata column %s in schema: %#v", name, schema.Columns)
		}
	}
}

func TestMetadataEnrichmentUsesMappingCDCEventTimeAndLoadTime(t *testing.T) {
	sink := &Sink{
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			MetadataColumns: config.IcebergMetadataColumnsConfig{
				CreatedAt: config.IcebergCreatedAtColumnConfig{
					SourceColumns: map[string]string{"app.tbl_reservasi": "waktu_pesan"},
				},
				UpdatedAt:   config.IcebergTimestampColumnConfig{Enabled: true},
				ETLLoadedAt: config.IcebergTimestampColumnConfig{Enabled: true},
			},
		}),
	}
	state := &tableState{
		sourceKey: "app.tbl_reservasi",
		sourceSchema: &model.TableSchema{
			SchemaName: "app",
			TableName:  "tbl_reservasi",
		},
	}
	eventTime := time.Date(2026, 5, 5, 10, 30, 0, 0, time.UTC)
	waktuPesan := time.Date(2026, 5, 4, 8, 15, 0, 0, time.UTC)
	row := map[string]interface{}{"id": int64(1), "waktu_pesan": waktuPesan}
	pending := pendingRow{
		row: row,
		event: model.Event{
			Origin:    model.EventOriginCDC,
			Timestamp: eventTime,
		},
	}

	sink.applyCreatedAt(row, nil, state, pending)
	sink.applyUpdatedAt(row, nil, pending)
	row[sink.cfg.MetadataColumns.ETLLoadedAt.Name] = time.Now().UTC()

	if got := row["created_at"]; got != waktuPesan {
		t.Fatalf("created_at = %v, want %v", got, waktuPesan)
	}
	if got := row["updated_at"]; got != eventTime {
		t.Fatalf("updated_at = %v, want %v", got, eventTime)
	}
	if _, ok := row["etl_loaded_at"].(time.Time); !ok {
		t.Fatalf("etl_loaded_at = %T, want time.Time", row["etl_loaded_at"])
	}
}

func TestMetadataEnrichmentPreservesUpdatedAtForSnapshotRows(t *testing.T) {
	sink := &Sink{
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			MetadataColumns: config.IcebergMetadataColumnsConfig{
				UpdatedAt: config.IcebergTimestampColumnConfig{Enabled: true},
			},
		}),
	}
	oldUpdatedAt := time.Date(2026, 5, 5, 11, 0, 0, 0, time.UTC)
	row := map[string]interface{}{"id": int64(1)}
	pending := pendingRow{
		row: row,
		event: model.Event{
			Origin:    model.EventOriginSnapshot,
			Timestamp: time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC),
		},
	}

	sink.applyUpdatedAt(row, map[string]interface{}{"updated_at": oldUpdatedAt}, pending)

	if got := row["updated_at"]; got != oldUpdatedAt {
		t.Fatalf("updated_at = %v, want preserved %v", got, oldUpdatedAt)
	}
}

func TestParseDDLPlanChangeColumn(t *testing.T) {
	actions, skip, err := parseDDLPlan("ALTER TABLE `orders` CHANGE COLUMN `old_status` `status` varchar(64) not null")
	if err != nil {
		t.Fatalf("parseDDLPlan returned error: %v", err)
	}
	if skip {
		t.Fatalf("parseDDLPlan unexpectedly skipped ddl")
	}
	if got, want := len(actions), 2; got != want {
		t.Fatalf("len(actions) = %d, want %d", got, want)
	}
	if actions[0].Kind != ddlActionRenameColumn {
		t.Fatalf("first action kind = %s, want %s", actions[0].Kind, ddlActionRenameColumn)
	}
	if actions[1].Kind != ddlActionUpdateColumn {
		t.Fatalf("second action kind = %s, want %s", actions[1].Kind, ddlActionUpdateColumn)
	}
	if got, want := actions[1].Column.Name, "status"; got != want {
		t.Fatalf("updated column name = %s, want %s", got, want)
	}
	if got, want := actions[1].Column.DataType, "varchar"; got != want {
		t.Fatalf("updated column datatype = %s, want %s", got, want)
	}
	if actions[1].Column.IsNullable {
		t.Fatalf("updated column should be not null")
	}
}

func TestParseDDLPlanSkipsTableRename(t *testing.T) {
	tests := []string{
		"ALTER TABLE `tbl_log_api` RENAME `tbl_log_api_backup_20260604`",
		"ALTER TABLE `tbl_log_api` RENAME TO `tbl_log_api_backup_20260604`",
		"RENAME TABLE `tbl_log_api` TO `tbl_log_api_backup_20260604`",
	}

	for _, ddl := range tests {
		actions, skip, err := parseDDLPlan(ddl)
		if err != nil {
			t.Fatalf("parseDDLPlan(%q) returned error: %v", ddl, err)
		}
		if !skip {
			t.Fatalf("parseDDLPlan(%q) skip = false, want true", ddl)
		}
		if len(actions) != 0 {
			t.Fatalf("parseDDLPlan(%q) actions = %d, want 0", ddl, len(actions))
		}
	}
}

func TestEvictIdleDropsInactiveTableHandle(t *testing.T) {
	now := time.Now()
	sink := &Sink{
		jobID: "job-1",
		cfg: config.IcebergConfig{
			IdleTableEvictSeconds: 60,
		},
		states: map[string]*tableState{
			"idle.table": {
				sourceKey:     "idle.table",
				table:         &table.Table{},
				lastTouchedAt: now.Add(-2 * time.Minute),
			},
			"busy.table": {
				sourceKey:     "busy.table",
				table:         &table.Table{},
				lastTouchedAt: now.Add(-2 * time.Minute),
				pending:       []model.Event{{Type: model.EventTypeInsert}},
			},
		},
	}

	sink.evictIdle(now)

	if sink.states["idle.table"].table != nil {
		t.Fatalf("idle table handle should be evicted")
	}
	if sink.states["busy.table"].table == nil {
		t.Fatalf("busy table handle should not be evicted")
	}
}

func TestCommitPendingOffsetUsesCheckpointKey(t *testing.T) {
	store := &testOffsetStore{}
	sink := &Sink{
		jobID:         "visible-job",
		stateKey:      "rivus/v1/checkpoint-key",
		offsetSto:     store,
		pendingOffset: &model.SourceOffset{BinlogFile: "mysql-bin.000010", BinlogPos: 42},
		states:        make(map[string]*tableState),
	}

	if err := sink.commitPendingOffset(context.Background()); err != nil {
		t.Fatalf("commitPendingOffset returned error: %v", err)
	}
	if store.savedJobID != "rivus/v1/checkpoint-key" {
		t.Fatalf("saved offset key = %q, want internal checkpoint key", store.savedJobID)
	}
	if store.offset == nil || store.offset.BinlogFile != "mysql-bin.000010" || store.offset.BinlogPos != 42 {
		t.Fatalf("saved offset = %#v, want pending source position", store.offset)
	}
}

type testOffsetStore struct {
	savedJobID string
	offset     *meta.Offset
}

func (s *testOffsetStore) GetOffset(context.Context, string) (*meta.Offset, error) {
	return s.offset, nil
}

func (s *testOffsetStore) SaveOffset(_ context.Context, jobID string, offset meta.Offset) error {
	s.savedJobID = jobID
	s.offset = &offset
	return nil
}

func (s *testOffsetStore) GetSnapshotState(context.Context, string) (*meta.SnapshotState, error) {
	return nil, nil
}

func (s *testOffsetStore) SaveSnapshotStart(context.Context, string, meta.Offset) error {
	return nil
}

func (s *testOffsetStore) MarkSnapshotDone(context.Context, string) error {
	return nil
}

func (s *testOffsetStore) GetSnapshotProgress(context.Context, string) (*meta.SnapshotProgress, error) {
	return nil, nil
}

func (s *testOffsetStore) SaveSnapshotProgress(context.Context, string, string, int64, string) error {
	return nil
}

func (s *testOffsetStore) ClearSnapshotProgress(context.Context, string) error {
	return nil
}

func (s *testOffsetStore) DeleteJobState(context.Context, string) error {
	return nil
}
