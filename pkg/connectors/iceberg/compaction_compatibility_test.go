package iceberg

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceberglib "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/gerinsp/rivus/pkg/config"
)

func TestS3CompatibilityEndpointDefaultsAndOverrides(t *testing.T) {
	cases := []struct {
		name, endpoint, env, configured string
		want                            bool
		invalid                         bool
	}{
		{name: "AWS default"},
		{name: "AWS endpoint", endpoint: "https://s3.ap-southeast-1.amazonaws.com"},
		{name: "AWS China", endpoint: "https://s3.cn-north-1.amazonaws.com.cn"},
		{name: "custom", endpoint: "https://nos.example.test", want: true},
		{name: "IP endpoint", endpoint: "http://172.16.1.4:19000", want: true},
		{name: "AWS-looking custom domain", endpoint: "https://s3.amazonaws.com.example.test", want: true},
		{name: "opt out", endpoint: "https://nos.example.test", env: "false"},
		{name: "explicit opt in", env: "true", want: true},
		{name: "config wins", env: "false", configured: "true", want: true},
		{name: "invalid override", env: "maybe", invalid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearCatalogEnv(t)
			t.Setenv("ICEBERG_S3_COMPAT_MODE", tc.env)
			p, err := catalogS3Properties(config.IcebergConfig{S3Endpoint: tc.endpoint, S3CompatMode: tc.configured})
			if tc.invalid {
				if err == nil {
					t.Fatal("expected invalid mode error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := p[iceio.S3CompatMode]; got != map[bool]string{true: "true", false: "false"}[tc.want] {
				t.Fatalf("compat-mode=%q want %v", got, tc.want)
			}
		})
	}
}

// Exercise the actual Iceberg -> Go Cloud -> AWS transfer-manager path. An
// environment-only checksum policy is insufficient for that upload path.
func TestS3CompatibleFileIOWritesPlainPayload(t *testing.T) {
	clearCatalogEnv(t)
	payload := bytes.Repeat([]byte("parquet-upload-payload"), 16384)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") || r.Header.Get("X-Amz-Trailer") != "" || !bytes.Equal(body, payload) {
			t.Errorf("unexpected upload framing: encoding=%q trailer=%q equal=%v", r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Trailer"), bytes.Equal(body, payload))
			w.WriteHeader(400)
			return
		}
		w.Header().Set("ETag", `"test-etag"`)
	}))
	defer server.Close()
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	props, err := catalogS3Properties(config.IcebergConfig{S3Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs, err := iceio.LoadFS(ctx, props, "s3://test-bucket/data")
	if err != nil {
		t.Fatal(err)
	}
	writer, err := fs.(iceio.WriteFileIO).Create("s3://test-bucket/data/compacted.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionPlansWholeFilesAndPreservesRowsAcrossScanSplits(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	tx := tbl.NewTransaction()
	if err = tx.SetProperties(iceberglib.Properties{icetable.ParquetRowGroupLimitKey: "2", icetable.ReadSplitTargetSizeKey: "1"}); err != nil {
		t.Fatal(err)
	}
	tbl, err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]interface{}, 12)
	for i := range rows {
		rows[i] = map[string]interface{}{"id": int64(i + 1), "status": "seed"}
	}
	reader, release, err := buildRecordReader(tbl.Schema(), rows)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err = tbl.Append(ctx, reader, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	// Retain delete semantics while collapsing split tasks.
	tbl, err = tbl.Delete(ctx, iceberglib.EqualTo(iceberglib.Reference("id"), int64(7)), nil)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) < 2 {
		t.Fatalf("fixture did not generate split scan tasks: %d", len(tasks))
	}
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 1
	cfg.DeleteFileThreshold = 1
	plan, err := analyzeWholeFileCompaction(ctx, tbl, cfg)
	if err != nil {
		t.Fatal(err)
	}
	work := buildCompactionWorkload(plan)
	if work.SelectedDataFiles != 1 {
		t.Fatalf("selected %d data files for split ranges of one file", work.SelectedDataFiles)
	}
	tx = tbl.NewTransaction()
	if _, err = tx.RewriteDataFiles(ctx, work.Groups, icetable.RewriteDataFilesOptions{}); err != nil {
		t.Fatal(err)
	}
	tbl, err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, records, err := tbl.Scan(icetable.WithSelectedFields("id")).ToArrowRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rec, readErr := range records {
		if readErr != nil {
			t.Fatal(readErr)
		}
		col := rec.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	if want := []int64{1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("ids=%v want %v", ids, want)
	}
}

func TestNativeParquetLimitFallbackIsSpecific(t *testing.T) {
	for _, message := range []string{"parquet: compressed page size 87180373 exceeds configured limit 67108864", "parquet: uncompressed page size 123 exceeds configured limit 100", "native Parquet reader page-size limit; Spark compaction failed"} {
		if !nativeParquetLimitError(message) {
			t.Fatalf("expected fallback for %q", message)
		}
	}
	for _, message := range []string{"S3: PutObject 400", "parquet: invalid magic bytes", "out of memory", "compressed page size unknown"} {
		if nativeParquetLimitError(message) {
			t.Fatalf("unexpected fallback for %q", message)
		}
	}
}

func TestDirectSparkS3CompatibilityPreservesJavaOptions(t *testing.T) {
	clearCatalogEnv(t)
	cfg := config.IcebergConfig{Warehouse: "raw", S3Endpoint: "https://nos.example.test", TableMaintenance: config.IcebergTableMaintenanceConfig{CatalogName: "raw", SparkProperties: map[string]string{"spark.driver.extraJavaOptions": "-Dexample.setting=kept", "spark.executor.extraJavaOptions": "-Daws.requestChecksumCalculation=WHEN_SUPPORTED"}}}
	p, err := sparkMaintenanceProperties("job", "job", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := p["spark.driver.extraJavaOptions"]; got != "-Dexample.setting=kept -Daws.requestChecksumCalculation=WHEN_REQUIRED" {
		t.Fatalf("driver options=%q", got)
	}
	if got := p["spark.executor.extraJavaOptions"]; got != "-Daws.requestChecksumCalculation=WHEN_SUPPORTED" {
		t.Fatalf("explicit checksum policy overridden: %q", got)
	}
	if got := p["spark.sql.catalog.raw.s3.chunked-encoding-enabled"]; got != "false" {
		t.Fatalf("chunked encoding=%q", got)
	}
}

func TestWholeFileCompactionRejectsConflictingAndFilteredTasks(t *testing.T) {
	builder, err := iceberglib.NewDataFileBuilder(*iceberglib.UnpartitionedSpec, iceberglib.EntryContentData, "file.parquet", iceberglib.ParquetFile, map[int]any{}, map[int]string{}, map[int]int{}, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	task := icetable.FileScanTask{File: builder.Build(), Length: 50}
	other := task
	other.Start = 50
	other.DataSequenceNumber = new(int64)
	if _, err := wholeFileCompactionTasks([]icetable.FileScanTask{task, other}); err == nil {
		t.Fatal("inconsistent scan metadata must fail closed")
	}
	task.Residual = iceberglib.EqualTo(iceberglib.Reference("id"), int64(1))
	if _, err := wholeFileCompactionTasks([]icetable.FileScanTask{task}); err == nil {
		t.Fatal("filtered tasks must not be expanded to whole files")
	}
}
