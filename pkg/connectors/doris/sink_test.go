package doris

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type recordingOffsetStore struct {
	savedJobIDs []string
	saved       []meta.Offset
}

func (s *recordingOffsetStore) GetOffset(context.Context, string) (*meta.Offset, error) {
	return nil, nil
}

func (s *recordingOffsetStore) SaveOffset(_ context.Context, jobID string, offset meta.Offset) error {
	s.savedJobIDs = append(s.savedJobIDs, jobID)
	s.saved = append(s.saved, offset)
	return nil
}

func (s *recordingOffsetStore) GetSnapshotState(context.Context, string) (*meta.SnapshotState, error) {
	return nil, nil
}

func (s *recordingOffsetStore) SaveSnapshotStart(context.Context, string, meta.Offset) error {
	return nil
}

func (s *recordingOffsetStore) MarkSnapshotDone(context.Context, string) error {
	return nil
}

func (s *recordingOffsetStore) GetSnapshotProgress(context.Context, string) (*meta.SnapshotProgress, error) {
	return nil, nil
}

func (s *recordingOffsetStore) SaveSnapshotProgress(context.Context, string, string, int64, string) error {
	return nil
}

func (s *recordingOffsetStore) ClearSnapshotProgress(context.Context, string) error {
	return nil
}

func (s *recordingOffsetStore) DeleteJobState(context.Context, string) error {
	return nil
}

func TestBuildColumnsHeaderQuotesReservedKeywords(t *testing.T) {
	got := buildColumnsHeader([]string{"Pri", "IsCharter", "ShowManifest", "Group"})
	want := "`Pri`,`IsCharter`,`ShowManifest`,`Group`"
	if got != want {
		t.Fatalf("buildColumnsHeader() = %q, want %q", got, want)
	}
}

func TestQuoteDorisIdentifierEscapesBackticks(t *testing.T) {
	got := quoteDorisIdentifier("odd`name")
	want := "`odd``name`"
	if got != want {
		t.Fatalf("quoteDorisIdentifier() = %q, want %q", got, want)
	}
}

func TestSanitizeDorisColumnNameKeepsRegularNames(t *testing.T) {
	got := sanitizeDorisColumnName("TglBerangkat", 0, map[string]int{})
	want := "TglBerangkat"
	if got != want {
		t.Fatalf("sanitizeDorisColumnName() = %q, want %q", got, want)
	}
}

func TestSanitizeDorisColumnNameRewritesExpressionColumns(t *testing.T) {
	got := sanitizeDorisColumnName("DATE(tsp.TglBerangkat)", 0, map[string]int{})
	want := "DATE_tsp_TglBerangkat"
	if got != want {
		t.Fatalf("sanitizeDorisColumnName() = %q, want %q", got, want)
	}
}

func TestWriteBatchPayloadUsesSourceBindings(t *testing.T) {
	var payload bytes.Buffer
	err := (&Sink{}).writeBatchPayload(
		&payload,
		[]columnBinding{{Source: "DATE(tsp.TglBerangkat)", Target: "DATE_tsp_TglBerangkat"}},
		[]bool{false},
		[]int{0},
		[]model.Event{{
			Data: map[string]interface{}{
				"DATE(tsp.TglBerangkat)": "2022-01-01",
			},
		}},
	)
	if err != nil {
		t.Fatalf("writeBatchPayload() returned error: %v", err)
	}
	want := "2022-01-01" + fieldSep + "0\n"
	if payload.String() != want {
		t.Fatalf("payload = %q, want %q", payload.String(), want)
	}
}

func TestWriteBatchPayloadAddsDeleteMarker(t *testing.T) {
	var payload bytes.Buffer
	err := (&Sink{}).writeBatchPayload(
		&payload,
		[]columnBinding{{Source: "id", Target: "id"}},
		[]bool{false},
		[]int{0},
		[]model.Event{
			{Type: model.EventTypeInsert, Data: map[string]interface{}{"id": 1}},
			{Type: model.EventTypeUpdate, Data: map[string]interface{}{"id": 2}},
			{Type: model.EventTypeDelete, Data: map[string]interface{}{"id": 3}},
		},
	)
	if err != nil {
		t.Fatalf("writeBatchPayload() returned error: %v", err)
	}
	want := strings.Join([]string{
		"1" + fieldSep + "0",
		"2" + fieldSep + "0",
		"3" + fieldSep + "1",
		"",
	}, "\n")
	if payload.String() != want {
		t.Fatalf("payload = %q, want %q", payload.String(), want)
	}
}

func TestWriteBatchPayloadNormalizesMySQLPartialDates(t *testing.T) {
	var payload bytes.Buffer
	err := (&Sink{}).writeBatchPayload(
		&payload,
		[]columnBinding{
			{Source: "nullable_date", Target: "nullable_date", TargetType: "DATE", Nullable: true},
			{Source: "required_date", Target: "required_date", TargetType: "DATE", Nullable: false},
			{Source: "date_text", Target: "date_text", TargetType: "VARCHAR(32)", Nullable: true},
		},
		[]bool{false, false, true},
		[]int{0, 0, 32},
		[]model.Event{{Data: map[string]interface{}{
			"nullable_date": "2014-05-00",
			"required_date": "2014-05-00",
			"date_text":     "2014-05-00",
		}}},
	)
	if err != nil {
		t.Fatalf("writeBatchPayload() returned error: %v", err)
	}
	want := "\\N" + fieldSep + "2014-05-01" + fieldSep + "2014-05-00" + fieldSep + "0\n"
	if payload.String() != want {
		t.Fatalf("payload = %q, want %q", payload.String(), want)
	}
}

func TestSendBatchUsesMergeDeleteSemantics(t *testing.T) {
	var gotHeaders http.Header
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"Status":"Success",
			"Message":"OK",
			"Label":"test-label",
			"NumberTotalRows":2,
			"NumberLoadedRows":2,
			"NumberFilteredRows":0,
			"NumberUnselectedRows":0
		}`)
	}))
	defer server.Close()

	targetKey := "target.orders"
	sink := &Sink{
		jobID:      "job-1",
		httpClient: server.Client(),
		retry:      config.RetryPolicy{MaxAttempts: 1},
		cfg: config.DorisConfig{
			HTTPHost: server.URL,
		},
		columnBindings: map[string][]columnBinding{
			targetKey: {{Source: "id", Target: "id"}},
		},
		isString: map[string]map[string]bool{
			targetKey: {"id": false},
		},
		maxLen: map[string]map[string]int{
			targetKey: {"id": 0},
		},
	}
	err := sink.sendBatch(context.Background(), "target", "orders", []model.Event{
		{Type: model.EventTypeInsert, Data: map[string]interface{}{"id": 1}},
		{Type: model.EventTypeDelete, Data: map[string]interface{}{"id": 2}},
	})
	if err != nil {
		t.Fatalf("sendBatch() returned error: %v", err)
	}
	if got := gotHeaders.Get("merge_type"); got != "MERGE" {
		t.Fatalf("merge_type = %q, want MERGE", got)
	}
	if got := gotHeaders.Get("delete"); got != dorisDeleteMarkerColumn+" = 1" {
		t.Fatalf("delete = %q, want delete marker condition", got)
	}
	if got := gotHeaders.Get("columns"); !strings.Contains(got, dorisDeleteMarkerColumn) {
		t.Fatalf("columns = %q, want synthetic delete marker", got)
	}
	wantBody := "1" + fieldSep + "0\n2" + fieldSep + "1\n"
	if gotBody != wantBody {
		t.Fatalf("body = %q, want %q", gotBody, wantBody)
	}
}

func TestValidateStreamLoadCountsRejectsIncompleteSuccess(t *testing.T) {
	tests := []struct {
		name string
		resp streamLoadResp
	}{
		{
			name: "filtered",
			resp: streamLoadResp{NumberTotalRows: 2, NumberLoadedRows: 1, NumberFilteredRows: 1},
		},
		{
			name: "unselected",
			resp: streamLoadResp{NumberTotalRows: 2, NumberLoadedRows: 1, NumberUnselectedRows: 1},
		},
		{
			name: "missing total row",
			resp: streamLoadResp{NumberTotalRows: 1, NumberLoadedRows: 1},
		},
		{
			name: "missing loaded row",
			resp: streamLoadResp{NumberTotalRows: 2, NumberLoadedRows: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateStreamLoadCounts(tt.resp, 2); err == nil {
				t.Fatal("validateStreamLoadCounts() returned nil")
			}
		})
	}

	if err := validateStreamLoadCounts(streamLoadResp{
		NumberTotalRows:  2,
		NumberLoadedRows: 2,
	}, 2); err != nil {
		t.Fatalf("complete response rejected: %v", err)
	}
}

func TestTranslateMySQLDDLToDorisUsesStructuredParser(t *testing.T) {
	sink := &Sink{}
	stmts, ok, reason := sink.translateMySQLDDLToDoris(
		"ALTER TABLE `orders` ADD COLUMN `note` varchar(64) DEFAULT 'x,ADD COLUMN phantom', ADD COLUMN `TglBerangkat` date",
		"bronze",
		"orders",
	)
	if !ok {
		t.Fatalf("translation failed: %s", reason)
	}
	if got, want := len(stmts), 2; got != want {
		t.Fatalf("statement count = %d, want %d", got, want)
	}
	if got, want := stmts[0], "ALTER TABLE `bronze`.`orders` ADD COLUMN `note` VARCHAR(64) NULL"; got != want {
		t.Fatalf("first statement = %q, want %q", got, want)
	}
	if got, want := stmts[1], "ALTER TABLE `bronze`.`orders` ADD COLUMN `TglBerangkat` DATE NULL"; got != want {
		t.Fatalf("second statement = %q, want %q", got, want)
	}
}

func TestTranslateMySQLDDLToDorisIgnoresIndexChanges(t *testing.T) {
	sink := &Sink{}
	stmts, ok, reason := sink.translateMySQLDDLToDoris(
		"ALTER TABLE `orders` ADD INDEX `idx_departure` (`TglBerangkat`)",
		"bronze",
		"orders",
	)
	if ok {
		t.Fatalf("translation unexpectedly succeeded with statements %#v", stmts)
	}
	if got, want := reason, "no row-schema changes"; got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
}

func TestRewriteStreamLoadRedirectURLWithoutOverride(t *testing.T) {
	rawURL, err := url.Parse("http://192.0.2.10:8040/api/demo/users/_stream_load")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	got := rewriteStreamLoadRedirectURL(rawURL, config.DorisConfig{})
	want := "http://192.0.2.10:8040/api/demo/users/_stream_load"
	if got != want {
		t.Fatalf("rewriteStreamLoadRedirectURL() = %q, want %q", got, want)
	}
}

func TestRewriteStreamLoadRedirectURLUsesFEHostWhenOnlyBEPortConfigured(t *testing.T) {
	rawURL, err := url.Parse("http://192.0.2.10:8040/api/demo/users/_stream_load")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	got := rewriteStreamLoadRedirectURL(rawURL, config.DorisConfig{
		HTTPHost:   "http://198.51.100.20:28030",
		BEHTTPPort: 28040,
	})
	want := "http://198.51.100.20:28040/api/demo/users/_stream_load"
	if got != want {
		t.Fatalf("rewriteStreamLoadRedirectURL() = %q, want %q", got, want)
	}
}

func TestRewriteStreamLoadRedirectURLUsesExplicitBEHostAndPort(t *testing.T) {
	rawURL, err := url.Parse("http://192.0.2.10:8040/api/demo/users/_stream_load")
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	got := rewriteStreamLoadRedirectURL(rawURL, config.DorisConfig{
		HTTPHost:   "http://198.51.100.20:28030",
		BEHTTPHost: "203.0.113.30",
		BEHTTPPort: 28040,
	})
	want := "http://203.0.113.30:28040/api/demo/users/_stream_load"
	if got != want {
		t.Fatalf("rewriteStreamLoadRedirectURL() = %q, want %q", got, want)
	}
}

func TestResolveTargetUsesSchemaWildcardOverride(t *testing.T) {
	sink := &Sink{cfg: config.DorisConfig{
		Overrides: map[string]config.DorisTarget{
			"source_delta.*": {
				Database: "target_delta",
			},
		},
	}}

	gotDB, gotTable := sink.resolveTarget("source_delta", "orders")
	if gotDB != "target_delta" || gotTable != "orders" {
		t.Fatalf("resolveTarget() = %s.%s, want %s.%s", gotDB, gotTable, "target_delta", "orders")
	}
}

func TestResolveTargetPrefersExactOverrideOverSchemaWildcard(t *testing.T) {
	sink := &Sink{cfg: config.DorisConfig{
		Overrides: map[string]config.DorisTarget{
			"source_delta.*": {
				Database: "target_delta",
			},
			"source_delta.orders": {
				Database: "special_operator_delta",
				Table:    "orders_v2",
			},
		},
	}}

	gotDB, gotTable := sink.resolveTarget("source_delta", "orders")
	if gotDB != "special_operator_delta" || gotTable != "orders_v2" {
		t.Fatalf("resolveTarget() = %s.%s, want %s.%s", gotDB, gotTable, "special_operator_delta", "orders_v2")
	}
}

func TestMapMySQLColumnToDorisUsesVarcharForKeyText(t *testing.T) {
	charMax := int64(65535)
	got := mapMySQLColumnToDoris(model.TableColumn{
		Name:       "NoTiket",
		DataType:   "longtext",
		CharMaxLen: &charMax,
	}, true)
	want := "VARCHAR(65533)"
	if got != want {
		t.Fatalf("mapMySQLColumnToDoris() = %q, want %q", got, want)
	}
}

func TestMapMySQLColumnToDorisKeepsStringForNonKeyText(t *testing.T) {
	got := mapMySQLColumnToDoris(model.TableColumn{
		Name:     "payload",
		DataType: "longtext",
	}, false)
	want := "STRING"
	if got != want {
		t.Fatalf("mapMySQLColumnToDoris() = %q, want %q", got, want)
	}
}

func TestMapMySQLColumnToDorisCapsOversizedKeyVarchar(t *testing.T) {
	charMax := int64(100000)
	got := mapMySQLColumnToDoris(model.TableColumn{
		Name:       "NoTiket",
		DataType:   "varchar",
		CharMaxLen: &charMax,
	}, true)
	want := "VARCHAR(65533)"
	if got != want {
		t.Fatalf("mapMySQLColumnToDoris() = %q, want %q", got, want)
	}
}

func TestMapMySQLColumnToDorisUsesDecimalForKeyDouble(t *testing.T) {
	got := mapMySQLColumnToDoris(model.TableColumn{
		Name:     "jarak",
		DataType: "double",
	}, true)
	want := "DECIMAL(27,9)"
	if got != want {
		t.Fatalf("mapMySQLColumnToDoris() = %q, want %q", got, want)
	}
}

func TestMapMySQLColumnToDorisKeepsDoubleForNonKeyDouble(t *testing.T) {
	got := mapMySQLColumnToDoris(model.TableColumn{
		Name:     "jarak",
		DataType: "double",
	}, false)
	want := "DOUBLE"
	if got != want {
		t.Fatalf("mapMySQLColumnToDoris() = %q, want %q", got, want)
	}
}

func TestMapMySQLColumnToDorisFollowsFlinkBooleanAndYearMapping(t *testing.T) {
	tests := []struct {
		name string
		col  model.TableColumn
		want string
	}{
		{
			name: "tinyint one",
			col:  model.TableColumn{DataType: "tinyint", ColumnType: "tinyint(1)"},
			want: "BOOLEAN",
		},
		{
			name: "unsigned tinyint one",
			col:  model.TableColumn{DataType: "tinyint", ColumnType: "tinyint(1) unsigned"},
			want: "BIGINT",
		},
		{
			name: "bit one",
			col:  model.TableColumn{DataType: "bit", ColumnType: "bit(1)"},
			want: "BOOLEAN",
		},
		{
			name: "year",
			col:  model.TableColumn{DataType: "year", ColumnType: "year"},
			want: "BIGINT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapMySQLColumnToDoris(tt.col, false); got != tt.want {
				t.Fatalf("type = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMapMySQLColumnToDorisUsesVarcharForOversizedDecimal(t *testing.T) {
	numPrec := int64(65)
	numScale := int64(30)
	got := mapMySQLColumnToDoris(model.TableColumn{
		Name:     "amount",
		DataType: "decimal",
		NumPrec:  &numPrec,
		NumScale: &numScale,
	}, false)
	want := "VARCHAR(67)"
	if got != want {
		t.Fatalf("mapMySQLColumnToDoris() = %q, want %q", got, want)
	}
}

func TestCheckpointKeyUsesInternalStateKey(t *testing.T) {
	sink := &Sink{jobID: "visible-job", stateKey: "rivus/v1/checkpoint-key"}
	if got := sink.checkpointKey(); got != "rivus/v1/checkpoint-key" {
		t.Fatalf("checkpointKey() = %q, want internal state key", got)
	}
}

func TestCheckpointWaitsForNormalFlushBoundary(t *testing.T) {
	store := &recordingOffsetStore{}
	var loaded [][]model.Event
	sink := &Sink{
		jobID:     "job-1",
		stateKey:  "rivus/v1/job-1",
		offsetSto: store,
		cfg: config.DorisConfig{
			BatchSize: 500,
		},
		sendBatchOverride: func(_ context.Context, db, table string, batch []model.Event) error {
			if db != "source_db" || table != "orders" {
				t.Fatalf("target = %s.%s, want source_db.orders", db, table)
			}
			loaded = append(loaded, append([]model.Event(nil), batch...))
			return nil
		},
	}
	state := newDorisRunState(sink)

	row := model.Event{
		Type:   model.EventTypeInsert,
		Schema: "source_db",
		Table:  "orders",
		Data:   map[string]interface{}{"id": 1},
	}
	checkpoint := model.Event{
		Type:    model.EventTypeCheckpoint,
		TraceID: "mysql-bin.000184:456:checkpoint",
		SourceOffset: &model.SourceOffset{
			BinlogFile: "mysql-bin.000184",
			BinlogPos:  456,
		},
	}

	if err := state.handleEvent(context.Background(), row); err != nil {
		t.Fatalf("handle row: %v", err)
	}
	if err := state.handleEvent(context.Background(), checkpoint); err != nil {
		t.Fatalf("handle checkpoint: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("checkpoint triggered %d Stream Loads, want none", len(loaded))
	}
	if len(store.saved) != 0 {
		t.Fatalf("checkpoint triggered %d offset saves, want none", len(store.saved))
	}

	if err := state.flushAll(context.Background()); err != nil {
		t.Fatalf("flushAll: %v", err)
	}
	if len(loaded) != 1 || len(loaded[0]) != 1 {
		t.Fatalf("loaded batches = %#v, want one batch containing one row", loaded)
	}
	if len(store.saved) != 1 {
		t.Fatalf("saved offsets = %d, want 1", len(store.saved))
	}
	if store.savedJobIDs[0] != "rivus/v1/job-1" {
		t.Fatalf("saved job id = %q, want internal state key", store.savedJobIDs[0])
	}
	if got := store.saved[0]; got.BinlogFile != "mysql-bin.000184" || got.BinlogPos != 456 {
		t.Fatalf("saved offset = %#v, want mysql-bin.000184:456", got)
	}
}

func TestCheckpointsCoalesceToLatestOffset(t *testing.T) {
	store := &recordingOffsetStore{}
	sink := &Sink{
		jobID:     "job-1",
		offsetSto: store,
		cfg: config.DorisConfig{
			BatchSize: 500,
		},
	}
	state := newDorisRunState(sink)

	for _, pos := range []uint32{100, 200, 300} {
		err := state.handleEvent(context.Background(), model.Event{
			Type: model.EventTypeCheckpoint,
			SourceOffset: &model.SourceOffset{
				BinlogFile: "mysql-bin.000184",
				BinlogPos:  pos,
			},
		})
		if err != nil {
			t.Fatalf("handle checkpoint %d: %v", pos, err)
		}
	}
	if len(store.saved) != 0 {
		t.Fatalf("saved offsets before flush = %d, want 0", len(store.saved))
	}

	if err := state.flushAll(context.Background()); err != nil {
		t.Fatalf("flushAll: %v", err)
	}
	if len(store.saved) != 1 {
		t.Fatalf("saved offsets = %d, want 1", len(store.saved))
	}
	if got := store.saved[0].BinlogPos; got != 300 {
		t.Fatalf("saved position = %d, want latest position 300", got)
	}
}

func TestFailedStreamLoadDoesNotCommitCheckpoint(t *testing.T) {
	store := &recordingOffsetStore{}
	loadErr := errors.New("stream load failed")
	sink := &Sink{
		jobID:     "job-1",
		offsetSto: store,
		cfg: config.DorisConfig{
			BatchSize: 500,
		},
		sendBatchOverride: func(context.Context, string, string, []model.Event) error {
			return loadErr
		},
	}
	state := newDorisRunState(sink)

	if err := state.handleEvent(context.Background(), model.Event{
		Type:   model.EventTypeInsert,
		Schema: "source_db",
		Table:  "orders",
	}); err != nil {
		t.Fatalf("handle row: %v", err)
	}
	if err := state.handleEvent(context.Background(), model.Event{
		Type: model.EventTypeCheckpoint,
		SourceOffset: &model.SourceOffset{
			BinlogFile: "mysql-bin.000184",
			BinlogPos:  456,
		},
	}); err != nil {
		t.Fatalf("handle checkpoint: %v", err)
	}

	err := state.flushAll(context.Background())
	if !errors.Is(err, loadErr) {
		t.Fatalf("flushAll error = %v, want %v", err, loadErr)
	}
	if len(store.saved) != 0 {
		t.Fatalf("saved offsets after failed load = %d, want 0", len(store.saved))
	}
	if state.allBatchesEmpty() {
		t.Fatal("failed batch was discarded")
	}
}

func TestSnapshotBatchAcknowledgesOnlyAfterDorisFlush(t *testing.T) {
	var loaded [][]model.Event
	sink := &Sink{
		jobID: "job-1",
		cfg: config.DorisConfig{
			BatchSize: 2,
		},
		sendBatchOverride: func(_ context.Context, db, table string, batch []model.Event) error {
			if db != "source_db" || table != "orders" {
				t.Fatalf("target = %s.%s, want source_db.orders", db, table)
			}
			loaded = append(loaded, append([]model.Event(nil), batch...))
			return nil
		},
	}
	state := newDorisRunState(sink)
	ack := make(chan error, 1)

	err := state.handleEvent(context.Background(), model.Event{
		Type:   model.EventTypeSnapshotBatch,
		Schema: "source_db",
		Table:  "orders",
		Rows: []map[string]interface{}{
			{"id": 1},
			{"id": 2},
			{"id": 3},
		},
		Ack: ack,
	})
	if err != nil {
		t.Fatalf("handle snapshot batch: %v", err)
	}
	if got, want := len(loaded), 2; got != want {
		t.Fatalf("loaded chunks = %d, want %d", got, want)
	}
	for _, batch := range loaded {
		for _, ev := range batch {
			if ev.Type != model.EventTypeInsert || ev.Origin != model.EventOriginSnapshot {
				t.Fatalf("snapshot row event = %#v", ev)
			}
		}
	}
	if ackErr, ok := <-ack; !ok || ackErr != nil {
		t.Fatalf("ack = %v, open=%t, want nil acknowledgement", ackErr, ok)
	}
}

func TestSnapshotBatchReturnsFlushFailureThroughAcknowledgement(t *testing.T) {
	loadErr := errors.New("stream load failed")
	sink := &Sink{
		jobID: "job-1",
		cfg: config.DorisConfig{
			BatchSize: 2,
		},
		sendBatchOverride: func(context.Context, string, string, []model.Event) error {
			return loadErr
		},
	}
	state := newDorisRunState(sink)
	ack := make(chan error, 1)

	err := state.handleEvent(context.Background(), model.Event{
		Type:   model.EventTypeSnapshotBatch,
		Schema: "source_db",
		Table:  "orders",
		Rows:   []map[string]interface{}{{"id": 1}},
		Ack:    ack,
	})
	if !errors.Is(err, loadErr) {
		t.Fatalf("handle snapshot batch error = %v, want %v", err, loadErr)
	}
	if ackErr, ok := <-ack; !ok || !errors.Is(ackErr, loadErr) {
		t.Fatalf("ack = %v, open=%t, want %v", ackErr, ok, loadErr)
	}
}
