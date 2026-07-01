package doris

import (
	"bytes"
	"net/url"
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
)

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
	want := "2022-01-01\n"
	if payload.String() != want {
		t.Fatalf("payload = %q, want %q", payload.String(), want)
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

func TestMapMySQLTypeStrToDorisUsesVarcharForOversizedDecimal(t *testing.T) {
	got := mapMySQLTypeStrToDoris("decimal(65,30)")
	want := "VARCHAR(67)"
	if got != want {
		t.Fatalf("mapMySQLTypeStrToDoris() = %q, want %q", got, want)
	}
}

func TestCheckpointKeyUsesInternalStateKey(t *testing.T) {
	sink := &Sink{jobID: "visible-job", stateKey: "rivus/v1/checkpoint-key"}
	if got := sink.checkpointKey(); got != "rivus/v1/checkpoint-key" {
		t.Fatalf("checkpointKey() = %q, want internal state key", got)
	}
}
