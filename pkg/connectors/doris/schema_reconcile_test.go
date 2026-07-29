package doris

import (
	"strings"
	"testing"

	"github.com/gerinsp/rivus/pkg/model"
)

func TestDesiredDorisColumnsMapsTextToString(t *testing.T) {
	schema := &model.TableSchema{
		Columns: []model.TableColumn{
			{Name: "IdConnecting", DataType: "int", IsPK: true},
			{Name: "ListJadwal", DataType: "text", ColumnType: "text", IsNullable: true},
		},
	}

	got := desiredDorisColumns(schema)
	if len(got) != 2 {
		t.Fatalf("desired columns = %d, want 2", len(got))
	}
	if got[1].TargetName != "ListJadwal" || got[1].Type != "STRING" {
		t.Fatalf("ListJadwal mapping = %#v, want STRING", got[1])
	}
}

func TestPlanDorisSchemaChangesWidensVarcharToString(t *testing.T) {
	current := []dorisTargetColumn{
		{Name: "IdConnecting", Type: "BIGINT", IsKey: true},
		{Name: "ListJadwal", Type: "VARCHAR(100)", IsNullable: true},
	}
	desired := []dorisDesiredColumn{
		{TargetName: "IdConnecting", Type: "BIGINT", IsKey: true},
		{TargetName: "ListJadwal", Type: "STRING", IsNullable: true},
	}

	changes, err := planDorisSchemaChanges(current, desired)
	if err != nil {
		t.Fatalf("planDorisSchemaChanges returned error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %#v, want one type widening", changes)
	}
	change := changes[0]
	if change.Kind != dorisSchemaModifyColumn || change.Desired.TargetName != "ListJadwal" {
		t.Fatalf("change = %#v, want ListJadwal modify", change)
	}
	sql := dorisSchemaChangeSQL("asmat_daytrans", "tbl_md_rute_connecting", change)
	want := "ALTER TABLE `asmat_daytrans`.`tbl_md_rute_connecting` MODIFY COLUMN `ListJadwal` STRING NULL"
	if sql != want {
		t.Fatalf("schema change SQL = %q, want %q", sql, want)
	}
}

func TestPlanDorisSchemaChangesAddsMissingValueColumnAsNullable(t *testing.T) {
	current := []dorisTargetColumn{
		{Name: "id", Type: "BIGINT", IsKey: true},
	}
	desired := []dorisDesiredColumn{
		{TargetName: "id", Type: "BIGINT", IsKey: true},
		{TargetName: "status", Type: "VARCHAR(32)"},
	}

	changes, err := planDorisSchemaChanges(current, desired)
	if err != nil {
		t.Fatalf("planDorisSchemaChanges returned error: %v", err)
	}
	if len(changes) != 1 || changes[0].Kind != dorisSchemaAddColumn {
		t.Fatalf("changes = %#v, want one add column", changes)
	}
	sql := dorisSchemaChangeSQL("analytics", "orders", changes[0])
	want := "ALTER TABLE `analytics`.`orders` ADD COLUMN `status` VARCHAR(32) NULL"
	if sql != want {
		t.Fatalf("schema change SQL = %q, want %q", sql, want)
	}
}

func TestPlanDorisSchemaChangesKeepsWiderTargetType(t *testing.T) {
	current := []dorisTargetColumn{
		{Name: "id", Type: "BIGINT", IsKey: true},
		{Name: "status", Type: "STRING", IsNullable: true},
	}
	desired := []dorisDesiredColumn{
		{TargetName: "id", Type: "BIGINT", IsKey: true},
		{TargetName: "status", Type: "VARCHAR(32)", IsNullable: true},
	}

	changes, err := planDorisSchemaChanges(current, desired)
	if err != nil {
		t.Fatalf("planDorisSchemaChanges returned error: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("changes = %#v, want wider target type to be preserved", changes)
	}
}

func TestPlanDorisSchemaChangesRelaxesNullability(t *testing.T) {
	current := []dorisTargetColumn{
		{Name: "id", Type: "BIGINT", IsKey: true},
		{Name: "note", Type: "STRING", IsNullable: false},
	}
	desired := []dorisDesiredColumn{
		{TargetName: "id", Type: "BIGINT", IsKey: true},
		{TargetName: "note", Type: "STRING", IsNullable: true},
	}

	changes, err := planDorisSchemaChanges(current, desired)
	if err != nil {
		t.Fatalf("planDorisSchemaChanges returned error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %#v, want nullability relaxation", changes)
	}
	if sql := dorisSchemaChangeSQL("analytics", "orders", changes[0]); !strings.HasSuffix(sql, "STRING NULL") {
		t.Fatalf("schema change SQL = %q, want nullable STRING", sql)
	}
}

func TestPlanDorisSchemaChangesRejectsMissingKeyColumn(t *testing.T) {
	_, err := planDorisSchemaChanges(
		nil,
		[]dorisDesiredColumn{{TargetName: "id", Type: "BIGINT", IsKey: true}},
	)
	if err == nil || !strings.Contains(err.Error(), "missing source key column") {
		t.Fatalf("error = %v, want missing key column error", err)
	}
}

func TestPlanDorisSchemaChangesRejectsKeyMismatch(t *testing.T) {
	_, err := planDorisSchemaChanges(
		[]dorisTargetColumn{{Name: "id", Type: "BIGINT", IsKey: false}},
		[]dorisDesiredColumn{{TargetName: "id", Type: "BIGINT", IsKey: true}},
	)
	if err == nil || !strings.Contains(err.Error(), "key mismatch") {
		t.Fatalf("error = %v, want key mismatch error", err)
	}
}
