package schemachange

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/model"
)

func TestParseMySQLDDLQuotedCommaAndMultipleColumns(t *testing.T) {
	changes, err := ParseMySQLDDL(
		"ALTER TABLE `orders` ADD COLUMN `note` varchar(64) DEFAULT 'x,ADD COLUMN phantom', ADD COLUMN `TglBerangkat` date",
	)
	if err != nil {
		t.Fatalf("ParseMySQLDDL returned error: %v", err)
	}
	if got, want := len(changes), 2; got != want {
		t.Fatalf("change count = %d, want %d", got, want)
	}
	if got, want := changes[0].Column.Name, "note"; got != want {
		t.Fatalf("first column = %q, want %q", got, want)
	}
	if got, want := changes[1].Column.Name, "TglBerangkat"; got != want {
		t.Fatalf("second column = %q, want %q", got, want)
	}
}

func TestParseMySQLDDLChangeColumnProducesTypedEvents(t *testing.T) {
	changes, err := ParseMySQLDDL(
		"ALTER TABLE `orders` CHANGE COLUMN `old_status` `status` varchar(64) NOT NULL AFTER `id`",
	)
	if err != nil {
		t.Fatalf("ParseMySQLDDL returned error: %v", err)
	}
	if got, want := len(changes), 2; got != want {
		t.Fatalf("change count = %d, want %d", got, want)
	}
	if changes[0].Type != model.SchemaChangeRenameColumn {
		t.Fatalf("first type = %q, want %q", changes[0].Type, model.SchemaChangeRenameColumn)
	}
	if changes[1].Type != model.SchemaChangeAlterColumnType {
		t.Fatalf("second type = %q, want %q", changes[1].Type, model.SchemaChangeAlterColumnType)
	}
	if changes[1].Column.IsNullable {
		t.Fatal("changed column should be required")
	}
	if changes[1].Position != model.ColumnPositionAfter || changes[1].AfterColumn != "id" {
		t.Fatalf("position = %q after %q, want after id", changes[1].Position, changes[1].AfterColumn)
	}
}

func TestParseMySQLDDLRetainsUnsignedAndDecimalMetadata(t *testing.T) {
	changes, err := ParseMySQLDDL(
		"ALTER TABLE `orders` ADD COLUMN `sequence` bigint unsigned, ADD COLUMN `amount` decimal(40,4)",
	)
	if err != nil {
		t.Fatalf("ParseMySQLDDL returned error: %v", err)
	}
	if got, want := changes[0].Column.ColumnType, "bigint(20) unsigned"; got != want {
		t.Fatalf("unsigned type = %q, want %q", got, want)
	}
	if got, want := *changes[1].Column.NumPrec, int64(40); got != want {
		t.Fatalf("decimal precision = %d, want %d", got, want)
	}
	if got, want := *changes[1].Column.NumScale, int64(4); got != want {
		t.Fatalf("decimal scale = %d, want %d", got, want)
	}
}

func TestParseMySQLDDLModifyWithoutPositionDoesNotMoveColumn(t *testing.T) {
	changes, err := ParseMySQLDDL(
		"ALTER TABLE `orders` MODIFY COLUMN `amount` decimal(12,2) NOT NULL",
	)
	if err != nil {
		t.Fatalf("ParseMySQLDDL returned error: %v", err)
	}
	if got, want := len(changes), 1; got != want {
		t.Fatalf("change count = %d, want %d", got, want)
	}
	if changes[0].Position != "" {
		t.Fatalf("position = %q, want no position change", changes[0].Position)
	}
}

func TestParseMySQLDDLIgnoresNonRowSchemaActions(t *testing.T) {
	changes, err := ParseMySQLDDL(
		"ALTER TABLE `orders` ADD INDEX `idx_status` (`status`), ALGORITHM=INPLACE",
	)
	if err != nil {
		t.Fatalf("ParseMySQLDDL returned error: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("changes = %#v, want none", changes)
	}
}
