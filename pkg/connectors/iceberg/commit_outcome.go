package iceberg

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/util"
)

// This error is emitted only by local preparation paths which have not called
// Transaction.Commit. Unlike a catalog transport failure, it proves the data
// operation could not have been accepted by the catalog.
type icebergPreparationError struct{ cause error }

func (e *icebergPreparationError) Error() string {
	return fmt.Sprintf("Iceberg write preparation failed before catalog commit: %v", e.cause)
}
func (e *icebergPreparationError) Unwrap() error { return e.cause }

func icebergPreparationFailed(err error) error {
	if err == nil {
		return nil
	}
	return &icebergPreparationError{cause: err}
}

// classifyCatalogCommitError is called only after Transaction.Commit returns.
// A rejected conflict is safe to retry. The library does not expose a typed
// "request sent" boundary for every other error, so those errors cannot prove
// the catalog rejected the commit and the enclosing sink retry must stop.
func classifyCatalogCommitError(updated *icetable.Table, err error) error {
	if err == nil {
		return nil
	}
	if updated == nil && errors.Is(err, icetable.ErrCommitFailed) {
		return err
	}
	return util.Permanent(fmt.Errorf("Iceberg catalog commit outcome UNKNOWN; automatic retry stopped, inspect catalog state before restarting job: %w", err))
}

// Keep preparation errors separate: no catalog commit was attempted yet.
func commitAppend(ctx context.Context, tbl *icetable.Table, reader array.RecordReader, props iceberglib.Properties) (*icetable.Table, error) {
	txn := tbl.NewTransaction()
	if err := txn.Append(ctx, reader, props); err != nil {
		return nil, icebergPreparationFailed(err)
	}
	updated, err := txn.Commit(ctx)
	return updated, classifyCatalogCommitError(updated, err)
}

func commitDelete(ctx context.Context, tbl *icetable.Table, filter iceberglib.BooleanExpression, props iceberglib.Properties, opts ...icetable.DeleteOption) (*icetable.Table, error) {
	txn := tbl.NewTransaction()
	if err := txn.Delete(ctx, filter, props, opts...); err != nil {
		return nil, icebergPreparationFailed(err)
	}
	updated, err := txn.Commit(ctx)
	return updated, classifyCatalogCommitError(updated, err)
}
