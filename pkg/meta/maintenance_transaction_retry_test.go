package meta

import (
	"context"
	"errors"
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"
)

func TestRetryMaintenanceTransactionRetriesDeadlock(t *testing.T) {
	attempts := 0
	err := retryMaintenanceTransaction(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return &drivermysql.MySQLError{Number: 1213, Message: "deadlock"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryMaintenanceTransactionDoesNotRetryPermanentError(t *testing.T) {
	attempts := 0
	want := errors.New("invalid ownership selector")
	err := retryMaintenanceTransaction(context.Background(), func() error {
		attempts++
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRetryMaintenanceTransactionStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := retryMaintenanceTransaction(ctx, func() error {
		attempts++
		cancel()
		return &drivermysql.MySQLError{Number: 1205, Message: "lock wait timeout"}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}
