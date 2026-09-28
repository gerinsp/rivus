package meta

import (
	"context"
	"errors"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

const maintenanceTransactionMaxAttempts = 5

func retryMaintenanceTransaction(ctx context.Context, operation func() error) error {
	var err error
	for attempt := 0; attempt < maintenanceTransactionMaxAttempts; attempt++ {
		err = operation()
		if err == nil || !isRetryableMaintenanceTransactionError(err) {
			return err
		}
		if attempt == maintenanceTransactionMaxAttempts-1 {
			break
		}
		delay := 25 * time.Millisecond * time.Duration(1<<attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func isRetryableMaintenanceTransactionError(err error) bool {
	var mysqlErr *drivermysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
}
