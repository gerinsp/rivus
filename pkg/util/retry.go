package util

import (
	"context"
	"errors"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

type permanentError struct {
	err error
}

func (e *permanentError) Error() string {
	return e.err.Error()
}

func (e *permanentError) Unwrap() error {
	return e.err
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	var perr *permanentError
	if errors.As(err, &perr) {
		return err
	}
	return &permanentError{err: err}
}

func IsPermanent(err error) bool {
	var perr *permanentError
	return errors.As(err, &perr)
}

func RetryWithBackoff(ctx context.Context, policy config.RetryPolicy, fn func() error) error {
	attempt := 0
	backoff := policy.BaseBackoff
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	maxBackoff := policy.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 10 * time.Second
	}

	for {
		err := fn()
		if err == nil {
			return nil
		}
		if IsPermanent(err) {
			return err
		}
		attempt++
		if policy.MaxAttempts > 0 && attempt >= policy.MaxAttempts {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}
