package main

import (
	"testing"
	"time"
)

func TestGracefulShutdownTimeoutFromEnv(t *testing.T) {
	t.Setenv("RIVUS_SHUTDOWN_TIMEOUT_SECONDS", "45")
	if got, want := gracefulShutdownTimeoutFromEnv(), 45*time.Second; got != want {
		t.Fatalf("shutdown timeout = %s, want %s", got, want)
	}
}

func TestGracefulShutdownTimeoutFromEnvUsesDefault(t *testing.T) {
	for _, value := range []string{"", "invalid", "0", "-1"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("RIVUS_SHUTDOWN_TIMEOUT_SECONDS", value)
			if got := gracefulShutdownTimeoutFromEnv(); got != defaultGracefulShutdownTimeout {
				t.Fatalf("shutdown timeout = %s, want %s", got, defaultGracefulShutdownTimeout)
			}
		})
	}
}
