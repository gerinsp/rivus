package main

import (
	"strings"
	"testing"
	"time"
)

func TestGracefulShutdownTimeoutFromEnv(t *testing.T) {
	t.Setenv("RIVUS_SHUTDOWN_TIMEOUT_SECONDS", "45")
	if got, want := gracefulShutdownTimeoutFromEnv(), 45*time.Second; got != want {
		t.Fatalf("shutdown timeout = %s, want %s", got, want)
	}
}

func TestRuntimeInstanceIDPrefersExplicitInstanceID(t *testing.T) {
	t.Setenv("RIVUS_INSTANCE_ID", "rivus-streaming-1")
	if got, want := runtimeInstanceID("streaming", "worker-id"), "rivus-streaming-1"; got != want {
		t.Fatalf("runtime instance id = %q, want %q", got, want)
	}
}

func TestRuntimeInstanceIDIsBoundedForMetadataStore(t *testing.T) {
	t.Setenv("RIVUS_INSTANCE_ID", strings.Repeat("x", 300))
	if got := runtimeInstanceID("streaming", ""); len(got) != 255 {
		t.Fatalf("runtime instance id length = %d, want 255", len(got))
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
