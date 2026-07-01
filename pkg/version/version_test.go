package version

import "testing"

func TestCurrentUsesRuntimeEnvOverrides(t *testing.T) {
	t.Setenv("RIVUS_VERSION", "v1.2.3")
	t.Setenv("RIVUS_COMMIT", "abc1234")
	t.Setenv("RIVUS_BUILD_DATE", "2026-06-05T00:00:00Z")

	info := Current()
	if info.Version != "v1.2.3" {
		t.Fatalf("Version = %q, want v1.2.3", info.Version)
	}
	if info.Commit != "abc1234" {
		t.Fatalf("Commit = %q, want abc1234", info.Commit)
	}
	if info.BuildDate != "2026-06-05T00:00:00Z" {
		t.Fatalf("BuildDate = %q, want 2026-06-05T00:00:00Z", info.BuildDate)
	}
}
