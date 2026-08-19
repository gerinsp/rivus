package iceberg

import (
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestMaintenanceConcurrencyDefaults(t *testing.T) {
	t.Setenv("RIVUS_MAINTENANCE_COMPACT_CONCURRENCY", "")
	t.Setenv("RIVUS_MAINTENANCE_EXPIRE_CONCURRENCY", "")
	t.Setenv("RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY", "")

	got := maintenanceConcurrencyFromEnv()
	if got.Compact != 1 || got.ExpireSnapshots != 4 || got.OrphanCleanup != 1 {
		t.Fatalf("unexpected concurrency defaults: %#v", got)
	}
}

func TestMaintenanceConcurrencyOverridesAndCaps(t *testing.T) {
	t.Setenv("RIVUS_MAINTENANCE_COMPACT_CONCURRENCY", "2")
	t.Setenv("RIVUS_MAINTENANCE_EXPIRE_CONCURRENCY", "8")
	t.Setenv("RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY", "99")

	got := maintenanceConcurrencyFromEnv()
	if got.Compact != 2 || got.ExpireSnapshots != 8 || got.OrphanCleanup != 32 {
		t.Fatalf("unexpected concurrency overrides: %#v", got)
	}
}

func TestMaintenanceQueueBacklogRequiresCountAndAge(t *testing.T) {
	settings := maintenanceQueueAlertSettings{
		PendingThreshold: 100,
		OldestAge:        30 * time.Minute,
	}
	cases := []struct {
		name    string
		summary meta.IcebergMaintenanceSummary
		want    bool
	}{
		{
			name:    "healthy",
			summary: meta.IcebergMaintenanceSummary{QueuedTasks: 20, OldestQueuedAgeSec: 60},
			want:    false,
		},
		{
			name:    "large but fresh burst",
			summary: meta.IcebergMaintenanceSummary{QueuedTasks: 200, OldestQueuedAgeSec: 60},
			want:    false,
		},
		{
			name:    "old but small queue",
			summary: meta.IcebergMaintenanceSummary{QueuedTasks: 20, OldestQueuedAgeSec: 7200},
			want:    false,
		},
		{
			name:    "persistent backlog",
			summary: meta.IcebergMaintenanceSummary{QueuedTasks: 90, RetryTasks: 10, OldestQueuedAgeSec: 1800},
			want:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maintenanceQueueBacklogged(tc.summary, settings); got != tc.want {
				t.Fatalf("backlogged=%t, want %t", got, tc.want)
			}
		})
	}
}

func TestMaintenanceQueueAlertReusesTelegramEnvironment(t *testing.T) {
	t.Setenv("RIVUS_TELEGRAM_ENABLED", "true")
	t.Setenv("TELEGRAM_BOT_TOKEN", "bot-token")
	t.Setenv("TELEGRAM_CHAT_ID", "chat-id")
	t.Setenv("RIVUS_TELEGRAM_NOTIFY_MAINTENANCE_QUEUE", "")

	settings := maintenanceQueueAlertSettingsFromEnv()
	if !settings.Enabled {
		t.Fatal("maintenance queue alert should inherit enabled Telegram configuration")
	}
	if settings.BotToken != "bot-token" || settings.ChatID != "chat-id" {
		t.Fatalf("unexpected Telegram settings: %#v", settings)
	}
	if settings.PendingThreshold != 100 || settings.OldestAge != 30*time.Minute {
		t.Fatalf("unexpected queue alert defaults: %#v", settings)
	}
}

func TestMaintenanceQueueAlertCanBeDisabledIndependently(t *testing.T) {
	t.Setenv("RIVUS_TELEGRAM_ENABLED", "true")
	t.Setenv("TELEGRAM_BOT_TOKEN", "bot-token")
	t.Setenv("TELEGRAM_CHAT_ID", "chat-id")
	t.Setenv("RIVUS_TELEGRAM_NOTIFY_MAINTENANCE_QUEUE", "false")

	if settings := maintenanceQueueAlertSettingsFromEnv(); settings.Enabled {
		t.Fatal("maintenance queue alert should honor explicit disable")
	}
}
