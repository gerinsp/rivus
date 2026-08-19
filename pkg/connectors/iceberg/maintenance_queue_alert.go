package iceberg

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	defaultMaintenanceQueueAlertThreshold = 100
	defaultMaintenanceQueueAlertAge       = 30 * time.Minute
	defaultMaintenanceQueueAlertCheck     = time.Minute
	defaultMaintenanceQueueAlertCooldown  = time.Hour
)

type maintenanceQueueAlertSettings struct {
	Enabled          bool
	BotToken         string
	ChatID           string
	PendingThreshold int
	OldestAge        time.Duration
	CheckInterval    time.Duration
	Cooldown         time.Duration
}

type maintenanceQueueAlertManager struct {
	workerID  string
	settings  maintenanceQueueAlertSettings
	client    *http.Client
	lastCheck time.Time
	lastSent  time.Time
	alerting  bool
}

func newMaintenanceQueueAlertManager(workerID string) *maintenanceQueueAlertManager {
	return &maintenanceQueueAlertManager{
		workerID: workerID,
		settings: maintenanceQueueAlertSettingsFromEnv(),
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func maintenanceQueueAlertSettingsFromEnv() maintenanceQueueAlertSettings {
	telegramEnabled := maintenanceEnvBoolFallback("RIVUS_TELEGRAM_ENABLED", "TELEGRAM_ENABLED", false)
	notifyEnabled := maintenanceEnvBoolDefault("RIVUS_TELEGRAM_NOTIFY_MAINTENANCE_QUEUE", telegramEnabled)
	return maintenanceQueueAlertSettings{
		Enabled:          telegramEnabled && notifyEnabled,
		BotToken:         strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")),
		ChatID:           strings.TrimSpace(os.Getenv("TELEGRAM_CHAT_ID")),
		PendingThreshold: intEnv("RIVUS_MAINTENANCE_QUEUE_ALERT_THRESHOLD", defaultMaintenanceQueueAlertThreshold),
		OldestAge:        durationEnv("RIVUS_MAINTENANCE_QUEUE_ALERT_AGE_SECONDS", defaultMaintenanceQueueAlertAge),
		CheckInterval:    durationEnv("RIVUS_MAINTENANCE_QUEUE_ALERT_CHECK_SECONDS", defaultMaintenanceQueueAlertCheck),
		Cooldown:         durationEnv("RIVUS_MAINTENANCE_QUEUE_ALERT_COOLDOWN_SECONDS", defaultMaintenanceQueueAlertCooldown),
	}
}

func (m *maintenanceQueueAlertManager) MaybeCheck(ctx context.Context, store *meta.IcebergMaintenanceStore, now time.Time) {
	if m == nil || store == nil || !m.settings.Enabled || m.settings.BotToken == "" || m.settings.ChatID == "" {
		return
	}
	if !m.lastCheck.IsZero() && now.Sub(m.lastCheck) < m.settings.CheckInterval {
		return
	}
	m.lastCheck = now

	summary, err := store.Summary(ctx, now)
	if err != nil {
		log.Printf("[maintenance-worker %s] queue alert summary error=%v", m.workerID, err)
		return
	}
	backlogged := maintenanceQueueBacklogged(summary, m.settings)
	if !backlogged {
		if m.alerting {
			if err := m.send(ctx, formatMaintenanceQueueRecovered(summary)); err != nil {
				log.Printf("[maintenance-worker %s] queue recovery notification error=%v", m.workerID, err)
				return
			}
			m.alerting = false
		}
		return
	}

	if m.alerting && !m.lastSent.IsZero() && now.Sub(m.lastSent) < m.settings.Cooldown {
		return
	}
	if err := m.send(ctx, formatMaintenanceQueueBacklog(summary, m.settings)); err != nil {
		log.Printf("[maintenance-worker %s] queue backlog notification error=%v", m.workerID, err)
		return
	}
	m.alerting = true
	m.lastSent = now
}

func maintenanceQueueBacklogged(summary meta.IcebergMaintenanceSummary, settings maintenanceQueueAlertSettings) bool {
	pending := summary.QueuedTasks + summary.RetryTasks
	return pending >= settings.PendingThreshold &&
		time.Duration(summary.OldestQueuedAgeSec)*time.Second >= settings.OldestAge
}

func formatMaintenanceQueueBacklog(summary meta.IcebergMaintenanceSummary, settings maintenanceQueueAlertSettings) string {
	pending := summary.QueuedTasks + summary.RetryTasks
	return fmt.Sprintf(
		"⚠️ Rivus Maintenance Queue Backlog\n\n• Pending: %d\n• Queued: %d\n• Retry: %d\n• Active leases: %d\n• Failed: %d\n• Oldest pending: %s\n• Alert threshold: %d tasks for %s",
		pending,
		summary.QueuedTasks,
		summary.RetryTasks,
		summary.ActiveLeases,
		summary.FailedTasks,
		formatMaintenanceQueueAge(summary.OldestQueuedAgeSec),
		settings.PendingThreshold,
		settings.OldestAge,
	)
}

func formatMaintenanceQueueRecovered(summary meta.IcebergMaintenanceSummary) string {
	pending := summary.QueuedTasks + summary.RetryTasks
	return fmt.Sprintf(
		"✅ Rivus Maintenance Queue Recovered\n\n• Pending: %d\n• Queued: %d\n• Retry: %d\n• Active leases: %d",
		pending,
		summary.QueuedTasks,
		summary.RetryTasks,
		summary.ActiveLeases,
	)
}

func formatMaintenanceQueueAge(seconds int64) string {
	if seconds <= 0 {
		return "0s"
	}
	return (time.Duration(seconds) * time.Second).Round(time.Second).String()
}

func (m *maintenanceQueueAlertManager) send(ctx context.Context, text string) error {
	values := url.Values{}
	values.Set("chat_id", m.settings.ChatID)
	values.Set("text", text)
	values.Set("disable_web_page_preview", "true")

	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", m.settings.BotToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram sendMessage failed: %s", resp.Status)
	}
	return nil
}

func maintenanceEnvBoolFallback(primary, secondary string, fallback bool) bool {
	if value, ok := maintenanceEnvBoolLookup(primary); ok {
		return value
	}
	if value, ok := maintenanceEnvBoolLookup(secondary); ok {
		return value
	}
	return fallback
}

func maintenanceEnvBoolDefault(key string, fallback bool) bool {
	if value, ok := maintenanceEnvBoolLookup(key); ok {
		return value
	}
	return fallback
}

func maintenanceEnvBoolLookup(key string) (bool, bool) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return false, false
	}
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, false
	}
	return value, true
}
