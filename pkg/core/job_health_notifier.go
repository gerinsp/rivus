package core

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/gerinsp/rivus/pkg/config"
)

type jobHealthAlertType string

const (
	jobHealthAlertCDCLag           jobHealthAlertType = "cdc_lag"
	jobHealthAlertCheckpointPurged jobHealthAlertType = "checkpoint_purged"
	jobHealthAlertBackpressure     jobHealthAlertType = "backpressure"
)

type jobHealthNotification struct {
	AlertType      jobHealthAlertType
	JobID          string
	JobName        string
	SinkType       string
	Detail         string
	CheckpointFile string
	CheckpointPos  uint32
	LatestFile     string
	LatestPos      uint32
	EarliestFile   string
	AvailableCount int
	LagFiles       int
	DashboardURL   string
	Telegram       config.TelegramNotificationConfig
}

type jobHealthNotifier interface {
	NotifyJobHealth(context.Context, jobHealthNotification) error
}

func (n *telegramJobFailureNotifier) NotifyJobHealth(ctx context.Context, payload jobHealthNotification) error {
	tg := payload.Telegram
	if !tg.Enabled || strings.TrimSpace(tg.BotToken) == "" || strings.TrimSpace(tg.ChatID) == "" {
		return nil
	}
	switch payload.AlertType {
	case jobHealthAlertCDCLag:
		if !tg.NotifyCDCLag {
			return nil
		}
	case jobHealthAlertCheckpointPurged:
		if !tg.NotifyCheckpointPurged {
			return nil
		}
	case jobHealthAlertBackpressure:
		if !tg.NotifyBackpressure {
			return nil
		}
	default:
		return nil
	}

	values := url.Values{}
	values.Set("chat_id", tg.ChatID)
	values.Set("text", formatJobHealthTelegramText(payload))
	values.Set("disable_web_page_preview", "true")

	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", strings.TrimRight(n.apiBaseURL, "/"), tg.BotToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram sendMessage failed: %s", resp.Status)
	}
	return nil
}

func formatJobHealthTelegramText(payload jobHealthNotification) string {
	title := "⚠️ Rivus Health Alert"
	switch payload.AlertType {
	case jobHealthAlertCDCLag:
		title = "⚠️ Rivus CDC Lag"
	case jobHealthAlertCheckpointPurged:
		title = "🚨 Rivus Binlog Checkpoint Purged"
	case jobHealthAlertBackpressure:
		title = "⚠️ Rivus Buffer Backpressure"
	}

	lines := []string{
		title,
		"",
		notificationField("Job", firstNonEmptyLabel(payload.JobName, payload.JobID)),
		notificationField("Job ID", payload.JobID),
	}
	if payload.SinkType != "" {
		lines = append(lines, notificationField("Sink", payload.SinkType))
	}
	if payload.AlertType == jobHealthAlertCDCLag {
		lines = append(lines,
			notificationField("Lag", fmt.Sprintf("%d binlog file(s) behind latest", payload.LagFiles)),
			notificationField("Checkpoint", formatBinlogPosition(payload.CheckpointFile, payload.CheckpointPos)),
			notificationField("Latest", formatBinlogPosition(payload.LatestFile, payload.LatestPos)),
		)
	}
	if payload.AlertType == jobHealthAlertCheckpointPurged {
		lines = append(lines,
			notificationField("Checkpoint", formatBinlogPosition(payload.CheckpointFile, payload.CheckpointPos)),
			notificationField("Earliest available", payload.EarliestFile),
			notificationField("Latest", formatBinlogPosition(payload.LatestFile, payload.LatestPos)),
			notificationField("Available binlogs", fmt.Sprintf("%d", payload.AvailableCount)),
			"",
			"The live CDC connection may continue, but restarting now cannot resume from this checkpoint. Increase MySQL binlog retention.",
		)
	}
	if strings.TrimSpace(payload.Detail) != "" {
		lines = append(lines, "", "📍 Detail", notificationSnippet(payload.Detail, telegramDetailSnippetMaxLen))
	}
	if payload.DashboardURL != "" {
		lines = append(lines, "", "🔎 Review", cleanNotificationText(payload.DashboardURL))
	}
	return truncateTelegramText(strings.Join(lines, "\n"))
}

func formatBinlogPosition(file string, pos uint32) string {
	file = strings.TrimSpace(file)
	if file == "" {
		file = "unknown"
	}
	return fmt.Sprintf("%s:%d", file, pos)
}

func buildJobHealthNotification(job *Job, progress *JobProgress, alertType jobHealthAlertType) (jobHealthNotification, bool) {
	if job == nil || job.Config == nil || progress == nil {
		return jobHealthNotification{}, false
	}
	tg, ok := jobHealthTelegramConfig(job.Config)
	if !ok {
		return jobHealthNotification{}, false
	}
	return jobHealthNotification{
		AlertType:      alertType,
		JobID:          strings.TrimSpace(job.Config.ID),
		JobName:        strings.TrimSpace(job.Config.Name),
		SinkType:       sinkTypeFromConfig(job.Config),
		Detail:         firstNonEmptyLabel(progress.Detail, progress.Summary),
		CheckpointFile: progress.CDCCheckpointFile,
		CheckpointPos:  progress.CDCCheckpointPos,
		LatestFile:     progress.CDCLatestFile,
		LatestPos:      progress.CDCLatestPos,
		EarliestFile:   progress.CDCEarliestFile,
		AvailableCount: progress.CDCAvailableBinlogs,
		LagFiles:       progress.CDCLagFiles,
		DashboardURL:   buildJobDashboardURL(tg.UIBaseURL, job.Config),
		Telegram:       tg,
	}, true
}

func jobHealthTelegramConfig(cfg *config.JobConfig) (config.TelegramNotificationConfig, bool) {
	tg := telegramFailureConfigFromEnv()
	if cfg != nil {
		tg = mergeTelegramNotificationConfig(tg, cfg.Notifications.Telegram)
	}
	if tg.CDCLagFilesThreshold <= 0 {
		tg.CDCLagFilesThreshold = 2
	}
	if tg.AlertCooldownSeconds <= 0 {
		tg.AlertCooldownSeconds = 600
	}
	tg.BotToken = strings.TrimSpace(tg.BotToken)
	tg.ChatID = strings.TrimSpace(tg.ChatID)
	tg.UIBaseURL = strings.TrimRight(strings.TrimSpace(tg.UIBaseURL), "/")
	if !tg.Enabled || tg.BotToken == "" || tg.ChatID == "" || (!tg.NotifyCDCLag && !tg.NotifyCheckpointPurged && !tg.NotifyBackpressure) {
		return config.TelegramNotificationConfig{}, false
	}
	return tg, true
}
