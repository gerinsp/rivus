package core

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

type jobFailureNotification struct {
	JobID           string
	JobName         string
	SinkType        string
	ErrorComponent  string
	ErrorMessage    string
	ProgressSummary string
	ProgressDetail  string
	DashboardURL    string
	Telegram        config.TelegramNotificationConfig
}

type jobFailureNotifier interface {
	NotifyJobFailed(ctx context.Context, payload jobFailureNotification) error
}

type telegramJobFailureNotifier struct {
	client     *http.Client
	apiBaseURL string
}

const (
	telegramMessageMaxLen        = 3900
	telegramErrorSnippetMaxLen   = 700
	telegramDetailSnippetMaxLen  = 500
	telegramSnippetTruncateLabel = " ... [see dashboard]"
)

func newTelegramJobFailureNotifier(client *http.Client) jobFailureNotifier {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &telegramJobFailureNotifier{
		client:     client,
		apiBaseURL: "https://api.telegram.org",
	}
}

func (n *telegramJobFailureNotifier) NotifyJobFailed(ctx context.Context, payload jobFailureNotification) error {
	tg := payload.Telegram
	if !tg.Enabled || !tg.NotifyJobFailed {
		return nil
	}
	if strings.TrimSpace(tg.BotToken) == "" || strings.TrimSpace(tg.ChatID) == "" {
		return nil
	}

	text := formatJobFailureTelegramText(payload)

	values := url.Values{}
	values.Set("chat_id", tg.ChatID)
	values.Set("text", text)
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
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		msg := strings.TrimSpace(string(data))
		if msg == "" {
			msg = resp.Status
		}
		return fmt.Errorf("telegram sendMessage failed: %s", msg)
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func formatJobFailureTelegramText(payload jobFailureNotification) string {
	lines := []string{
		"🚨 Rivus Job Failed",
		"",
		notificationField("Job", firstNonEmptyLabel(payload.JobName, payload.JobID)),
		notificationField("Job ID", payload.JobID),
	}
	if payload.SinkType != "" {
		lines = append(lines, notificationField("Sink", payload.SinkType))
	}
	if payload.ErrorComponent != "" {
		lines = append(lines, notificationField("Component", payload.ErrorComponent))
	}
	if payload.ErrorMessage != "" {
		lines = append(lines, "", "💥 Error", notificationSnippet(payload.ErrorMessage, telegramErrorSnippetMaxLen))
	}
	progressLines := make([]string, 0, 2)
	if payload.ProgressSummary != "" {
		progressLines = append(progressLines, "• "+cleanNotificationText(payload.ProgressSummary))
	}
	if payload.ProgressDetail != "" && payload.ProgressDetail != payload.ErrorMessage {
		progressLines = append(progressLines, "• "+notificationSnippet(payload.ProgressDetail, telegramDetailSnippetMaxLen))
	}
	if len(progressLines) > 0 {
		lines = append(lines, "", "📍 Progress")
		lines = append(lines, progressLines...)
	}
	if payload.DashboardURL != "" {
		lines = append(lines, "", "🔎 Review", cleanNotificationText(payload.DashboardURL))
	}

	return truncateTelegramText(strings.Join(lines, "\n"))
}

func notificationField(label, value string) string {
	return fmt.Sprintf("• %s: %s", label, cleanNotificationText(value))
}

func notificationSnippet(value string, maxLen int) string {
	return truncateNotificationText(cleanNotificationText(value), maxLen)
}

func cleanNotificationText(value string) string {
	lines := strings.FieldsFunc(strings.TrimSpace(value), func(r rune) bool {
		return r == '\r' || r == '\n'
	})
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return strings.Join(out, " | ")
}

func truncateNotificationText(text string, maxLen int) string {
	if maxLen <= 0 || len(text) <= maxLen {
		return text
	}
	limit := maxLen - len(telegramSnippetTruncateLabel)
	if limit <= 0 {
		return text[:maxLen]
	}
	end := 0
	for idx := range text {
		if idx > limit {
			break
		}
		end = idx
	}
	if end == 0 {
		return text[:limit] + telegramSnippetTruncateLabel
	}
	return strings.TrimSpace(text[:end]) + telegramSnippetTruncateLabel
}

func truncateTelegramText(text string) string {
	if len(text) <= telegramMessageMaxLen {
		return text
	}
	const suffix = "\n... [truncated]"
	limit := telegramMessageMaxLen - len(suffix)
	if limit <= 0 {
		return text[:telegramMessageMaxLen]
	}
	end := 0
	for idx := range text {
		if idx > limit {
			break
		}
		end = idx
	}
	if end == 0 {
		return text[:limit] + suffix
	}
	return text[:end] + suffix
}

func buildJobFailureNotification(job *Job) (jobFailureNotification, bool) {
	if job == nil || job.Config == nil {
		return jobFailureNotification{}, false
	}

	tg, ok := jobFailureTelegramConfig(job.Config)
	if !ok {
		return jobFailureNotification{}, false
	}

	payload := jobFailureNotification{
		JobID:        strings.TrimSpace(job.Config.ID),
		JobName:      strings.TrimSpace(job.Config.Name),
		SinkType:     sinkTypeFromConfig(job.Config),
		DashboardURL: buildJobDashboardURL(tg.UIBaseURL, job.Config),
		Telegram:     tg,
	}

	if progress := job.Progress(); progress != nil {
		payload.ProgressSummary = strings.TrimSpace(progress.Summary)
		payload.ProgressDetail = strings.TrimSpace(progress.Detail)
	}
	if lastError := job.GetLastError(); lastError != nil {
		payload.ErrorComponent = strings.TrimSpace(lastError.Component)
		payload.ErrorMessage = strings.TrimSpace(lastError.Message)
	}

	return payload, true
}

func jobFailureTelegramConfig(cfg *config.JobConfig) (config.TelegramNotificationConfig, bool) {
	tg := telegramFailureConfigFromEnv()

	if sinkTG, ok := icebergSinkTelegramFailureConfig(cfg); ok {
		tg = mergeTelegramNotificationConfig(tg, sinkTG)
	}

	if cfg != nil {
		tg = mergeTelegramNotificationConfig(tg, cfg.Notifications.Telegram)
	}

	tg.BotToken = strings.TrimSpace(tg.BotToken)
	tg.ChatID = strings.TrimSpace(tg.ChatID)
	tg.UIBaseURL = strings.TrimRight(strings.TrimSpace(tg.UIBaseURL), "/")
	if !tg.Enabled || !tg.NotifyJobFailed || tg.BotToken == "" || tg.ChatID == "" {
		return config.TelegramNotificationConfig{}, false
	}
	return tg, true
}

func icebergSinkTelegramFailureConfig(cfg *config.JobConfig) (config.TelegramNotificationConfig, bool) {
	if cfg == nil || cfg.Sink == nil || cfg.Sink.Config == nil {
		return config.TelegramNotificationConfig{}, false
	}
	sinkType := sinkTypeFromConfig(cfg)
	if sinkType != "iceberg_native" {
		return config.TelegramNotificationConfig{}, false
	}

	if !boolConfigValue(cfg.Sink.Config["telegram_enabled"]) {
		return config.TelegramNotificationConfig{}, false
	}

	botToken := stringConfigValue(cfg.Sink.Config["telegram_bot_token"])
	chatID := stringConfigValue(cfg.Sink.Config["telegram_chat_id"])
	if botToken == "" || chatID == "" {
		return config.TelegramNotificationConfig{}, false
	}

	return config.TelegramNotificationConfig{
		Enabled:         true,
		BotToken:        botToken,
		ChatID:          chatID,
		UIBaseURL:       strings.TrimRight(stringConfigValue(cfg.Sink.Config["ui_base_url"]), "/"),
		NotifyJobFailed: true,
	}, true
}

func telegramFailureConfigFromEnv() config.TelegramNotificationConfig {
	enabled := parseEnvBoolLocal(os.Getenv("RIVUS_TELEGRAM_ENABLED")) || parseEnvBoolLocal(os.Getenv("TELEGRAM_ENABLED"))

	notifyFailed, ok := lookupEnvBool("RIVUS_TELEGRAM_NOTIFY_JOB_FAILED")
	if !ok {
		notifyFailed, ok = lookupEnvBool("TELEGRAM_NOTIFY_JOB_FAILED")
	}
	if !ok {
		notifyFailed = enabled
	}

	return config.TelegramNotificationConfig{
		Enabled:         enabled,
		BotToken:        strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")),
		ChatID:          strings.TrimSpace(os.Getenv("TELEGRAM_CHAT_ID")),
		UIBaseURL:       strings.TrimRight(strings.TrimSpace(os.Getenv("RIVUS_UI_BASE_URL")), "/"),
		NotifyJobFailed: notifyFailed,
	}
}

func mergeTelegramNotificationConfig(base, override config.TelegramNotificationConfig) config.TelegramNotificationConfig {
	out := base

	if strings.TrimSpace(override.BotToken) != "" {
		out.BotToken = strings.TrimSpace(override.BotToken)
	}
	if strings.TrimSpace(override.ChatID) != "" {
		out.ChatID = strings.TrimSpace(override.ChatID)
	}
	if strings.TrimSpace(override.UIBaseURL) != "" {
		out.UIBaseURL = strings.TrimRight(strings.TrimSpace(override.UIBaseURL), "/")
	}
	out.Enabled = out.Enabled || override.Enabled
	out.NotifyJobFailed = out.NotifyJobFailed || override.NotifyJobFailed

	return out
}

func buildJobDashboardURL(baseURL string, cfg *config.JobConfig) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return ""
	}

	tab := dashboardTabForJob(cfg)
	if tab == "" {
		return baseURL
	}
	return baseURL + "/?tab=" + url.QueryEscape(tab)
}

func dashboardTabForJob(cfg *config.JobConfig) string {
	switch sinkTypeFromConfig(cfg) {
	case "doris":
		return "doris"
	case "iceberg_native":
		return "iceberg"
	default:
		return ""
	}
}

func stringConfigValue(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func boolConfigValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return parseEnvBoolLocal(v)
	default:
		return false
	}
}

func lookupEnvBool(key string) (bool, bool) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return false, false
	}
	return parseEnvBoolLocal(raw), true
}

func parseEnvBoolLocal(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func firstNonEmptyLabel(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
