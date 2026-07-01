package core

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/model"
)

func TestJobManagerNotifiesOnFailedJob(t *testing.T) {
	reg := connector.NewRegistry()
	reg.RegisterSource("failing_source", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		return sourceFunc(func(ctx context.Context, out chan<- model.Event) error {
			return errors.New("binlog connection lost")
		}), nil
	})
	reg.RegisterSink("doris", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		return sinkFunc(func(ctx context.Context, in <-chan model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})

	notifier := &recordingJobFailureNotifier{ch: make(chan jobFailureNotification, 1)}
	manager := NewJobManager(
		reg,
		withJobFailureNotifier(notifier),
	)

	cfg := &config.JobConfig{
		ID:   "job-failed",
		Name: "job-failed",
		Mode: config.JobModeInitial,
		Source: &config.ConnectorSpec{
			Type:   "failing_source",
			Config: map[string]any{},
		},
		Sink: &config.ConnectorSpec{
			Type:   "doris",
			Config: map[string]any{},
		},
		Notifications: config.JobNotificationsConfig{
			Telegram: config.TelegramNotificationConfig{
				Enabled:         true,
				BotToken:        "bot-token",
				ChatID:          "chat-id",
				UIBaseURL:       "https://rivus.example.com/",
				NotifyJobFailed: true,
			},
		},
	}
	config.ApplyDefaults(cfg)

	job, err := manager.Submit(cfg)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	waitForCondition(t, "job to fail", func() bool {
		return job.GetStatus() == JobStatusFailed
	})

	select {
	case payload := <-notifier.ch:
		if payload.JobID != "job-failed" {
			t.Fatalf("payload.JobID = %q, want %q", payload.JobID, "job-failed")
		}
		if payload.SinkType != "doris" {
			t.Fatalf("payload.SinkType = %q, want %q", payload.SinkType, "doris")
		}
		if payload.ErrorComponent != "source" {
			t.Fatalf("payload.ErrorComponent = %q, want %q", payload.ErrorComponent, "source")
		}
		if !strings.Contains(payload.ErrorMessage, "binlog connection lost") {
			t.Fatalf("payload.ErrorMessage = %q, want message to contain source failure", payload.ErrorMessage)
		}
		if payload.DashboardURL != "https://rivus.example.com/?tab=doris" {
			t.Fatalf("payload.DashboardURL = %q", payload.DashboardURL)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failed-job notification")
	}
}

func TestJobManagerRejectsImmediateStartFailureWithoutNotification(t *testing.T) {
	reg := connector.NewRegistry()
	reg.RegisterSource("invalid_source", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		return nil, errors.New("source config invalid")
	})

	notifier := &recordingJobFailureNotifier{ch: make(chan jobFailureNotification, 1)}
	manager := NewJobManager(
		reg,
		withJobFailureNotifier(notifier),
	)

	cfg := newNotificationTestJobConfig("job-immediate-failed", "invalid_source", "doris")
	if _, err := manager.Submit(cfg); err == nil {
		t.Fatalf("Submit succeeded, want source factory error")
	} else if !strings.Contains(err.Error(), "source config invalid") {
		t.Fatalf("Submit error = %v, want source factory error", err)
	}

	if _, err := manager.Get("job-immediate-failed"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("manager.Get error = %v, want %v", err, ErrJobNotFound)
	}

	select {
	case payload := <-notifier.ch:
		t.Fatalf("unexpected immediate failed-job notification: %+v", payload)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestJobManagerRecoversAndNotifiesSourcePanic(t *testing.T) {
	reg := connector.NewRegistry()
	reg.RegisterSource("panic_source", func(jctx connector.JobContext, cfg any) (connector.Source, error) {
		return sourceFunc(func(ctx context.Context, out chan<- model.Event) error {
			panic("snapshot worker crashed")
		}), nil
	})
	reg.RegisterSink("doris", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		return sinkFunc(func(ctx context.Context, in <-chan model.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	})

	notifier := &recordingJobFailureNotifier{ch: make(chan jobFailureNotification, 1)}
	manager := NewJobManager(
		reg,
		withJobFailureNotifier(notifier),
	)

	job, err := manager.Submit(newNotificationTestJobConfig("job-panic", "panic_source", "doris"))
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	waitForCondition(t, "panic job to fail", func() bool {
		return job.GetStatus() == JobStatusFailed
	})

	select {
	case payload := <-notifier.ch:
		if payload.ErrorComponent != "source" {
			t.Fatalf("payload.ErrorComponent = %q, want %q", payload.ErrorComponent, "source")
		}
		if !strings.Contains(payload.ErrorMessage, "source panic: snapshot worker crashed") {
			t.Fatalf("payload.ErrorMessage = %q, want panic message", payload.ErrorMessage)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for panic failed-job notification")
	}
}

func TestBuildJobFailureNotificationFallsBackToNativeIcebergSinkTelegramConfig(t *testing.T) {
	cfg := &config.JobConfig{
		ID:   "native-job",
		Name: "native-job",
		Sink: &config.ConnectorSpec{
			Type: "iceberg_native",
			Config: map[string]any{
				"telegram_enabled":   true,
				"telegram_bot_token": "native-bot-token",
				"telegram_chat_id":   "native-chat-id",
				"ui_base_url":        "https://rivus.example.com/",
			},
		},
	}

	job := NewJob(cfg, nil)
	job.addError("sink", errors.New("iceberg write failed"))
	job.setStatus(JobStatusFailed)

	payload, ok := buildJobFailureNotification(job)
	if !ok {
		t.Fatal("expected native iceberg failure notification to be enabled")
	}
	if payload.Telegram.BotToken != "native-bot-token" {
		t.Fatalf("payload.Telegram.BotToken = %q", payload.Telegram.BotToken)
	}
	if payload.DashboardURL != "https://rivus.example.com/?tab=iceberg" {
		t.Fatalf("payload.DashboardURL = %q", payload.DashboardURL)
	}
	if payload.ErrorMessage != "iceberg write failed" {
		t.Fatalf("payload.ErrorMessage = %q", payload.ErrorMessage)
	}
}

func TestFormatJobFailureTelegramTextUsesErrorSnippet(t *testing.T) {
	text := formatJobFailureTelegramText(jobFailureNotification{
		JobID:        "job-long",
		JobName:      "Long Failure",
		SinkType:     "doris",
		ErrorMessage: strings.Repeat("source error ", 1000),
		Telegram: config.TelegramNotificationConfig{
			Enabled:         true,
			BotToken:        "bot-token",
			ChatID:          "chat-id",
			NotifyJobFailed: true,
		},
	})

	if len(text) > telegramMessageMaxLen {
		t.Fatalf("formatted text length = %d, want <= %d", len(text), telegramMessageMaxLen)
	}
	if len(text) > 1000 {
		t.Fatalf("formatted text length = %d, want a short notification snippet", len(text))
	}
	if !strings.Contains(text, "[see dashboard]") {
		t.Fatalf("formatted text should point to dashboard for full error")
	}
}

func TestTelegramJobFailureNotifierSendsExpectedPayload(t *testing.T) {
	var (
		gotPath string
		gotForm url.Values
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll returned error: %v", err)
		}
		values, err := url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("ParseQuery returned error: %v", err)
		}
		gotForm = values
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	notifier := &telegramJobFailureNotifier{
		client:     server.Client(),
		apiBaseURL: server.URL,
	}

	err := notifier.NotifyJobFailed(context.Background(), jobFailureNotification{
		JobID:           "job-1",
		JobName:         "Nightly Sync",
		SinkType:        "doris",
		ErrorComponent:  "sink",
		ErrorMessage:    "stream load failed",
		ProgressSummary: "Job failed",
		ProgressDetail:  "table users",
		DashboardURL:    "https://rivus.example.com/?tab=doris",
		Telegram: config.TelegramNotificationConfig{
			Enabled:         true,
			BotToken:        "123:abc",
			ChatID:          "999",
			NotifyJobFailed: true,
		},
	})
	if err != nil {
		t.Fatalf("NotifyJobFailed returned error: %v", err)
	}

	if gotPath != "/bot123:abc/sendMessage" {
		t.Fatalf("request path = %q", gotPath)
	}
	if got := gotForm.Get("chat_id"); got != "999" {
		t.Fatalf("chat_id = %q, want %q", got, "999")
	}
	if got := gotForm.Get("disable_web_page_preview"); got != "true" {
		t.Fatalf("disable_web_page_preview = %q, want %q", got, "true")
	}
	text := gotForm.Get("text")
	for _, needle := range []string{
		"🚨 Rivus Job Failed",
		"• Job: Nightly Sync",
		"• Job ID: job-1",
		"• Component: sink",
		"💥 Error\nstream load failed",
		"📍 Progress\n• Job failed\n• table users",
		"🔎 Review\nhttps://rivus.example.com/?tab=doris",
	} {
		if !strings.Contains(text, needle) {
			t.Fatalf("telegram text %q does not contain %q", text, needle)
		}
	}
}

func newNotificationTestJobConfig(id, sourceType, sinkType string) *config.JobConfig {
	cfg := &config.JobConfig{
		ID:   id,
		Name: id,
		Mode: config.JobModeInitial,
		Source: &config.ConnectorSpec{
			Type:   sourceType,
			Config: map[string]any{},
		},
		Sink: &config.ConnectorSpec{
			Type:   sinkType,
			Config: map[string]any{},
		},
		Notifications: config.JobNotificationsConfig{
			Telegram: config.TelegramNotificationConfig{
				Enabled:         true,
				BotToken:        "bot-token",
				ChatID:          "chat-id",
				UIBaseURL:       "https://rivus.example.com/",
				NotifyJobFailed: true,
			},
		},
	}
	config.ApplyDefaults(cfg)
	return cfg
}

type recordingJobFailureNotifier struct {
	ch chan jobFailureNotification
}

func (n *recordingJobFailureNotifier) NotifyJobFailed(ctx context.Context, payload jobFailureNotification) error {
	select {
	case n.ch <- payload:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
