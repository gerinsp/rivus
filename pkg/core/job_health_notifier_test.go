package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

type recordingJobHealthNotifier struct {
	ch chan jobHealthNotification
}

func (n *recordingJobHealthNotifier) NotifyJobHealth(ctx context.Context, payload jobHealthNotification) error {
	select {
	case n.ch <- payload:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestJobManagerNotifiesOnCDCLagAndAppliesCooldown(t *testing.T) {
	notifier := &recordingJobHealthNotifier{ch: make(chan jobHealthNotification, 2)}
	manager := NewJobManager(nil, withJobHealthNotifier(notifier))
	job := newRunningHealthNotificationJob("lag-job", config.TelegramNotificationConfig{
		Enabled:              true,
		BotToken:             "bot-token",
		ChatID:               "chat-id",
		NotifyCDCLag:         true,
		CDCLagFilesThreshold: 2,
		AlertCooldownSeconds: 600,
	})
	progress := &JobProgress{
		Phase:             "streaming",
		Summary:           "CDC streaming",
		CDCCheckpointFile: "mysql-bin.000181",
		CDCCheckpointPos:  100,
		CDCLatestFile:     "mysql-bin.000184",
		CDCLatestPos:      900,
		CDCLagFiles:       3,
	}

	manager.maybeNotifyJobHealth(job, progress)
	manager.maybeNotifyJobHealth(job, progress)

	select {
	case payload := <-notifier.ch:
		if payload.AlertType != jobHealthAlertCDCLag {
			t.Fatalf("alert type = %q, want %q", payload.AlertType, jobHealthAlertCDCLag)
		}
		if payload.LagFiles != 3 {
			t.Fatalf("lag files = %d, want 3", payload.LagFiles)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for CDC lag notification")
	}

	select {
	case payload := <-notifier.ch:
		t.Fatalf("cooldown allowed duplicate notification: %+v", payload)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestJobManagerNotifiesOnBackpressure(t *testing.T) {
	notifier := &recordingJobHealthNotifier{ch: make(chan jobHealthNotification, 1)}
	manager := NewJobManager(nil, withJobHealthNotifier(notifier))
	job := newRunningHealthNotificationJob("pressure-job", config.TelegramNotificationConfig{
		Enabled:            true,
		BotToken:           "bot-token",
		ChatID:             "chat-id",
		NotifyBackpressure: true,
	})

	manager.maybeNotifyJobHealth(job, &JobProgress{
		Phase:   "streaming",
		Summary: "Waiting for sink flush",
		Detail:  "Event buffer remained full for 10 seconds",
	})

	select {
	case payload := <-notifier.ch:
		if payload.AlertType != jobHealthAlertBackpressure {
			t.Fatalf("alert type = %q, want %q", payload.AlertType, jobHealthAlertBackpressure)
		}
		if !strings.Contains(payload.Detail, "10 seconds") {
			t.Fatalf("detail = %q, want backpressure duration", payload.Detail)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for backpressure notification")
	}
}

func TestJobManagerNotifiesOncePerPurgedCheckpointIncident(t *testing.T) {
	t.Setenv("RIVUS_TELEGRAM_ENABLED", "true")
	t.Setenv("TELEGRAM_BOT_TOKEN", "bot-token")
	t.Setenv("TELEGRAM_CHAT_ID", "chat-id")
	t.Setenv("RIVUS_TELEGRAM_NOTIFY_CHECKPOINT_PURGED", "true")

	notifier := &recordingJobHealthNotifier{ch: make(chan jobHealthNotification, 3)}
	manager := NewJobManager(nil, withJobHealthNotifier(notifier))
	job := newRunningHealthNotificationJob("purged-job", config.TelegramNotificationConfig{})
	purged := &JobProgress{
		Phase:               "streaming",
		CDCCheckpointFile:   "mysql-bin.000149",
		CDCCheckpointPos:    1052464371,
		CDCEarliestFile:     "mysql-bin.000150",
		CDCLatestFile:       "mysql-bin.000150",
		CDCLatestPos:        101379729,
		CDCAvailableBinlogs: 1,
		CDCBinlogStatus:     "purged",
	}

	manager.maybeNotifyJobHealth(job, purged)
	manager.maybeNotifyJobHealth(job, purged)

	select {
	case payload := <-notifier.ch:
		if payload.AlertType != jobHealthAlertCheckpointPurged {
			t.Fatalf("alert type = %q, want %q", payload.AlertType, jobHealthAlertCheckpointPurged)
		}
		if payload.EarliestFile != "mysql-bin.000150" || payload.AvailableCount != 1 {
			t.Fatalf("unexpected purge details: %+v", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for purged-checkpoint notification")
	}

	select {
	case payload := <-notifier.ch:
		t.Fatalf("unchanged purge incident sent duplicate notification: %+v", payload)
	case <-time.After(100 * time.Millisecond):
	}

	manager.maybeNotifyJobHealth(job, &JobProgress{
		CDCBinlogStatus:   "available",
		CDCCheckpointFile: "mysql-bin.000150",
		CDCEarliestFile:   "mysql-bin.000150",
	})
	manager.maybeNotifyJobHealth(job, purged)

	select {
	case <-notifier.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("purged-checkpoint notification did not re-arm after recovery")
	}
}

func TestJobManagerDoesNotNotifyBelowCDCLagThreshold(t *testing.T) {
	notifier := &recordingJobHealthNotifier{ch: make(chan jobHealthNotification, 1)}
	manager := NewJobManager(nil, withJobHealthNotifier(notifier))
	job := newRunningHealthNotificationJob("healthy-job", config.TelegramNotificationConfig{
		Enabled:              true,
		BotToken:             "bot-token",
		ChatID:               "chat-id",
		NotifyCDCLag:         true,
		CDCLagFilesThreshold: 2,
	})

	manager.maybeNotifyJobHealth(job, &JobProgress{
		CDCCheckpointFile: "mysql-bin.000183",
		CDCLatestFile:     "mysql-bin.000184",
		CDCLagFiles:       1,
	})

	select {
	case payload := <-notifier.ch:
		t.Fatalf("unexpected notification below threshold: %+v", payload)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestFormatJobHealthTelegramTextIncludesLagPositions(t *testing.T) {
	text := formatJobHealthTelegramText(jobHealthNotification{
		AlertType:      jobHealthAlertCDCLag,
		JobID:          "job-1",
		JobName:        "Reservations",
		SinkType:       "doris",
		CheckpointFile: "mysql-bin.000181",
		CheckpointPos:  100,
		LatestFile:     "mysql-bin.000184",
		LatestPos:      900,
		LagFiles:       3,
	})
	for _, want := range []string{
		"Rivus CDC Lag",
		"3 binlog file(s) behind latest",
		"mysql-bin.000181:100",
		"mysql-bin.000184:900",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("formatted notification %q does not contain %q", text, want)
		}
	}
}

func TestFormatJobHealthTelegramTextIncludesPurgedCheckpointDetails(t *testing.T) {
	text := formatJobHealthTelegramText(jobHealthNotification{
		AlertType:      jobHealthAlertCheckpointPurged,
		JobID:          "job-1",
		JobName:        "Reservations",
		SinkType:       "iceberg",
		CheckpointFile: "mysql-bin.000149",
		CheckpointPos:  1052464371,
		EarliestFile:   "mysql-bin.000150",
		LatestFile:     "mysql-bin.000150",
		LatestPos:      101379729,
		AvailableCount: 1,
	})
	for _, want := range []string{
		"Rivus Binlog Checkpoint Purged",
		"mysql-bin.000149:1052464371",
		"mysql-bin.000150",
		"Available binlogs: 1",
		"restarting now cannot resume",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("formatted notification %q does not contain %q", text, want)
		}
	}
}

func newRunningHealthNotificationJob(id string, telegram config.TelegramNotificationConfig) *Job {
	job := NewJob(&config.JobConfig{
		ID:   id,
		Name: id,
		Sink: &config.ConnectorSpec{Type: "doris"},
		Notifications: config.JobNotificationsConfig{
			Telegram: telegram,
		},
	}, nil)
	job.setStatus(JobStatusRunning)
	return job
}
