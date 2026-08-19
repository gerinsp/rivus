# Maintenance concurrency and queue alerts

Queue-mode Iceberg maintenance uses bounded operation-specific executor pools. This keeps lightweight snapshot expiration moving even when compaction is slow, without allowing every maintenance operation to scale together.

## Concurrency

Defaults:

```bash
RIVUS_MAINTENANCE_COMPACT_CONCURRENCY=1
RIVUS_MAINTENANCE_EXPIRE_CONCURRENCY=4
RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY=1
RIVUS_MAINTENANCE_EXECUTOR_IDLE_POLL_SECONDS=5
```

`RIVUS_MAINTENANCE_COMPACT_CONCURRENCY` caps all compaction tasks, including tasks that later route to Spark. Rivus decides the final compaction engine only after planning the table workload, so a single compact pool is the reliable resource guardrail for both native and Spark execution.

`RIVUS_MAINTENANCE_TASK_PAGE_SIZE` is not a queue concurrency setting. Queue executors claim one task at a time so they do not lease a long batch whose later tasks might expire before execution. The setting remains for one-shot compatibility.

For a large installation, a conservative starting point is:

```yaml
environment:
  RIVUS_MAINTENANCE_COMPACT_CONCURRENCY: ${RIVUS_MAINTENANCE_COMPACT_CONCURRENCY:-1}
  RIVUS_MAINTENANCE_EXPIRE_CONCURRENCY: ${RIVUS_MAINTENANCE_EXPIRE_CONCURRENCY:-4}
  RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY: ${RIVUS_MAINTENANCE_ORPHAN_CONCURRENCY:-1}
```

Increase concurrency only after checking CPU, memory, object-storage traffic, catalog load, and queue age.

## Telegram queue backlog alert

The maintenance worker reuses the existing global Telegram configuration:

```bash
RIVUS_TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

Maintenance queue notifications are enabled by default when global Telegram notifications are enabled. They can be controlled independently:

```bash
RIVUS_TELEGRAM_NOTIFY_MAINTENANCE_QUEUE=true
RIVUS_MAINTENANCE_QUEUE_ALERT_THRESHOLD=100
RIVUS_MAINTENANCE_QUEUE_ALERT_AGE_SECONDS=1800
RIVUS_MAINTENANCE_QUEUE_ALERT_CHECK_SECONDS=60
RIVUS_MAINTENANCE_QUEUE_ALERT_COOLDOWN_SECONDS=3600
```

A backlog notification is sent only when **both** conditions are true:

1. queued + retry tasks are at least `RIVUS_MAINTENANCE_QUEUE_ALERT_THRESHOLD`; and
2. the oldest pending task has been waiting at least `RIVUS_MAINTENANCE_QUEUE_ALERT_AGE_SECONDS`.

Using both count and age prevents a healthy short burst from generating an alert. While the queue remains backlogged, repeat alerts are limited by the cooldown. When the queue returns below the backlog condition, Rivus sends a recovery notification.

Example backlog notification:

```text
⚠️ Rivus Maintenance Queue Backlog

• Pending: 184
• Queued: 179
• Retry: 5
• Active leases: 6
• Failed: 0
• Oldest pending: 43m12s
• Alert threshold: 100 tasks for 30m0s
```
