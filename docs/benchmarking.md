# Benchmarking methodology

Rivus does not currently publish official throughput numbers.

That is intentional: a CDC benchmark without source version, schema shape, transaction size, sink/catalog behavior, network latency, object-store characteristics, maintenance state, and checkpoint settings is easy to misread.

This document defines the minimum information required for a public benchmark so future numbers are reproducible and useful to operators.

## Goals

A Rivus benchmark should answer operational questions such as:

- How many CDC row events per second can a given deployment sustain?
- What end-to-end replication latency is observed under steady load?
- How much CPU and memory do streaming and snapshot workers consume independently?
- How quickly does a worker recover after graceful restart?
- How does Iceberg file count grow under a given flush policy?
- What maintenance cost is introduced by the generated file pattern?

A benchmark should not be presented as a universal maximum.

## Required environment description

Every published result must include:

### Rivus

- Rivus version and commit SHA;
- runtime topology;
- worker CPU/memory limits;
- `GOMEMLIMIT` and relevant concurrency settings;
- snapshot batch settings;
- CDC flush/checkpoint settings;
- Iceberg commit concurrency when applicable;
- maintenance settings when enabled.

### Source

- source product and version;
- CPU/RAM/storage class;
- dataset size before test;
- table count;
- primary-key shape;
- row width distribution;
- binlog settings;
- generator transaction size and commit rate.

### Sink

For Iceberg:

- REST catalog implementation/version;
- object-store product/region;
- network path;
- target file size/flush settings;
- partitioning;
- table format/version settings relevant to the test.

For Doris:

- Doris version;
- FE/BE topology;
- table model/key type;
- stream-load settings;
- replication/storage configuration relevant to throughput.

### Network

- whether components are on the same host, LAN, region, or WAN;
- measured or representative latency between source, Rivus, metadata DB, catalog, object store, and sink.

## Workload profiles

At minimum, use separate profiles instead of one synthetic score.

### Profile A — steady CDC

- preloaded source table;
- continuous inserts/updates/deletes;
- fixed transaction-size distribution;
- no snapshot running;
- maintenance disabled for the first run.

Measure:

- source commit rate;
- Rivus event processing rate;
- p50/p95/p99 source-to-sink latency;
- streaming-worker CPU and RSS;
- checkpoint freshness;
- sink errors/retries.

### Profile B — snapshot throughput

- fixed source dataset;
- no concurrent CDC generation unless the scenario explicitly tests handoff pressure;
- snapshot worker isolated from streaming worker.

Measure:

- rows/second;
- bytes/second;
- snapshot-worker CPU/RSS;
- spool disk usage;
- source query load;
- sink file/load characteristics;
- total snapshot duration.

### Profile C — snapshot + CDC handoff

Start an `initial` job while source writes continue.

Measure:

- snapshot duration;
- captured binlog checkpoint age at handoff;
- time from snapshot completion to streaming claim;
- catch-up duration;
- peak lag;
- final source/sink validation result.

### Profile D — graceful restart

Under steady CDC load:

1. send normal `SIGTERM`;
2. allow configured drain time;
3. replace the streaming worker;
4. observe lease reclaim/resume.

Measure:

- shutdown duration;
- last checkpoint before exit;
- time until replacement worker resumes;
- maximum source-to-sink lag;
- post-restart validation result.

### Profile E — Iceberg maintenance

Run a CDC workload long enough to create representative small-file/delete-file pressure, then enable normal maintenance.

Measure:

- file count before/after;
- selected input bytes/files;
- native vs Spark routing when hybrid mode is enabled;
- maintenance duration;
- maintenance-worker CPU/RSS;
- CDC latency during maintenance;
- commit conflicts/retries;
- object-store read/write volume when available.

## Correctness validation

Performance numbers are invalid if correctness is not checked.

For each run, record a validation strategy such as:

- source/sink row count for a stable window;
- primary-key checksum/sample comparison;
- known inserted/updated/deleted test keys;
- checkpoint position progression;
- absence of unexpected failed jobs/tasks.

For a mutable CDC workload, use a deterministic generator or an audit table so the expected final state can be reconstructed.

## Warm-up and run duration

Do not publish results from a few seconds of startup burst.

Recommended practice:

- warm up until connection pools, table caches, and steady batching behavior stabilize;
- measure steady CDC for at least several checkpoint/flush intervals;
- run long enough for GC/memory behavior to be visible;
- repeat each profile and report variance.

## Metrics to report

A useful result table contains at least:

| Metric | Value |
|---|---|
| Source committed rows/s | |
| Rivus processed rows/s | |
| Sink applied rows/s | |
| p50 replication latency | |
| p95 replication latency | |
| p99 replication latency | |
| Streaming CPU | |
| Streaming RSS | |
| Snapshot CPU/RSS | |
| Metadata DB CPU/latency | |
| Restart recovery time | |
| Error/retry count | |

For Iceberg also report:

| Metric | Value |
|---|---|
| Data files created/hour | |
| Delete files created/hour | |
| Average data-file size | |
| Maintenance frequency | |
| Compaction duration | |
| File count before/after | |

## Result naming

Name benchmark results by environment rather than marketing labels.

Prefer:

```text
MySQL 8.x → Rivus <sha> → Iceberg REST / S3-compatible object store
8 vCPU streaming worker, 4 GiB GOMEMLIMIT, <flush settings>
```

Avoid labels such as “enterprise scale” or “1M events/s ready” without the full test conditions.

## Public benchmark status

No official Rivus benchmark result is committed yet.

When a reproducible result is available, add it under `docs/benchmarks/` with:

- environment manifest;
- job config with secrets removed;
- generator description;
- raw summary metrics;
- correctness result;
- Rivus commit SHA;
- notes about bottlenecks and known limitations.
