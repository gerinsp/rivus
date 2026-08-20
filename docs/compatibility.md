# Compatibility

Rivus is pre-1.0 and does not yet publish a broad vendor/version certification matrix. This document separates **implemented integrations** from **formally validated version ranges** so operators do not mistake compilation or anecdotal use for a compatibility guarantee.

## Current integration matrix

| Component | Status | Notes |
|---|---|---|
| MySQL source snapshots | Implemented | Chunked initial snapshots and snapshot-only mode are first-class source workflows. |
| MySQL binlog CDC | Implemented | Uses `go-mysql`; source must expose the binlog and retain required history for resume. |
| Apache Iceberg REST catalog | Implemented | Native Iceberg sink with REST catalog and object-storage-backed tables. |
| S3-compatible object storage | Implemented integration surface | Configured through Iceberg/S3 settings. Vendor-specific certification is not yet published. |
| Apache Doris | Implemented | Native sink includes table creation, DDL forwarding, batching, retries, and stream load support. |
| Metadata MySQL | Required | The reference Compose uses MySQL 8.0. Durable maintenance leasing relies on MySQL 8 behavior such as `FOR UPDATE SKIP LOCKED`. |
| Spark maintenance runner | Optional | Used for Spark compaction when Iceberg maintenance is configured for `hybrid` or `spark`. |
| Linux amd64 images | Published | Built by the container workflow. |
| Linux arm64 images | Published | Built by the container workflow with Buildx/QEMU. |
| Go toolchain | Go 1.25.x in CI | Source development and CI currently target Go 1.25.x. |

## What “implemented” means

“Implemented” means the integration exists in the codebase and is part of the normal Rivus execution path. It does **not** mean every upstream patch release, cloud distribution, proxy, storage gateway, or configuration combination has been certified.

Until a version appears in a published tested-version table, production adopters should run an environment-specific qualification test before rollout.

## Qualification checklist

For each external system used in production, record:

- exact product and version;
- deployment mode or managed-service name;
- authentication mechanism;
- network/proxy layer;
- relevant source/sink settings;
- successful snapshot test;
- successful CDC insert/update/delete test;
- restart/resume test;
- schema-change test if DDL forwarding is required;
- graceful worker replacement test;
- expected failure/retry behavior;
- throughput and memory observed under representative load.

Keep the qualification record with the deployment configuration so future upgrades can compare known-good combinations.

## MySQL source expectations

Rivus requires a source that supports the MySQL binlog protocol used by `go-mysql` and permits the configured Rivus account to read required schema/table data and replication events.

Before production use, verify:

- binlog is enabled;
- binlog format/settings match the CDC mode used by the deployment;
- the Rivus account has required snapshot and replication permissions;
- source timeouts allow long-lived replication connections;
- binlog retention exceeds the maximum expected outage and snapshot handoff window.

A missing/purged checkpoint binlog may require a new snapshot rather than a normal resume.

## Metadata MySQL expectations

Metadata MySQL is part of Rivus' correctness model, not a disposable cache.

The reference deployment uses MySQL 8.0. Operators should treat MySQL 8.x as the current safe baseline until the project publishes tests for alternatives.

Do not point multiple unrelated Rivus installations at the same metadata schema unless they are intentionally part of the same control plane.

## Iceberg expectations

Rivus' native Iceberg integration expects an Iceberg REST catalog and object storage reachable from the runtime roles that need them.

For split-runtime deployments, keep catalog and object-store configuration consistent across:

- streaming worker;
- snapshot worker;
- maintenance worker.

Catalog-specific behavior can vary. Validate namespace/table creation, commit behavior, schema evolution used by your jobs, object-store access, and maintenance operations against the exact catalog implementation you deploy.

## Doris expectations

The Doris sink is implemented as an analytical destination, but a formal Doris version certification table is not yet published.

Before adopting a new Doris version in production, validate:

- table creation/mapping used by your job configuration;
- stream-load behavior;
- nullability/type conversion behavior;
- DDL forwarding if enabled;
- retry behavior during FE/BE interruptions;
- restart/resume without duplicate or missing expected state for your table model.

## Tested-version policy

A future entry in this document should only be marked **verified** when the version combination has a repeatable integration test or a recorded qualification run.

Recommended format:

| Rivus version | Source | Sink/catalog | Metadata DB | Result |
|---|---|---|---|---|
| `vX.Y.Z` | `MySQL X.Y.Z` | `Iceberg REST implementation X` | `MySQL 8.x` | verified |

Do not add guessed version ranges.

## Reporting compatibility problems

When opening an issue, include sanitized versions and configuration details, but remove credentials, private hostnames, tokens, and customer data.

Useful information includes:

- Rivus version or commit;
- source version;
- sink/catalog version;
- metadata MySQL version;
- relevant log excerpt with secrets removed;
- smallest reproducible job configuration;
- whether the failure occurs during snapshot, CDC, handoff, or maintenance.
