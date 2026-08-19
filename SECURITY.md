# Security Policy

## Supported versions

Rivus is currently pre-1.0.

Security fixes are actively targeted at:

| Version | Support |
|---|---|
| latest tagged release | supported |
| `main` | supported for upcoming fixes and development |
| older pre-1.0 releases | best effort |

When practical, a security fix that affects the latest stable release will be included in a new patch release in addition to landing on `main`.

Because older pre-1.0 releases may contain deployment or metadata differences, operators should plan to move to the latest qualified release rather than depend on long-term patch branches.

See [`docs/release-policy.md`](docs/release-policy.md) for the broader release policy.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability.

Report security issues privately to the project maintainers using a private repository/maintainer communication channel. Include:

- affected Rivus version or commit;
- steps to reproduce;
- impact and affected components;
- whether credentials, source data, checkpoints, or object storage are exposed;
- any relevant logs with secrets, private hostnames, and customer data removed.

Do not paste API tokens, database passwords, cloud keys, source credentials, or unredacted production logs into a public issue.

Maintainers will acknowledge the report, reproduce/assess the issue when possible, coordinate a fix, and avoid public disclosure of exploitable details before a remediation path is available.

## Deployment responsibility

Rivus is distributed under the Apache License 2.0 on an `AS IS` basis. Operators remain responsible for securing their deployment, including:

- UI/API authentication;
- TLS termination;
- metadata database access;
- source/sink credentials;
- object-storage permissions;
- network segmentation;
- secret rotation;
- log redaction and retention.

See [`docs/production-deployment.md`](docs/production-deployment.md) for the production security checklist.
