# Release policy

Rivus is currently a pre-1.0 project. The goal of this policy is to make production adoption predictable without pretending the project already has 1.0-level compatibility guarantees.

## Versioning

Rivus uses semantic-style version tags:

```text
vMAJOR.MINOR.PATCH
```

While `MAJOR` is `0`:

- `PATCH` releases should focus on bug fixes, documentation, and backwards-compatible operational improvements;
- `MINOR` releases may introduce breaking configuration, metadata, API, or deployment changes;
- breaking changes must be called out in release notes and, when operationally relevant, in [migration.md](migration.md).

After 1.0, the project should tighten compatibility expectations around semantic versioning.

## Container tags

The publish workflow produces multiple GHCR tags.

### `latest`

`latest` follows the default branch and is a moving target.

Use it for development, evaluation, or environments that intentionally track `main`.

Do **not** use `latest` as the only production rollback reference.

### `sha-<commit>`

SHA tags identify an immutable source commit and are recommended for reproducible deployments and incident rollback.

Example:

```text
ghcr.io/gerinsp/rivus:sha-a2f6c39
```

### `vX.Y.Z`

Version tags identify planned releases and are the preferred human-readable production reference once a release has been qualified in the target environment.

## Release channels

Treat Rivus artifacts as three practical channels:

| Channel | Example | Intended use |
|---|---|---|
| Development | `latest` / `main` | testing new changes |
| Immutable build | `sha-<commit>` | reproducible deployment and rollback |
| Stable release | `vX.Y.Z` | planned production adoption |

A company can choose to deploy a tested SHA before a formal version tag, but it should record that SHA as an internal release artifact.

## Release checklist

Before creating a version tag:

1. CI passes on the release commit;
2. README and migration notes match the runtime behavior;
3. new environment variables are documented;
4. breaking changes are explicitly listed;
5. metadata/schema changes are documented with rollback implications;
6. container build succeeds for supported architectures;
7. at least one representative snapshot + CDC + restart path has been exercised for changes that affect execution;
8. Iceberg maintenance behavior is exercised when maintenance code changed;
9. security-sensitive changes are reviewed for accidental credential/log exposure;
10. the release notes identify known limitations.

## Support window

Before 1.0, the project actively supports:

- the latest tagged release;
- `main` for development and upcoming fixes.

Older pre-1.0 releases are best-effort unless a security issue justifies a targeted patch.

Production operators who cannot upgrade quickly should pin an internally qualified version and test the next release in staging before changing production.

## Security fixes

Security fixes should land on `main` and, when practical, be included in a new patch release for the latest stable line.

If a vulnerability materially affects currently deployed versions, the advisory/release notes should identify the affected range as precisely as the available evidence allows.

See [../SECURITY.md](../SECURITY.md).

## Deprecation

Pre-1.0 features may be deprecated before removal.

When practical, a deprecation should include:

- the replacement behavior;
- the first release where the old behavior is deprecated;
- the planned removal release or milestone;
- migration instructions.

Deployment/runtime changes should not be hidden behind an undocumented default change.

## Release notes format

Recommended release notes sections:

```text
Highlights
Breaking changes
Upgrade notes
Fixes
Security
Known limitations
Container image
```

The container section should include both the version tag and the release commit SHA so an operator can verify the exact artifact.

## Rollback expectations

Every production deployment should retain the previous known-good image tag and a metadata backup from before high-risk upgrades.

Code rollback is only safe when the previous version can understand the current metadata state. Release notes must call out changes that make downgrade unsafe.

See [migration.md](migration.md) for the operational procedure.
