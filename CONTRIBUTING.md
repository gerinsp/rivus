# Contributing

Thanks for contributing to Rivus.

## Development

Run the full test suite before opening a pull request:

```sh
go test ./...
```

Keep changes focused. For connector behavior, add or update tests near the connector package that owns the behavior.

## Pull Requests

- Describe the behavior change and why it is needed.
- Include tests for bug fixes and new behavior.
- Keep examples generic. Do not include real hosts, database names, credentials, logs, customer names, or production job configs.
- Run formatting and tests before requesting review.

## Secrets

Never commit real credentials or operational logs. Use `.env` for local values and keep public examples under `examples/` with placeholders only.
