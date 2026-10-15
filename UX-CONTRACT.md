# Maintenance presentation contract

This contract covers the shared maintenance panel in job and monitor details.
Business rules come from `docs/iceberg-maintenance.md`,
`docs/maintenance-monitors.md`, and the worker's `compactionTriggersFor`.
Visual ownership is recorded in `DESIGN.md`; runtime tokens remain in
`ui/styles.css`.

## Canonical UI Map

| Capability | Canonical owner | Source of truth | Allowed variants | Verification |
|---|---|---|---|---|
| Maintenance table | `ui/iceberg-maintenance.js` | Durable API inventory and worker planner | Job / monitor owner | `ui/iceberg-maintenance.test.mjs` |
| Date and number display | `ui/core/api.js` | Browser locale | Cached inventory timestamp / counts | Browser verification |

## Inventory and eligibility

- Small-file counts and thresholds remain visible with their existing labels.
- Table names have a quiet partitioning line from the inventoried Iceberg spec:
  `Partitioned · date` or `Unpartitioned`. Missing metadata shows pending
  partitioning; counts and worker state must never be used to guess the type.
- Planner counts (`compactable_files`, `compaction_groups`) explain which files
  can be rewritten together. They must not be inferred from table totals.
- Zero planner groups means no eligible file groups, not an unexecuted action.
- Stale or refreshing inventory presents eligibility as the last scan's result.
- Missing inventory or planner counts show pending eligibility, never an assumed zero.
- Worker state remains authoritative; delete thresholds and worker errors remain visible.
- Actual worker routing reasons take precedence over an inventory explanation.

## Navigation and layout

Reuse the same renderer, existing eight columns, and existing pagination in both
owner views. Keep the current table frame and internal scrolling. Explanations
must be readable in cells without relying on hover. This correction adds no
actions or state transitions. Existing panel paging stays scoped to its owner;
changing owner resets it.

## Verification

Run the UI presentation tests and API tests. Check the rendered panel at desktop
and narrow widths, including stale, pending, eligible, empty, and error inventory.
