---
version: alpha
name: Rivus Monitor
description: An operations console for inspecting ingestion jobs and Iceberg maintenance.
colors:
  primary: "#0b65a3"
  background: "#eef2f6"
  surface: "#ffffff"
  border: "#d7dde7"
  text: "#17202f"
  muted: "#5d6b82"
typography:
  sans:
    fontFamily: '"IBM Plex Sans", ui-sans-serif, system-ui, sans-serif'
  mono:
    fontFamily: '"IBM Plex Mono", ui-monospace, monospace'
rounded:
  card: "3px"
omitted:
  - section: spacing
    reason: Existing Tailwind utilities own local table spacing; this change introduces no spacing tokens.
  - section: components
    reason: Component ownership and maintenance presentation are documented below and in UX-CONTRACT.md.
---

## Overview

Rivus is a product console for operators checking pipeline health and table
maintenance. Preserve its existing Flink-style console: dense tables, quiet
surfaces, and monospaced identifiers and counts. English UI copy follows the
existing interface; dates and numbers use the browser locale. No Japan-specific
market or locale behavior is established by this workflow.

For maintenance corrections, use existing columns and short factual explanations.
Avoid new banners, cards, decorations, or a redesign for a clarification of counts.

## Colors

Runtime values remain owned by `ui/styles.css`, not generated from this file.
The frontmatter mirrors `--brand-600`, `--app-bg`, `--surface`, `--line`, `--text`,
and `--muted`, respectively. Existing status utilities retain their semantic tones.

## Typography

`body` uses IBM Plex Sans; `.mono` uses IBM Plex Mono with the existing system
fallbacks in `ui/styles.css`. Keep table numbers aligned and explanatory text quiet.

## Layout

Reuse `renderIcebergMaintenance` for job and monitor details. Its table owns its
existing overflow container; surrounding pages retain document scrolling.

## Elevation & Depth

Use the existing `.panel` border and subtle shadow. Informational text needs no
additional surface or elevation.

## Shapes

The canonical `.stat-card` radius is 3px. Preserve existing table and status shapes.

## Components

`ui/iceberg-maintenance.js` owns maintenance table rendering. `ui/core/api.js`
owns escaping, date/number formatting, and stable HTML updates. See
`UX-CONTRACT.md` for inventory and eligibility semantics.

## Do's and Don'ts

Keep total counts and configured triggers visible. Explain planner eligibility
separately in the existing reason column. Never equate a table-wide threshold
with a guarantee that files from different partitions can be merged.
