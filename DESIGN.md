# Flink SQL Lineage Design

## Visual Direction

Use a dark developer-tool interface inspired by the VoltAgent DESIGN.md pattern: near-black canvas, precise hairline borders, a single electric green accent, calm Inter/system typography, and dense product surfaces. This is an operational data lineage app, not a marketing site.

## Tokens

### Colors

- `--lineage-canvas`: `#101010`
- `--lineage-canvas-soft`: `#151515`
- `--lineage-surface`: `#1a1a1a`
- `--lineage-surface-raised`: `#202020`
- `--lineage-hairline`: `#333634`
- `--lineage-hairline-soft`: `#262826`
- `--lineage-accent`: `#00d992`
- `--lineage-accent-soft`: `#2fd6a1`
- `--lineage-accent-deep`: `#10b981`
- `--lineage-ink`: `#f2f2f2`
- `--lineage-ink-strong`: `#ffffff`
- `--lineage-body`: `#bdbdbd`
- `--lineage-muted`: `#8b949e`
- `--lineage-danger`: `#ef4444`
- `--lineage-warning`: `#f59e0b`
- `--lineage-info`: `#3b82f6`

### Typography

- Sans: `Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`
- Mono: `SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace`
- App shell labels: 13-14px, weight 500-600.
- Page titles: 20-24px, weight 650-700.
- Table and graph labels: 13-14px, compact line height.
- Do not use negative letter spacing in the app shell.

### Shape And Spacing

- Base spacing unit: 4px.
- Buttons and inputs: 6px radius.
- Panels and cards: 8px radius.
- Pills only for status chips.
- Use 1px hairline borders instead of heavy shadows.
- Keep repeated operational surfaces compact and scan-friendly.

## Component Rules

### App Shell

- Sidebar uses `--lineage-canvas` with a subtle right hairline.
- Active nav row uses a soft green background tint, green text, and a left accent bar.
- Header stays dark and compact, with icon buttons as dark bordered squares.
- Footer is dark, low-contrast, and unobtrusive.

### Operational Panels

- Main workspace uses `--lineage-canvas-soft`.
- List/table/detail regions use `--lineage-surface` or white only where third-party graph readability requires it.
- Panel dividers use `--lineage-hairline-soft`.
- Avoid floating marketing cards and ornamental gradients.

### Buttons, Inputs, And Tabs

- Primary actions use electric green with black text.
- Secondary actions use dark surfaces with hairline borders.
- Active tabs use green text and a green underline.
- Inputs retain Ant Design behavior but use dark chrome where practical.

### Graph And Lineage Canvas

- Preserve graph readability first.
- The graph viewport may remain light if edge and node colors depend on a light canvas.
- Surround graph tools, top metadata, and split panes with the dark shell tokens.

## Do

- Make it feel like a professional data engineering tool.
- Use green sparingly for active, healthy, focused, and primary states.
- Use mono typography for SQL, IDs, connector names, and compact metadata.
- Keep density high, especially in tables, tree lists, and catalog detail views.

## Do Not

- Do not turn this into a landing page.
- Do not add decorative gradient orbs, hero art, or oversized marketing headings.
- Do not introduce extra bright accent colors beyond semantic states.
- Do not make every surface green.
- Do not sacrifice lineage graph readability for dark mode purity.
