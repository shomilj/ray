## Ray Data TUI Dashboard — Product Requirements Document (PRD)

### 1) Executive Summary
A terminal-first Ray Data dashboard that gives operators, ML/data engineers, and SREs a powerful, low-latency, glanceable, and keyboard-driven view into Ray Data pipelines. It should rival the best TUI tools (k9s, btop, htop, lazygit) in responsiveness, ergonomics, and information density, enabling rapid diagnosis of stalls, hotspots, backpressure, memory pressure, and failures—without leaving the terminal.

### 2) Goals & Non-Goals
- **Goals**
  - Real-time, glanceable progress of datasets and operators with clear statuses (RUNNING/FINISHED/FAILED).
  - Lightning-fast navigation and search across datasets/operators/nodes with minimal CPU overhead.
  - Immediate visibility into resource usage (CPU/GPU cores, object store memory), throughput (row rates), backpressure, queued blocks, and actor health.
  - Render operator DAGs (linear and dependency graph), including sub-stages.
  - Degrade gracefully when Prometheus isn’t available; work with only StatsActor.
  - Support remote clusters, multiple sessions, and large-scale jobs (hundreds of datasets and operators).
- **Non-Goals**
  - Not a full replacement for the web dashboard or Grafana; it complements them.
  - No write-path control of Ray jobs (beyond safe read-only insights). Optional lightweight actions (follow/pin/filter) are allowed.

### 3) Target Users & Personas
- **ML Engineer / Data Engineer**: Wants immediate insight into pipeline progress, performance, and bottlenecks.
- **SRE / Platform Engineer**: Monitors cluster/resource usage and detects anomalies or regressions quickly.
- **Researcher / Power User**: Prefers terminal workflows; needs fast filtering and keyboard-first navigation.

### 4) Success Metrics
- p95 refresh-to-render latency < 150ms for UI updates; typical polling 1–5s.
- Overhead on the Ray head node < 1% CPU, < 100MB RSS under heavy listing (configurable).
- Accurate progress within 5s of reality (aligned with StatsManager’s update interval).
- Handles 500+ datasets, 2,000+ operators, and 200 nodes without lagging or freezing.
- Time-to-detect a stalled/backpressured operator < 10s.

### 5) Competitive Benchmarks (TUI quality bar)
- **k9s**: command palette, contextual panes, filtering, statusline info density.
- **btop/htop**: responsive charts/sparklines, color coding, low CPU usage.
- **lazygit**: keyboard ergonomics, modal navigation, discoverability.
Adopt keybindings, status lines, help overlays, color themes, and smooth incremental updates.

### 6) Core UX & IA (Information Architecture)
- **Primary layout**: Split-pane, resizable with mouse/keys; persistent statusline.
  - Left: Datasets list (sortable, filterable, paginated) with compact progress bars and health indicators.
  - Right (tabbed views):
    - Dataset Detail (operators table + per-op progress, alerts, queued blocks, backpressure flags).
    - DAG View (ASCII/box-drawing graph; toggle linear vs dependency; sub-stages folded).
    - Metrics View (sparklines: output rows rate, CPU/GPU usage, object store memory, spills).
    - Node View (per-node task completions, bytes output, object store memory, hostnames/IPs).
    - Logs/Events (tail dataset logs, recent errors/exceptions if accessible).
- **Global statusline**: Session, cluster health, refresh interval, Prometheus status, selected dataset/operator, hints.
- **Command palette**: ‘/’ for search; ‘:’ for actions (filter: dataset/operator/node; sort; toggle views; follow dataset; export).
- **Keybindings (suggested)**: j/k (list nav), h/l (tab nav), g/G/home/end (jumps), f (filter), s (sort), p (pin/follow), r (refresh), ? (help), q (quit).
- **Discoverability**: Onboarding help overlay with essential bindings; context tooltips.
- **Theming**: Light/dark, high-contrast; 256-color and TrueColor support; minimal flicker during resize.

### 7) Feature Requirements
- **Overview (Datasets list)**
  - Columns: dataset id/name, state, progress (finished/total), total_rows, elapsed, output_rows (max), current_bytes (value/max), spilled_bytes (max), CPU/GPU (value/max).
  - Sorting: by start_time, state, progress %, output rate, memory, CPU/GPU.
  - Filtering: by job_id, dataset name prefix, state, regex, tags.
  - Quick actions: follow (auto-open detail when selected), pin (keep at top), copy id.
- **Dataset Detail**
  - Operators table: operator id/name, state, progress, total_rows, queued_blocks, backpressure flags (task/output), actors (running/pending/restarting), resource usage.
  - Alerts: stalled operators (no progress for N seconds), high memory, frequent failures.
  - Sparklines per operator (row rate, memory) with low CPU overhead.
- **DAG View**
  - ASCII/box-drawing dependency graph; fold/unfold; highlight critical path.
  - Sub-stages shown underneath operator with compact progress bars.
- **Metrics View**
  - Dataset/operator-level sparklines: `ray_data_output_rows` (rate), `ray_data_current_bytes`, `ray_data_spilled_bytes`, CPU/GPU usage cores.
  - Per-node rollups when enabled: `*_per_node{dataset,node_ip}` aggregated; ability to drill down by node.
- **Node View**
  - Per-node metrics: tasks finished, blocks output, object store memory; trendlines for recent window.
- **Logs/Events**
  - Tail Ray Data logs (if accessible); show recent errors/exceptions per dataset/operator.
- **Search/Filter Everywhere**
  - Global search across datasets/operators/nodes; fuzzy matching; query grammar (e.g., `state:RUNNING name:map`).
- **Export/Share**
  - Export current view to JSON/NDJSON; copy-as-text; optional screenshot-as-ASCII to file.
- **Resilience & Graceful Degradation**
  - If Prometheus unavailable: show progress/state and operator metadata; hide charts and keep tables functional.
  - If StatsActor missing: error banner with guidance; auto-retry; offline snapshot mode.

### 8) Data Sources & Integrations
- **Stats state (authoritative for progress)**
  - `_StatsActor.get_datasets(job_id)` (via Python or dashboard HTTP `GET /api/data/datasets/{job_id}`) for:
    - Dataset: `state`, `progress`, `total`, `total_rows`, `start_time`, `end_time`.
    - Operators: `name`, `state`, `progress`, `total`, `total_rows`, `queued_blocks`.
- **Prometheus metrics (resource/throughput)**
  - Series (tagged by `dataset,operator`): `ray_data_current_bytes`, `ray_data_spilled_bytes`, `ray_data_output_rows`, `ray_data_cpu_usage_cores`, `ray_data_gpu_usage_cores`.
  - Per-node optional: `data_*_per_node{dataset,node_ip}` (enable via DataContext).
- **Topology (operator DAG)**
  - Preferred: parse export events (`event_EXPORT_DATASET_METADATA.log`) for `Topology` (operators with `id`, `uuid`, `input_dependencies`, `sub_stages`).
  - Fallback: linear operators order from StatsActor state.

### 9) Technical Requirements
- **Implementation**
  - Language: Python 3.9+.
  - UI: Textual/urwid/curses-like library with diff rendering and async IO.
  - Data layer: Async HTTP to dashboard endpoint and Prometheus; optional direct Ray client for StatsActor when co-located.
  - Config:
    - Prometheus host/headers; refresh interval; theme; table columns; scale factors.
    - Session selection (via SessionName or discovery of running sessions).
- **Performance & Efficiency**
  - Incremental/differential rendering; coalesce updates; avoid repaint storms.
  - Poll StatsActor/dashboard every 2–5s; Prometheus queries batched; exponential backoff on errors.
  - Cache last N samples for sparklines; memory cap per dataset/operator.
- **Resiliency**
  - Timeouts and retries; clear banners on degraded modes; non-blocking UI when sources stall.
- **Security**
  - Support HTTPS endpoints; headers/tokens via env or config file; do not persist secrets by default.

### 10) Accessibility & UX Quality
- Keyboard-first; consistent hints; `?` overlay.
- High-contrast theme; colorblind-friendly palette; reduce flashing.
- Works in 80×24 terminals; responsive to resize; truncation with hover/expand.

### 11) Configuration & Deployment
- **Packaging**: `pip install ray-data-tui`; optional standalone binary (PyInstaller).
- **Runtime config** (env or file):
  - `RAY_TUI_REFRESH_INTERVAL=2`, `RAY_TUI_PROM_HOST=http(s)://...`, `RAY_TUI_SESSION=...`.
  - Toggle per-node metrics; disable charts; set max datasets/operators to display.

### 12) MVP Scope (v1.0)
- Overview (datasets) + Dataset Detail (operators) with progress/state/queued blocks.
- Basic sparklines for output rows rate and current bytes.
- Filtering/sorting; search; keybindings; help overlay.
- DAG linear view (list); dependency graph optional (if exporter detected).
- Prometheus optional; graceful degradation.

### 13) Post-MVP Roadmap
- Full dependency DAG with sub-stages; critical path highlighting.
- Per-node drilldown with heatmaps; node saturation alerts.
- Bookmarks, saved filters; workspace profiles.
- Notifications (desktop/system bell) on failures/stalls.
- Plugin APIs for custom metrics panels.

### 14) Risks & Mitigations
- **Large-scale metrics load**: Batch PromQL, rate-limit polls, cache results; user-tunable sampling.
- **Terminal rendering performance**: Use diff rendering and minimal redraw; limit sparkline length.
- **API drift in Ray**: Version checks; use dashboard HTTP where possible; adapters for StatsActor fields.
- **No Prometheus**: Degrade to progress-only mode; banner guidance.

### 15) Dependencies
- Ray (matching version with `_StatsActor` and dashboard `/api/data/datasets` endpoint).
- Optional Prometheus; network reachability.
- Python TUI lib (Textual/urwid) and `requests`/`httpx`.

### 16) Acceptance Criteria
- With Prometheus down, the TUI still shows accurate progress/state for active datasets/operators within 5s.
- With 300 datasets and 1,200 operators, UI remains responsive (key input echo < 100ms), CPU overhead < 1% on head node, app RSS < 100MB.
- User can filter datasets by `state:RUNNING` and sort by output rate.
- DAG view shows at least linear operator chain; dependency view when exporter is enabled.
- Backpressure indicator appears within 5s when `task_submission` or `task_output` backpressure is toggled for any operator.

### 17) Open Questions
- Should we add a lightweight WebSocket gateway proxy for push updates, or stick with polling?
- Is direct Ray client access expected for remote users, or should we mandate the dashboard HTTP path?
- How to authenticate to the dashboard API in secured clusters (mutual TLS, tokens) in a general way?

### 18) Glossary
- **StatsActor**: Ray actor aggregating dataset/operator progress and metrics, backing dashboard views.
- **Prometheus metrics**: `ray_data_*` series tagged by dataset/operator for observability.
- **Topology export**: Structured operator DAG export written to `event_EXPORT_DATASET_METADATA.log`.