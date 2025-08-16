## Ray Data progress and observability APIs (TUI + Dashboard)

### Components (how it works)
- **_StatsActor**:
  - Holds in-memory dataset/operator progress: `state`, `progress`, `total`, `total_rows`, `start_time`, `end_time`, per-operator `queued_blocks`.
  - Emits Prometheus series tagged by `dataset` and `operator` (and `node_ip` if enabled).
  - Exposes `get_datasets(job_id)` for the dashboard.
- **StatsManager**:
  - Aggregates `OpRuntimeMetrics` from operators and pushes snapshots to `_StatsActor` every ~5s.
- **StreamingExecutor**:
  - Registers dataset + operator tags; periodically calls `StatsManager.update_execution_metrics(...)`.
  - Builds console/TUI progress bars from operator state and refreshes them.
- **Dashboard Data module**:
  - HTTP route `GET /api/data/datasets/{job_id}` merges `_StatsActor.get_datasets` with Prometheus queries for a UI-ready view.

### Key APIs you can call

- **Dashboard HTTP (recommended for progress bars)**
  - `GET /api/data/datasets/{job_id}`
  - Returns recent datasets for that job with dataset-level and operator-level progress + selected Prom metrics.
```json
{
  "datasets": [
    {
      "dataset": "MyDataset_123",
      "job_id": "01000000",
      "start_time": 1734040000,
      "end_time": 1734040123,
      "state": "FINISHED",
      "progress": 42,
      "total": 42,
      "total_rows": 1000000,
      "ray_data_output_rows": { "max": 1000000 },
      "ray_data_spilled_bytes": { "max": 0 },
      "ray_data_current_bytes": { "value": 0, "max": 0 },
      "ray_data_cpu_usage_cores": { "value": 0, "max": 32 },
      "ray_data_gpu_usage_cores": { "value": 0, "max": 0 },
      "operators": [
        {
          "operator": "ReadParquet_0",
          "name": "ReadParquet",
          "queued_blocks": 0,
          "state": "FINISHED",
          "progress": 20,
          "total": 20,
          "total_rows": 1000000,
          "ray_data_current_bytes": { "value": 0, "max": 0 },
          "ray_data_spilled_bytes": { "max": 0 },
          "ray_data_output_rows": { "max": 1000000 },
          "ray_data_cpu_usage_cores": { "value": 0, "max": 16 },
          "ray_data_gpu_usage_cores": { "value": 0, "max": 0 }
        }
      ]
    }
  ]
}
```

- **Stats actor (Python; internal but useful for tooling)**
  - Get datasets: `ray.data._internal.stats._get_or_create_stats_actor().get_datasets.remote(job_id)`
  - Contains progress counters and per-operator state/queued blocks without Prom metrics.
  - Stats push interval: ~5s.

- **Prometheus series (for charts/overlays)**
  - Names (Ray prefixes them as `ray_*`):  
    - `ray_data_current_bytes`, `ray_data_spilled_bytes`, `ray_data_output_rows`, `ray_data_cpu_usage_cores`, `ray_data_gpu_usage_cores`
  - Tags: `dataset`, `operator` (and `node_ip` for per-node series like `ray_data_num_tasks_finished_per_node`)
  - Example PromQL:
```text
sum(ray_data_current_bytes{SessionName="YOUR_SESSION"}) by (dataset, operator)
sum(ray_data_cpu_usage_cores{SessionName="YOUR_SESSION"}) by (dataset, operator)
sum(rate(ray_data_output_rows{SessionName="YOUR_SESSION"}[1m])) by (dataset, operator)
```

- **Operator DAG export (Topology)**
  - Exported when the export API is enabled; each event includes:
    - Operators: `name`, `id` (e.g., `ReadParquet_0`), `uuid`, `input_dependencies` (edges), `sub_stages`, sanitized `args`.
  - Enable via env: set either `RAY_ENABLE_EXPORT_API_WRITE=1`, or `RAY_ENABLE_EXPORT_API_WRITE_CONFIG=EXPORT_DATASET_METADATA`.
  - Events are written to: `<session_dir>/logs/export_events/event_EXPORT_DATASET_METADATA.log`.

### Reconstructing progress bars

- **Dataset/global bar**
  - Source: `GET /api/data/datasets/{job_id}`.
  - Render:  
    - Finished = `progress`, Total = `total`.  
    - If `state == "RUNNING"`, show Running = `total - progress`.  
    - If `state == "FAILED"`, treat remaining as canceled/failed.
  - Show throughput: use `ray_data_output_rows.max` delta over time if desired.

- **Operator bars**
  - For each `operators[]` entry: same logic as dataset bar.
  - Add badges from fields: `queued_blocks`, `state`.
  - Overlay resource usage: `ray_data_current_bytes.value/max`, `ray_data_spilled_bytes.max`, CPU/GPU cores.

- **Update frequency**
  - Poll the HTTP endpoint every ~2–5s. Progress can update faster; Prom metrics may lag up to ~5s.

### Reconstructing the operator DAG

- **Lightweight (ordered list)**
  - Use the `operators[]` array returned by the HTTP endpoint (topologically ordered).
  - Each entry’s `operator` ID follows `Name_index` and matches metric tags.

- **Full DAG with edges and sub-stages**
  - Parse the dataset metadata export log (`event_EXPORT_DATASET_METADATA.log`).
  - Build nodes from `operators` with their `id` and edges from `input_dependencies`.
  - Join with runtime state/metrics by matching the operator `id` (same identifier used in dashboard/Prom tags).

### Configuration switches

- **UI/console behavior**
  - `DataContext.get_current().enable_progress_bars`
  - `DataContext.get_current().enable_operator_progress_bars`
  - `DataContext.get_current().enable_progress_bar_name_truncation`

- **Metrics detail**
  - `DataContext.get_current().enable_get_object_locations_for_metrics` (required for spill tracking)
  - `DataContext.get_current().enable_per_node_metrics` (emits `..._per_node{dataset,node_ip}` series)

### Minimal usage examples

- **Call the dashboard API**
```bash
curl -s "http://<ray-dashboard-host>:8265/api/data/datasets/<job_id>" | jq .
```

- **Fetch directly from the stats actor (Python)**
```python
import ray
from ray.data._internal.stats import _get_or_create_stats_actor
ray.init()
actor = _get_or_create_stats_actor()
datasets = ray.get(actor.get_datasets.remote("<job_id>"))
```

- **PromQL (per-operator current bytes)**
```text
sum(ray_data_current_bytes{SessionName="YOUR_SESSION"}) by (dataset, operator)
```

- **Topology export (enable and locate)**
```bash
# At Ray startup
export RAY_ENABLE_EXPORT_API_WRITE=1
# or: export RAY_ENABLE_EXPORT_API_WRITE_CONFIG=EXPORT_DATASET_METADATA

# Find events
ls "$(ray status --format=json | jq -r .session_dir)/logs/export_events"/event_EXPORT_DATASET_METADATA.log
```