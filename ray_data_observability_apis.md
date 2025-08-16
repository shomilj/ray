# Ray Data Observability APIs

Ray Data provides comprehensive observability through the StatsActor API and dashboard endpoints. This document explains the key APIs and how to reconstruct progress bars and operator DAGs programmatically.

## Core Components

### 1. StatsActor API

The `_StatsActor` is a Ray actor that collects and exposes dataset execution metrics across the cluster:

```python
from ray.data._internal.stats import _get_or_create_stats_actor

stats_actor = _get_or_create_stats_actor()
```

Key methods:
- `get_datasets(job_id=None)`: Returns dataset metadata including operators, state, and metrics
- `update_metrics(execution_metrics, iteration_metrics)`: Updates runtime metrics

### 2. Dashboard API

The Ray Dashboard exposes dataset information via REST endpoints:

```python
# GET /api/data/datasets/{job_id}
# Returns all datasets for a specific job with operator-level metrics
```

## Key Data Structures

### OpRuntimeMetrics
Tracks per-operator runtime metrics organized by groups:
- **Inputs**: `num_inputs_received`, `bytes_inputs_received`, etc.
- **Outputs**: `num_task_outputs_generated`, `bytes_task_outputs_generated`, etc.
- **Tasks**: `num_tasks_running`, `num_tasks_finished`, `mean_task_completion_time`
- **Object Store Memory**: `obj_store_mem_used`, `obj_store_mem_spilled`
- **Actors**: `num_alive_actors`, `num_pending_actors`

### Operator Topology
The DAG structure is captured via the `Topology` dataclass:
```python
@dataclass
class Topology:
    operators: List[Operator]  # Each operator has name, id, uuid, input_dependencies
```

## Reconstructing Progress Bars

Progress bars can be reconstructed from the execution state:

```python
# Access via StatsActor
datasets = await stats_actor.get_datasets.remote(job_id)

for dataset_id, dataset_info in datasets.items():
    # Dataset-level progress
    total_rows = dataset_info.get('total_rows', 0)
    progress = dataset_info.get('progress', 0)
    
    # Operator-level progress
    for op_name, op_info in dataset_info['operators'].items():
        op_progress = op_info.get('progress', 0)
        op_total = op_info.get('total', 0)
        queued_blocks = op_info.get('queued_blocks', 0)
```

## Reconstructing the Operator DAG

The operator DAG can be reconstructed from the topology metadata:

```python
# From StatsActor dataset metadata
topology = dataset_info.get('topology', {})
operators = topology.get('operators', [])

# Build dependency graph
dag = {}
for op in operators:
    dag[op['id']] = {
        'name': op['name'],
        'dependencies': op['input_dependencies'],
        'sub_stages': op.get('sub_stages', [])
    }
```

## Real-time Metrics Access

For real-time monitoring, combine StatsActor with Prometheus metrics:

```python
# Prometheus metrics exposed by Ray Data
# Query format: metric_name{SessionName='session_name', dataset='dataset_id', operator='op_name'}

metrics = [
    'ray_data_output_rows',        # Total rows output
    'ray_data_current_bytes',      # Current memory usage
    'ray_data_cpu_usage_cores',    # CPU allocation
    'ray_data_spilled_bytes',      # Spilled data
]
```

## Example: Building a Custom Progress Monitor

```python
import asyncio
import ray
from ray.data._internal.stats import _get_or_create_stats_actor

async def monitor_dataset_progress(dataset_id, job_id):
    stats_actor = _get_or_create_stats_actor()
    
    while True:
        datasets = await stats_actor.get_datasets.remote(job_id)
        
        if dataset_id in datasets:
            info = datasets[dataset_id]
            
            # Display overall progress
            print(f"Dataset: {dataset_id}")
            print(f"State: {info['state']}")
            print(f"Progress: {info['progress']}/{info['total_rows']} rows")
            
            # Display operator progress
            for op_name, op_data in info['operators'].items():
                print(f"  {op_name}: {op_data['progress']}/{op_data['total']} blocks")
                print(f"    Queued: {op_data['queued_blocks']}")
                
                # Access runtime metrics
                if 'num_tasks_running' in op_data:
                    print(f"    Running tasks: {op_data['num_tasks_running']}")
        
        await asyncio.sleep(1)
```

## Key Considerations

1. **Metrics Collection**: Enable metrics collection in DataContext:
   ```python
   ctx = ray.data.DataContext.get_current()
   ctx.enable_progress_bars = True
   ctx.enable_get_object_locations_for_metrics = True  # For spill metrics
   ```

2. **Performance**: The StatsActor updates metrics every 5 seconds by default
3. **Operator Fusion**: Fused operators appear as single operators in the DAG
4. **Sub-stages**: All-to-all operators may have sub-progress bars for internal stages

## Summary

Ray Data's observability APIs provide comprehensive access to execution state through:
- StatsActor for centralized metrics collection
- Dashboard REST API for external monitoring
- Prometheus metrics for time-series data
- Topology metadata for DAG reconstruction

These APIs enable building custom monitoring tools, dashboards, and debugging utilities for Ray Data applications.