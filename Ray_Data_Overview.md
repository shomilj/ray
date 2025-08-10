### Ray overview (tasks/actors model)

- Ray executes remote work as:
  - Tasks: stateless functions scheduled across the cluster; called with f.remote(...); return ObjectRef futures.
  - Actors: stateful, long-lived workers; class instances created remotely with Class.remote(...); remote methods keep state across calls.
- Scheduling/resources: each task/actor requests resources (num_cpus, num_gpus, memory, custom). The object store holds immutable results; ray.get fetches results to the driver.
- Concurrency: scale by launching more tasks or actor workers, not by threads/async on the driver. Within Ray Data, transforms run as Ray tasks or actor pools.

### Ray Data: technical overview

- A `Dataset` is a lazy, distributed pipeline of block-wise transformations. Execution is triggered by consumption (e.g., `take`, `iter`, `write_*`, `count`).
- Blocks are Arrow/Pandas/Numpy backed. Batch UDFs support formats: numpy dict, pandas DataFrame, pyarrow Table.
- Scaling knobs: number of blocks (read override_num_blocks, `repartition()`), vectorized `map_batches`, and task/actor `concurrency`.

### Quickstart mini example

```python
import numpy as np, ray
ray.init()

# Read + transform + iterate + write
ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

def add_area(batch):  # pandas DataFrame
    batch["petal_area"] = batch["petal.length"] * batch["petal.width"]
    return batch

out = ds.map_batches(add_area, batch_format="pandas").iter_batches(batch_size=1024)
for b in out:
    pass

ds.write_parquet("local:///tmp/iris_out/")
```

---

### Dataset creation and input APIs

- In-memory/synthetic
  - `ray.data.from_items(items, override_num_blocks=None)`
    - items: list[Any|dict] (scalars get column "item").
  - `ray.data.range(n, concurrency=None, override_num_blocks=None)`
  - `ray.data.range_tensor(n, shape=(1,), concurrency=None, override_num_blocks=None)`

- Common read_* kwargs (apply to most readers)
  - `filesystem`, `ray_remote_args`, `partition_filter`, `partitioning`, `include_paths`, `ignore_missing_paths`, `shuffle`, `file_extensions`, `concurrency`, `override_num_blocks`.

- Selected file/object readers (see docstrings for full args)
  - `ray.data.read_parquet(paths, columns=None, tensor_column_schema=None, partition_filter=None, partitioning=Partitioning("hive"), shuffle=None, include_paths=False, file_extensions=None, concurrency=None, override_num_blocks=None, ray_remote_args=None, **arrow_parquet_args)`
  - `ray.data.read_csv(paths, arrow_open_stream_args=None, partition_filter=None, partitioning=Partitioning("hive"), include_paths=False, ignore_missing_paths=False, shuffle=None, file_extensions=None, concurrency=None, override_num_blocks=None, ray_remote_args=None, **arrow_csv_args)`
  - `ray.data.read_json(paths, lines=False, arrow_open_stream_args=None, partition_filter=None, partitioning=Partitioning("hive"), include_paths=False, ignore_missing_paths=False, shuffle=None, file_extensions=None, concurrency=None, override_num_blocks=None, ray_remote_args=None, **arrow_json_args)`
  - `ray.data.read_text(paths, encoding="utf-8", drop_empty_lines=True, arrow_open_stream_args=None, partition_filter=None, partitioning=None, include_paths=False, ignore_missing_paths=False, shuffle=None, file_extensions=None, concurrency=None, override_num_blocks=None, ray_remote_args=None)`
  - `ray.data.read_images(paths, size=None, mode=None, include_paths=False, ignore_missing_paths=False, shuffle=None, file_extensions=None, concurrency=None, override_num_blocks=None, ray_remote_args=None)`
  - `ray.data.read_audio(paths, arrow_open_stream_args=None, partition_filter=None, partitioning=None, include_paths=False, ignore_missing_paths=False, file_extensions=AudioDatasource._FILE_EXTENSIONS, shuffle=None, concurrency=None, override_num_blocks=None, ray_remote_args=None)`
  - `ray.data.read_videos(paths, arrow_open_stream_args=None, partition_filter=None, partitioning=None, include_paths=False, include_timestamps=False, ignore_missing_paths=False, file_extensions=VideoDatasource._FILE_EXTENSIONS, shuffle=None, concurrency=None, override_num_blocks=None, ray_remote_args=None)`
  - `ray.data.read_binary_files(paths, include_paths=False, arrow_open_stream_args=None, partition_filter=None, partitioning=None, ignore_missing_paths=False, shuffle=None, file_extensions=None, concurrency=None, override_num_blocks=None, ray_remote_args=None)`
  - `ray.data.read_numpy(paths, arrow_open_stream_args=None, partition_filter=None, partitioning=None, include_paths=False, ignore_missing_paths=False, shuffle=None, file_extensions=NumpyDatasource._FILE_EXTENSIONS, concurrency=None, override_num_blocks=None, **numpy_load_args)`
  - `ray.data.read_tfrecords(paths, arrow_open_stream_args=None, partition_filter=None, include_paths=False, ignore_missing_paths=False, tf_schema=None, shuffle=None, file_extensions=None, tfx_read_options=None, concurrency=None, override_num_blocks=None, ray_remote_args=None)`
  - `ray.data.read_webdataset(paths, decoder=True, fileselect=None, filerename=None, suffixes=None, verbose_open=False, shuffle=None, include_paths=False, file_extensions=None, expand_json=False, concurrency=None, override_num_blocks=None, arrow_open_stream_args=None, filesystem=None)`

- Cloud/DB readers (selection)
  - `ray.data.read_mongo(uri, database, collection, pipeline=None, schema=None, concurrency=None, override_num_blocks=None, ray_remote_args=None, **mongo_args)`
  - `ray.data.read_bigquery(project_id, dataset=None, query=None, concurrency=None, override_num_blocks=None, ray_remote_args=None)`

- Custom datasource
  - `ray.data.read_datasource(datasource, parallelism=-1, ray_remote_args=None, concurrency=None, override_num_blocks=None, **read_args)` — implement `Datasource.get_read_tasks()` and `estimate_inmemory_data_size()`.

---

### Core transforms on Dataset

- Per-row / per-batch
  - `ds.map(fn, compute=None, fn_args=None, fn_kwargs=None, fn_constructor_args=None, fn_constructor_kwargs=None, num_cpus=None, num_gpus=None, memory=None, concurrency=None, ray_remote_args_fn=None, **ray_remote_args)`
  - `ds.map_batches(fn, batch_size=None, compute=None, batch_format="default", zero_copy_batch=False, fn_args=None, fn_kwargs=None, fn_constructor_args=None, fn_constructor_kwargs=None, num_cpus=None, num_gpus=None, memory=None, concurrency=None, ray_remote_args_fn=None, **ray_remote_args)`
    - If using GPUs, you must set `batch_size`.
  - `ds.flat_map(fn, compute=None, fn_args=None, fn_kwargs=None, fn_constructor_args=None, fn_constructor_kwargs=None, num_cpus=None, num_gpus=None, memory=None, concurrency=None, ray_remote_args_fn=None, **ray_remote_args)`

- Column operations
  - `ds.add_column(col, fn, batch_format="pandas", compute=None, concurrency=None, **ray_remote_args)`
  - `ds.drop_columns(cols, compute=None, concurrency=None, **ray_remote_args)`
  - `ds.select_columns(cols, compute=None, concurrency=None, **ray_remote_args)`
  - `ds.rename_columns(names, concurrency=None, **ray_remote_args)`
  - `ds.with_columns(exprs: dict[str, Expr])`

- Filter / sample
  - `ds.filter(fn=None, expr=None, compute=None, fn_args=None, fn_kwargs=None, fn_constructor_args=None, fn_constructor_kwargs=None, concurrency=None, ray_remote_args_fn=None, **ray_remote_args)`
  - `ds.random_sample(fraction: float, seed=None)`

- Grouping & aggregations
  - `ds.groupby(key: str|list[str]|None, num_partitions=None) -> GroupedData`
  - `grouped.map_groups(...)`
  - `grouped.aggregate(*aggs)`
  - Built-ins: `Sum`, `Min`, `Max`, `Mean`, `Std(ddof=1)`, `Count`, `Unique`, `Quantile(q)`, `AbsMax`.
  - Dataset-wide convenience: `ds.sum/min/max/mean/std`, `ds.unique(column)`.

- Sorting, shuffling, repartitioning
  - `ds.sort(by, descending=False)`
  - `ds.random_shuffle(seed=None)`
  - `ds.randomize_block_order(seed=None)`
  - `ds.repartition(num_blocks=None, target_num_rows_per_block=None, shuffle=False, keys=None, sort=False)`

- Joins / unions
  - `ds.join(ds2, join_type, num_partitions, on=("id",), right_on=None, left_suffix=None, right_suffix=None, partition_size_hint=None, aggregator_ray_remote_args=None, validate_schemas=False)`
  - `ds.union(*others)`

- Splits
  - `ds.streaming_split(n, equal=False, locality_hints=None) -> list[DataIterator]`
  - `ds.split(n, equal=False, locality_hints=None) -> list[MaterializedDataset]`
  - `ds.split_at_indices(indices)`, `ds.split_proportionately(proportions)`
  - `ds.train_test_split(test_size: float|int, shuffle=False, seed=None, stratify: Optional[str])`

---

### Consumption, iteration, stats

- Quick access
  - `ds.take(limit=20)`, `ds.take_all(limit=None)`, `ds.take_batch(batch_size=20, batch_format="default")`, `ds.show(limit=20)`, `ds.count()`
- Metadata
  - `ds.schema(fetch_if_missing=True)`, `ds.columns(fetch_if_missing=True)`, `ds.size_bytes()`, `ds.input_files()`
- Iterators & DL integration
  - `it = ds.iterator()`
  - `it.iter_batches(prefetch_batches=1, batch_size=256, batch_format="default", drop_last=False, local_shuffle_buffer_size=None, local_shuffle_seed=None)`
  - `it.iter_rows()`
  - `it.iter_torch_batches(prefetch_batches=1, batch_size=256, dtypes=None, device="auto", collate_fn=None, drop_last=False, local_shuffle_buffer_size=None, local_shuffle_seed=None, pin_memory=False)`
  - `it.iter_tf_batches(prefetch_batches=1, batch_size=256, dtypes=None, drop_last=False, local_shuffle_buffer_size=None, local_shuffle_seed=None)`
  - `it.to_torch(...)`, `it.to_tf(...)`, `it.materialize()`

---

### Output and sinks (writes)

- Common: file count ≈ number of blocks; adjust via `repartition()`. All writers accept `concurrency` and `ray_remote_args`.

- Files/object store
  - `ds.write_parquet(path, partition_cols=None, filesystem=None, try_create_dir=True, arrow_open_stream_args=None, filename_provider=None, arrow_parquet_args_fn=None, min_rows_per_file=None, max_rows_per_file=None, ray_remote_args=None, concurrency=None, mode="append", **arrow_parquet_args)`
  - `ds.write_csv(path, filesystem=None, try_create_dir=True, arrow_open_stream_args=None, filename_provider=None, arrow_csv_args_fn=None, min_rows_per_file=None, ray_remote_args=None, concurrency=None, mode="append", **arrow_csv_args)`
  - `ds.write_json(path, filesystem=None, try_create_dir=True, arrow_open_stream_args=None, filename_provider=None, pandas_json_args_fn=None, min_rows_per_file=None, ray_remote_args=None, concurrency=None, mode="append", **pandas_json_args)`
  - `ds.write_tfrecords(path, tf_schema=None, filesystem=None, try_create_dir=True, arrow_open_stream_args=None, filename_provider=None, min_rows_per_file=None, ray_remote_args=None, concurrency=None, mode="append")`
  - `ds.write_images(path, column, file_format="png", filesystem=None, try_create_dir=True, arrow_open_stream_args=None, filename_provider=None, ray_remote_args=None, concurrency=None, mode="append")`
  - `ds.write_numpy(path, column, filesystem=None, try_create_dir=True, arrow_open_stream_args=None, filename_provider=None, min_rows_per_file=None, ray_remote_args=None, concurrency=None, mode="append")`
  - `ds.write_webdataset(path, filesystem=None, try_create_dir=True, arrow_open_stream_args=None, filename_provider=None, min_rows_per_file=None, ray_remote_args=None, encoder=True, concurrency=None, mode="append")`
  - `ds.write_lance(path, schema=None, mode={"create","append","overwrite"}="create", min_rows_per_file=1_048_576, max_rows_per_file=67_108_864, data_storage_version=None, storage_options=None, ray_remote_args=None, concurrency=None)`

- Databases/warehouses
  - `ds.write_sql(sql, connection_factory, ray_remote_args=None, concurrency=None)`
  - `ds.write_snowflake(table, connection_parameters: dict, ray_remote_args=None, concurrency=None)`
  - `ds.write_mongo(uri, database, collection, ray_remote_args=None, concurrency=None)`
  - `ds.write_bigquery(project_id, dataset, max_retry_cnt=10, overwrite_table=True, ray_remote_args=None, concurrency=None)`
  - `ds.write_clickhouse(table, dsn, mode=SinkMode.CREATE|APPEND|OVERWRITE, schema=None, client_settings=None, client_kwargs=None, table_settings=None, max_insert_block_rows=None, ray_remote_args=None, concurrency=None)`
  - `ds.write_iceberg(table_identifier, catalog_kwargs=None, snapshot_properties=None, ray_remote_args=None, concurrency=None)`

- Custom sink
  - `ds.write_datasink(datasink, ray_remote_args=None, concurrency=None)` — implement `Datasink` (`write`, `on_write_start/complete/failed`, `get_name`, `supports_distributed_writes`).

---

### Concise examples per feature

- Batch UDF with zero-copy, NumPy format
```python
def norm(batch):  # Dict[str, np.ndarray]
    x = batch["features"].astype(np.float32)
    batch["features"] = (x - x.mean(0)) / (x.std(0) + 1e-6)
    return batch

ds2 = ds.map_batches(norm, batch_format="numpy", zero_copy_batch=False)
```

- Stateful GPU inference with actor pool
```python
class TorchPredictor:
    def __init__(self):
        import torch, torch.nn as nn
        self.model = nn.Identity().cuda(); self.model.eval()
    def __call__(self, batch):
        import torch
        t = torch.as_tensor(batch["data"], dtype=torch.float32).cuda()
        with torch.inference_mode(): batch["out"] = self.model(t).cpu().numpy()
        return batch

pred = ds.map_batches(TorchPredictor, concurrency=2, num_gpus=1, batch_size=128)
```

- Groupby aggregations
```python
from ray.data.aggregate import Sum, Mean, Count
ds.groupby("user_id").aggregate(Sum(on="amount"), Mean(on="amount"), Count())
```

- Join
```python
joined = left.join(right, join_type="inner", num_partitions=32, on=("id",))
```

- Streaming ingest for training
```python
it_train, it_val = ds.streaming_split(2, equal=True)
for batch in it_train.iter_torch_batches(batch_size=128, local_shuffle_buffer_size=10_000):
    pass
```

- Efficient reads with pushdown
```python
import pyarrow as pa
ds = ray.data.read_parquet(
  "s3://.../data.parquet",
  columns=["a","b","label"],
  filter=pa.dataset.field("dt") >= "2025-01-01",
)
```

---

### Concurrency and asyncio with Ray Data

- Prefer Ray concurrency primitives over asyncio:
  - Increase parallelism via more blocks (read `override_num_blocks`, `repartition()`), allow Ray to schedule more map tasks.
  - Use `map_batches(..., concurrency=n)` with function UDFs to cap concurrent tasks.
  - Use callable classes with `concurrency=n` (exact actor pool) or `(min,max)` (autoscaling) for stateful workers (e.g., GPU models).
  - Control per-worker resources: `num_cpus`, `num_gpus`, `memory`; per-worker remote args via `ray_remote_args_fn`.
- Async in UDFs:
  - Tasks/actors are separate processes; you can `asyncio.run()` inside UDFs if needed, but avoid relying on the driver’s event loop. Prefer Ray tasks/actors over manual asyncio for parallelism.
- Iterator-side overlap:
  - `iter_batches(prefetch_batches>0)` overlaps deserialization/formatting/collate; tune `batch_size`, `prefetch_batches`, `local_shuffle_buffer_size`.

---

### Best practices and performance tips

- Vectorize: prefer `map_batches` over `map` for NumPy/pandas operations.
- Tune read parallelism: use `override_num_blocks` on read_*; or `repartition()` to control file count and downstream parallelism.
- I/O concurrency: increase read parallelism via `ray_remote_args={"num_cpus": 0.25}` to allow >1 read per CPU.
- Memory/OOM:
  - Keep rows reasonably small (<10MB). Tune `map_batches(batch_size)` so one output batch fits in RAM/GPU.
  - Use `zero_copy_batch=True` when not mutating inputs; prefer `batch_format="pyarrow"` to reduce copying.
  - Avoid spilling by increasing output blocks or processing smaller chunks; reduce batch size if needed.
- Shuffles: expensive; use iterator `local_shuffle_buffer_size` for stochasticity during training instead of global shuffle.
- Pushdown/pruning: pass `columns` and `filter` to `read_parquet`; prefer `filter(expr=...)`.
- GPU inference: class UDF + `num_gpus=1` + tuned `batch_size`; move tensors to/from device inside `__call__`.
- Writing: set `min_rows_per_file`/`max_rows_per_file`, `mode="overwrite"|"append"`; use `filename_provider` for custom naming.
- Data locality: `streaming_split(equal=True)` distributes evenly; optionally pass `locality_hints` with node IDs.
- Execution config: tune via `DataContext` (target_max_block_size, resource limits, preserve_order, locality_with_output).