# Ray Data: Comprehensive Technical Overview

## Ray Framework Overview

Ray is a distributed computing framework that enables scaling Python applications across clusters. At its core, Ray provides two fundamental abstractions:

### Task/Actor Model

**Tasks** are stateless functions executed asynchronously across the cluster:
- Decorated with `@ray.remote`
- Called with `.remote()` instead of normal invocation
- Return futures (object references) that can be retrieved with `ray.get()`
- Executed by Ray workers in parallel

```python
@ray.remote
def process_data(data):
    return data * 2

# Parallel execution
futures = [process_data.remote(i) for i in range(10)]
results = ray.get(futures)
```

**Actors** are stateful workers that maintain internal state between method calls:
- Classes decorated with `@ray.remote`
- Instantiated with `.remote()` creating a dedicated worker process
- Methods execute serially, preserving state consistency
- Useful for managing models, connections, or stateful processing

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0
    
    def increment(self):
        self.value += 1
        return self.value

counter = Counter.remote()
ray.get([counter.increment.remote() for _ in range(5)])  # Returns [1, 2, 3, 4, 5]
```

### Ray's Distributed Object Store
- Efficiently manages data across the cluster
- Objects created implicitly (function returns) or explicitly (`ray.put()`)
- Zero-copy reads within same node
- Automatic memory management and spilling to disk

## Ray Data Core Concepts

### Dataset Architecture
- **Dataset**: Distributed collection of data, lazy-evaluated API
- **Blocks**: Partitions of data stored in object store (Arrow/Pandas format)
- **Operators**: Logical (what to do) and Physical (how to execute)
- **Streaming Execution**: Pipelined processing without materializing full dataset

### Execution Model
1. **Logical Plan**: High-level operations built as you chain methods
2. **Physical Plan**: Optimized execution strategy with operator fusion
3. **Streaming Pipeline**: Concurrent execution of multiple stages
4. **Block-level Parallelism**: Each block processed independently

## Complete API Reference

### Data Loading APIs

#### read_csv
```python
ray.data.read_csv(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **arrow_csv_args,
) -> Dataset
```

#### read_parquet
```python
ray.data.read_parquet(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **arrow_parquet_args,
) -> Dataset
```

#### read_json
```python
ray.data.read_json(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **arrow_json_args,
) -> Dataset
```

#### read_binary_files
```python
ray.data.read_binary_files(
    paths: Union[str, List[str]],
    *,
    include_paths: bool = False,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset
```

#### Creating from Python Objects
```python
# From items
ray.data.from_items(items: List[Any], *, parallelism: int = -1) -> Dataset

# From pandas
ray.data.from_pandas(dfs: Union[pd.DataFrame, List[pd.DataFrame]]) -> Dataset

# From NumPy
ray.data.from_numpy(ndarrays: Union[np.ndarray, List[np.ndarray]]) -> Dataset

# From Arrow
ray.data.from_arrow(tables: Union[pyarrow.Table, List[pyarrow.Table]]) -> Dataset

# Range datasets
ray.data.range(n: int, *, parallelism: int = -1) -> Dataset
ray.data.range_tensor(n: int, *, shape: Tuple[int, ...], parallelism: int = -1) -> Dataset
```

### Transformation APIs

#### map
```python
Dataset.map(
    fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    *,
    compute: Optional[ComputeStrategy] = None,
    fn_args: Optional[Iterable[Any]] = None,
    fn_kwargs: Optional[Dict[str, Any]] = None,
    fn_constructor_args: Optional[Iterable[Any]] = None,
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    memory: Optional[float] = None,
    concurrency: Optional[Union[int, Tuple[int, int]]] = None,
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    **ray_remote_args,
) -> Dataset
```

Example:
```python
# Row-wise transformation
def transform_row(row: Dict[str, Any]) -> Dict[str, Any]:
    row["new_col"] = row["col1"] * 2
    return row

ds = ray.data.read_csv("data.csv")
transformed = ds.map(transform_row)

# Using a class for stateful transformations
class ModelPredictor:
    def __init__(self):
        self.model = load_model()
    
    def __call__(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["prediction"] = self.model.predict(row["features"])
        return row

predictions = ds.map(ModelPredictor, concurrency=4)
```

#### map_batches
```python
Dataset.map_batches(
    fn: Callable[[DataBatch], DataBatch],
    *,
    batch_size: Union[int, None, Literal["default"]] = None,
    compute: Optional[ComputeStrategy] = None,
    batch_format: Optional[str] = "default",  # "pandas", "numpy", "pyarrow"
    zero_copy_batch: bool = False,
    fn_args: Optional[Iterable[Any]] = None,
    fn_kwargs: Optional[Dict[str, Any]] = None,
    fn_constructor_args: Optional[Iterable[Any]] = None,
    fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    memory: Optional[float] = None,
    concurrency: Optional[Union[int, Tuple[int, int]]] = None,
    ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    **ray_remote_args,
) -> Dataset
```

Example:
```python
# Batch transformation with pandas
def process_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch["normalized"] = (batch["value"] - batch["value"].mean()) / batch["value"].std()
    return batch

ds.map_batches(process_batch, batch_format="pandas", batch_size=1000)

# GPU batch inference
class GPUModel:
    def __init__(self):
        self.model = load_torch_model().cuda()
    
    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.from_numpy(batch["image"]).cuda()
        with torch.no_grad():
            outputs = self.model(inputs)
        batch["predictions"] = outputs.cpu().numpy()
        return batch

predictions = ds.map_batches(
    GPUModel,
    batch_size=32,
    num_gpus=1,
    concurrency=4  # 4 GPU workers
)
```

#### filter
```python
Dataset.filter(
    fn: Callable[[Dict[str, Any]], bool],
    *,
    compute: Optional[ComputeStrategy] = None,
    concurrency: Optional[Union[int, Tuple[int, int]]] = None,
    **ray_remote_args,
) -> Dataset
```

#### flat_map
```python
Dataset.flat_map(
    fn: Callable[[Dict[str, Any]], Iterable[Dict[str, Any]]],
    *,
    compute: Optional[ComputeStrategy] = None,
    concurrency: Optional[Union[int, Tuple[int, int]]] = None,
    **ray_remote_args,
) -> Dataset
```

#### add_column
```python
Dataset.add_column(
    col: str,
    fn: Callable[[Dict[str, Any]], Any],
    *,
    compute: Optional[ComputeStrategy] = None,
    concurrency: Optional[Union[int, Tuple[int, int]]] = None,
) -> Dataset
```

#### drop_columns / select_columns
```python
Dataset.drop_columns(cols: List[str]) -> Dataset
Dataset.select_columns(cols: List[str]) -> Dataset
```

### Grouping and Aggregation

#### groupby
```python
Dataset.groupby(key: Union[str, List[str], None]) -> GroupedData
```

#### GroupedData operations
```python
# Aggregate with built-in functions
grouped.aggregate(
    Sum("col1"),
    Mean("col2"),
    Max("col3"),
    Min("col4"),
    Count()
)

# Map groups
grouped.map_groups(
    fn: Callable[[Dict[str, Any]], Iterable[Dict[str, Any]]],
    *,
    compute: Optional[ComputeStrategy] = None,
    num_cpus: float = None,
    num_gpus: float = None,
    memory: float = None,
)
```

Example:
```python
# Group by category and compute statistics
ds = ray.data.read_csv("sales.csv")
stats = ds.groupby("category").aggregate(
    Sum("revenue"),
    Mean("price"),
    Count()
)

# Custom group processing
def process_group(group: Dict[str, Any]) -> Dict[str, Any]:
    df = pd.DataFrame(group)
    return {
        "category": df["category"].iloc[0],
        "total": df["amount"].sum(),
        "avg": df["amount"].mean(),
        "items": len(df)
    }

grouped_stats = ds.groupby("category").map_groups(process_group)
```

### Sorting and Shuffling

#### sort
```python
Dataset.sort(
    key: Union[str, List[str]],
    *,
    descending: Union[bool, List[bool]] = False,
) -> Dataset
```

#### random_shuffle
```python
Dataset.random_shuffle(
    *,
    seed: Optional[int] = None,
) -> Dataset
```

### Dataset Operations

#### union / zip
```python
Dataset.union(*other: List[Dataset]) -> Dataset
Dataset.zip(other: Dataset) -> Dataset
```

#### split
```python
Dataset.split(
    n: int,
    *,
    equal: bool = False,
    locality_hints: Optional[List[NodeIdStr]] = None,
) -> List[Dataset]

Dataset.train_test_split(
    test_size: Union[int, float],
    *,
    shuffle: bool = False,
    seed: Optional[int] = None,
) -> Tuple[Dataset, Dataset]
```

### Data Output APIs

#### write_parquet
```python
Dataset.write_parquet(
    path: str,
    *,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    try_create_dir: bool = True,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    filename_provider: Optional[FilenameProvider] = None,
    arrow_parquet_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    num_rows_per_file: Optional[int] = None,
    ray_remote_args: Dict[str, Any] = None,
    concurrency: Optional[int] = None,
    **arrow_parquet_args,
) -> None
```

#### write_csv
```python
Dataset.write_csv(
    path: str,
    *,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    try_create_dir: bool = True,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    filename_provider: Optional[FilenameProvider] = None,
    arrow_csv_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    num_rows_per_file: Optional[int] = None,
    ray_remote_args: Dict[str, Any] = None,
    concurrency: Optional[int] = None,
    **arrow_csv_args,
) -> None
```

#### write_json
```python
Dataset.write_json(
    path: str,
    *,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
    try_create_dir: bool = True,
    arrow_open_stream_args: Optional[Dict[str, Any]] = None,
    filename_provider: Optional[FilenameProvider] = None,
    pandas_json_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    num_rows_per_file: Optional[int] = None,
    ray_remote_args: Dict[str, Any] = None,
    concurrency: Optional[int] = None,
    **pandas_json_args,
) -> None
```

### Data Consumption APIs

#### Basic Access
```python
# Get rows
Dataset.take(limit: int = 20) -> List[Dict[str, Any]]
Dataset.take_batch(batch_size: int = 20, *, batch_format: str = "default") -> DataBatch
Dataset.take_all(limit: Optional[int] = None) -> List[Dict[str, Any]]

# Display
Dataset.show(limit: int = 20) -> None

# Get schema and stats
Dataset.schema(fetch_if_missing: bool = True) -> Schema
Dataset.count() -> int
Dataset.num_blocks() -> int
Dataset.size_bytes() -> int
Dataset.stats() -> str
```

#### Iterators
```python
# Iterate rows
Dataset.iter_rows() -> Iterable[Dict[str, Any]]

# Iterate batches
Dataset.iter_batches(
    *,
    batch_size: int = 1024,
    batch_format: str = "default",
    drop_last: bool = False,
    local_shuffle_buffer_size: Optional[int] = None,
    local_shuffle_seed: Optional[int] = None,
    prefetch_batches: int = 1,
) -> Iterable[DataBatch]

# PyTorch integration
Dataset.iter_torch_batches(
    *,
    batch_size: int = 1,
    dtypes: Optional[torch.dtype] = None,
    device: str = "cpu",
    collate_fn: Optional[Callable] = None,
    drop_last: bool = False,
    local_shuffle_buffer_size: Optional[int] = None,
    local_shuffle_seed: Optional[int] = None,
    prefetch_batches: int = 1,
) -> Iterable[TorchBatch]
```

Example:
```python
# Training loop with PyTorch
def train_epoch(model, dataset):
    for batch in dataset.iter_torch_batches(
        batch_size=32,
        device="cuda",
        local_shuffle_buffer_size=1000,
        prefetch_batches=2
    ):
        inputs, labels = batch["image"], batch["label"]
        outputs = model(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()
```

### Execution Control

#### materialize
```python
Dataset.materialize() -> MaterializedDataset
```
Forces execution of lazy operations and caches results in object store.

#### streaming_split
```python
Dataset.streaming_split(
    n: int,
    *,
    equal: bool = False,
    locality_hints: Optional[List[str]] = None,
) -> List[DataIterator]
```
Creates streaming iterators for distributed training.

## Asyncio and Concurrency Guidelines

### Concurrency Parameter
The `concurrency` parameter in `map`, `map_batches`, and `filter` controls parallelism:
- **Fixed concurrency**: `concurrency=4` - exactly 4 workers
- **Auto-scaling**: `concurrency=(2, 8)` - between 2-8 workers based on load
- **Default**: Ray Data automatically determines based on resources

### Actor-based Concurrency
For stateful transformations using classes:
```python
class AsyncProcessor:
    def __init__(self):
        self.session = aiohttp.ClientSession()
    
    async def process_batch(self, batch):
        # Async processing
        tasks = [self.fetch_data(row) for row in batch]
        results = await asyncio.gather(*tasks)
        return results

# Ray handles async automatically
ds.map_batches(AsyncProcessor, concurrency=10)
```

### Resource Management
```python
# CPU-bound tasks
ds.map_batches(
    cpu_intensive_fn,
    num_cpus=2,  # 2 CPUs per task
    concurrency=4,  # 4 parallel tasks
    memory=4 * 1024 * 1024 * 1024,  # 4GB per task
)

# GPU tasks
ds.map_batches(
    gpu_model,
    num_gpus=1,
    concurrency=num_gpus_in_cluster,
    batch_size=32,  # Required for GPU tasks
)

# Mixed CPU preprocessing + GPU inference
preprocessed = ds.map_batches(preprocess, concurrency=8)
predictions = preprocessed.map_batches(
    gpu_inference,
    num_gpus=1,
    concurrency=4,
    batch_size=64
)
```

### Avoiding Bottlenecks
1. **Separate CPU and GPU stages** for better pipeline efficiency
2. **Use prefetch_batches** in iterators to overlap data loading
3. **Set appropriate batch_size** - larger for GPU, smaller for CPU
4. **Monitor with Ray Dashboard** to identify bottlenecks

## Best Practices

### 1. Performance Optimization
```python
# Use map_batches for vectorized operations
ds.map_batches(lambda batch: batch * 2)  # Faster than map()

# Configure memory to avoid OOM
ds.map_batches(memory_intensive_fn, memory=8_000_000_000)  # 8GB

# Tune block size for your workload
ctx = ray.data.DataContext.get_current()
ctx.target_max_block_size = 512 * 1024 * 1024  # 512MB blocks
```

### 2. Data Loading
```python
# Override blocks for better parallelism
ds = ray.data.read_parquet(
    "s3://bucket/data",
    override_num_blocks=200,  # Explicit parallelism
)

# Enable file shuffling for better randomization
ds = ray.data.read_csv("*.csv", shuffle="files")

# Use column pruning
ds = ray.data.read_parquet("data.parquet", columns=["col1", "col2"])
```

### 3. Memory Management
```python
# Materialize intermediate results
ds_preprocessed = ds.map_batches(preprocess).materialize()

# Use streaming for large datasets
train_iterator = ds.streaming_split(n=num_workers)[worker_id]
for batch in train_iterator.iter_batches():
    process(batch)

# Configure spilling
ctx = ray.data.DataContext.get_current()
ctx.enable_operator_progress_bars = True
ctx.use_streaming_executor = True
```

### 4. Fault Tolerance
```python
# Retry on failures
ray.data.DataContext.get_current().max_errored_blocks = 10

# Save checkpoints
ds.write_parquet("checkpoint/")
recovered = ray.data.read_parquet("checkpoint/")
```

### 5. Common Patterns

#### Batch Inference Pipeline
```python
# Read → Preprocess → Inference → Postprocess → Save
predictions = (
    ray.data.read_images("s3://images/")
    .map_batches(ImagePreprocessor, concurrency=8)
    .map_batches(
        ModelInference,
        num_gpus=1,
        batch_size=32,
        concurrency=4
    )
    .map_batches(PostProcessor, concurrency=4)
    .write_parquet("s3://predictions/")
)
```

#### Distributed Training
```python
def train_worker(rank: int, dataset: ray.data.Dataset):
    data_iterator = dataset.streaming_split(n=num_workers)[rank]
    model = create_model()
    
    for epoch in range(num_epochs):
        for batch in data_iterator.iter_torch_batches(
            batch_size=32,
            local_shuffle_buffer_size=1000,
            prefetch_batches=2
        ):
            train_step(model, batch)

# Launch distributed training
ray.get([
    train_worker.remote(i, dataset)
    for i in range(num_workers)
])
```

#### ETL Pipeline
```python
# Complex ETL with multiple stages
cleaned_data = (
    ray.data.read_json("raw/*.json")
    .filter(lambda x: x["valid"] == True)
    .map_batches(ParseJSON, concurrency=16)
    .groupby("user_id")
    .map_groups(AggregateUserData)
    .sort("timestamp")
    .map_batches(EnrichData, concurrency=8)
    .repartition(100)
    .write_parquet("cleaned/")
)
```

### 6. Debugging Tips
- Use `.show()` to inspect data at any stage
- Enable progress bars: `ctx.enable_operator_progress_bars = True`
- Check `.stats()` for performance metrics
- Use Ray Dashboard for cluster monitoring
- Set `RAY_DATA_VERBOSE_STATS=1` for detailed logs

### 7. Scaling Guidelines
- Start with small data samples for development
- Increase `override_num_blocks` for more parallelism
- Use `concurrency` parameter to control resource usage
- Monitor memory usage and adjust `memory` parameter
- Consider data locality for large datasets

## Example: End-to-End ML Pipeline

```python
import ray
import torch
from typing import Dict
import numpy as np

# Initialize Ray
ray.init()

# 1. Load and preprocess data
train_ds = (
    ray.data.read_parquet("s3://data/train/")
    .map_batches(
        lambda batch: {
            "image": batch["image"] / 255.0,  # Normalize
            "label": batch["label"]
        },
        batch_format="numpy"
    )
)

# 2. Model training with data streaming
@ray.remote(num_gpus=1)
class Trainer:
    def __init__(self, rank: int, dataset: ray.data.Dataset):
        self.rank = rank
        self.model = create_model().cuda()
        self.data_iterator = dataset.streaming_split(n=4)[rank]
    
    def train_epoch(self):
        for batch in self.data_iterator.iter_torch_batches(
            batch_size=64,
            device="cuda",
            local_shuffle_buffer_size=1000,
            prefetch_batches=2
        ):
            # Training logic
            pass
        return self.model.state_dict()

# 3. Distributed training
trainers = [Trainer.remote(i, train_ds) for i in range(4)]
model_states = ray.get([t.train_epoch.remote() for t in trainers])

# 4. Batch inference
class ModelInference:
    def __init__(self):
        self.model = load_model().cuda()
        self.model.eval()
    
    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.from_numpy(batch["image"]).cuda()
        with torch.no_grad():
            outputs = self.model(inputs)
        batch["predictions"] = outputs.cpu().numpy()
        return batch

# 5. Run inference pipeline
test_ds = ray.data.read_parquet("s3://data/test/")
predictions = test_ds.map_batches(
    ModelInference,
    num_gpus=1,
    batch_size=128,
    concurrency=8
)

# 6. Save results
predictions.write_parquet("s3://results/predictions/")

# 7. Compute metrics
metrics = predictions.groupby("category").aggregate(
    Mean("confidence"),
    Count()
)
metrics.show()
```

This comprehensive overview covers Ray Data's architecture, complete API reference, concurrency patterns, and best practices for building scalable data processing pipelines.