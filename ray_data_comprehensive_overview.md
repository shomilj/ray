# Ray Data: Comprehensive Technical Overview

## Ray Framework Overview

Ray is a distributed computing framework that enables scalable Python applications through two core abstractions:

### Task/Actor Model

**Tasks** are stateless, immutable functions that can be executed in parallel across a cluster:
- Decorated with `@ray.remote`
- Executed asynchronously and return `ObjectRef`s
- Automatically distributed across available workers
- Ideal for embarrassingly parallel workloads

```python
@ray.remote
def process_data(data):
    return data * 2

# Execute tasks in parallel
futures = [process_data.remote(i) for i in range(1000)]
results = ray.get(futures)
```

**Actors** are stateful objects that maintain state across method calls:
- Classes decorated with `@ray.remote`
- Methods called asynchronously, returning `ObjectRef`s
- Enable complex stateful computations
- Support async/await patterns for concurrent operations

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0
    
    def increment(self):
        self.value += 1
        return self.value

# Create and use actor
counter = Counter.remote()
result = ray.get(counter.increment.remote())
```

## Ray Data Overview

Ray Data is Ray's data processing library providing scalable ETL, preprocessing, and batch inference. It operates on distributed datasets represented as collections of `ObjectRef[Block]` where each block contains data in Arrow format.

Key characteristics:
- **Lazy evaluation**: Transformations are deferred until consumption
- **Streaming execution**: Processes data incrementally to handle large datasets
- **Fault tolerance**: Automatic recovery from worker failures
- **Flexible data formats**: Support for Pandas, Arrow, NumPy, and raw Python objects

---

## Ray Data APIs

### Data Loading APIs

#### `read_csv(paths, *, filesystem=None, parallelism=-1, **read_args)`
Read CSV files into a Ray Dataset.

**Arguments:**
- `paths` (str | List[str]): File path(s) to read
- `filesystem` (Optional[FileSystem]): Custom filesystem implementation
- `parallelism` (int): Number of read tasks (-1 for auto)
- `read_args`: Additional arguments passed to pandas/pyarrow

```python
import ray

# Single file
ds = ray.data.read_csv("data.csv")

# Multiple files with custom parallelism
ds = ray.data.read_csv(["file1.csv", "file2.csv"], parallelism=8)

# With custom CSV options
ds = ray.data.read_csv("data.csv", delimiter="|", header=None)
```

#### `read_parquet(paths, *, filesystem=None, columns=None, parallelism=-1, **read_args)`
Read Parquet files into a Ray Dataset.

**Arguments:**
- `paths` (str | List[str]): File path(s) to read
- `filesystem` (Optional[FileSystem]): Custom filesystem implementation
- `columns` (Optional[List[str]]): Columns to read (column pruning)
- `parallelism` (int): Number of read tasks (-1 for auto)
- `read_args`: Additional arguments passed to pyarrow

```python
# Basic parquet reading
ds = ray.data.read_parquet("data.parquet")

# Column pruning for efficiency
ds = ray.data.read_parquet("data.parquet", columns=["col1", "col2"])

# S3 with custom filesystem
import pyarrow.fs as fs
s3_fs = fs.S3FileSystem(region="us-west-2")
ds = ray.data.read_parquet("s3://bucket/data.parquet", filesystem=s3_fs)
```

#### `read_json(paths, *, filesystem=None, parallelism=-1, **read_args)`
Read JSON files into a Ray Dataset.

```python
ds = ray.data.read_json("data.jsonl")  # JSON Lines format
ds = ray.data.read_json(["file1.json", "file2.json"])
```

#### `read_images(paths, *, filesystem=None, parallelism=-1, **read_args)`
Read image files into a Ray Dataset.

```python
ds = ray.data.read_images("images/")
# Each row contains 'image' (PIL Image) and 'path' columns
```

#### `read_text(paths, *, filesystem=None, parallelism=-1, **read_args)`
Read text files line-by-line.

```python
ds = ray.data.read_text("docs/")
# Each row contains a 'text' column with one line
```

#### Additional Read APIs
- `read_numpy(paths)`: Read NumPy files
- `read_tfrecords(paths)`: Read TensorFlow Records
- `read_binary_files(paths)`: Read binary files as bytes
- `read_sql(sql, connection_factory)`: Read from SQL databases
- `read_mongo(uri, database, collection)`: Read from MongoDB
- `read_bigquery(dataset, query)`: Read from Google BigQuery
- `read_snowflake(query, connection_params)`: Read from Snowflake

### Data Creation APIs

#### `from_items(items, *, parallelism=-1, override_num_blocks=None)`
Create dataset from Python objects.

```python
# Simple list
ds = ray.data.from_items([1, 2, 3, 4, 5])

# List of dictionaries
ds = ray.data.from_items([
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 30}
])
```

#### `from_pandas(dfs, *, parallelism=-1)`
Create dataset from Pandas DataFrames.

```python
import pandas as pd

df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
ds = ray.data.from_pandas([df])  # List of DataFrames
```

#### `from_arrow(tables, *, parallelism=-1)`
Create dataset from Arrow Tables.

```python
import pyarrow as pa

table = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
ds = ray.data.from_arrow([table])
```

#### `from_numpy(arrays, *, parallelism=-1)`
Create dataset from NumPy arrays.

```python
import numpy as np

arr = np.random.rand(1000, 10)
ds = ray.data.from_numpy([arr])
```

#### `range(n, *, parallelism=-1, override_num_blocks=None)`
Create dataset with range of integers.

```python
ds = ray.data.range(1000000)  # 0 to 999999
```

#### `range_tensor(n, *, shape=(1,), parallelism=-1)`
Create dataset of tensor data.

```python
ds = ray.data.range_tensor(1000, shape=(10, 10))  # 1000 10x10 tensors
```

### Core Transformation APIs

#### `Dataset.map(fn, *, compute=None, **kwargs)`
Apply function to each row.

**Arguments:**
- `fn` (Callable): Function that takes a row dict and returns a row dict
- `compute` (Optional[ComputeStrategy]): Execution strategy (tasks vs actors)
- `fn_args`, `fn_kwargs`: Arguments to pass to function
- `num_cpus`, `num_gpus`, `memory`: Resource requirements
- `concurrency`: Number of concurrent tasks/actors

```python
def transform_row(row):
    row["x_squared"] = row["x"] ** 2
    return row

ds = ray.data.range(1000)
transformed = ds.map(transform_row)

# With resource requirements
ds.map(transform_row, num_cpus=2, memory=1000_000_000)
```

#### `Dataset.map_batches(fn, *, batch_size=None, compute=None, **kwargs)`
Apply function to batches of data.

**Arguments:**
- `fn` (Callable): Function that takes a batch and returns a batch
- `batch_size` (Optional[int]): Rows per batch (None for auto)
- `batch_format` (str): "pandas", "pyarrow", "numpy", or "default"
- `zero_copy_batch` (bool): Whether to use zero-copy batching
- `compute` (Optional[ComputeStrategy]): Execution strategy

```python
def process_batch(batch):
    # batch is a dict of column_name -> np.array
    batch["x_squared"] = batch["x"] ** 2
    return batch

ds = ray.data.range(1000)
processed = ds.map_batches(process_batch, batch_size=100)

# With pandas format
def pandas_transform(df):
    df["x_squared"] = df["x"] ** 2
    return df

ds.map_batches(pandas_transform, batch_format="pandas")
```

#### `Dataset.filter(fn=None, expr=None, *, compute=None, **kwargs)`
Filter rows based on predicate.

**Arguments:**
- `fn` (Optional[Callable]): Predicate function returning bool
- `expr` (Optional[str]): Expression string for native filtering
- `compute` (Optional[ComputeStrategy]): Execution strategy

```python
# Function-based filtering
ds = ray.data.range(1000)
filtered = ds.filter(lambda row: row["id"] % 2 == 0)

# Expression-based filtering (more efficient)
filtered = ds.filter(expr="id % 2 == 0")
filtered = ds.filter(expr="id > 100 AND id < 500")
```

#### `Dataset.flat_map(fn, *, compute=None, **kwargs)`
Apply function that returns multiple rows per input row.

```python
def duplicate_row(row):
    return [row, row]  # Return list of rows

ds = ray.data.from_items([{"x": 1}, {"x": 2}])
expanded = ds.flat_map(duplicate_row)  # 4 rows total
```

#### `Dataset.add_column(col, fn, *, compute=None, **kwargs)`
Add a new column to the dataset.

```python
ds = ray.data.range(100)
with_new_col = ds.add_column("x_squared", lambda row: row["id"] ** 2)
```

#### `Dataset.drop_columns(cols)`
Remove columns from the dataset.

```python
ds = ds.drop_columns(["unwanted_col1", "unwanted_col2"])
```

#### `Dataset.select_columns(cols)`
Keep only specified columns.

```python
ds = ds.select_columns(["id", "name", "score"])
```

#### `Dataset.rename_columns(column_map)`
Rename columns.

```python
ds = ds.rename_columns({"old_name": "new_name", "id": "identifier"})
```

### Aggregation APIs

#### `Dataset.groupby(key)`
Group dataset by column(s).

**Arguments:**
- `key` (str | List[str]): Column name(s) to group by

```python
from ray.data.aggregate import Count, Sum, Mean, Max, Min, Std

ds = ray.data.from_items([
    {"group": "A", "value": 1},
    {"group": "A", "value": 2},
    {"group": "B", "value": 3}
])

# Group and aggregate
result = ds.groupby("group").aggregate(
    Count(),
    Sum("value"),
    Mean("value")
).take_all()
```

#### Built-in Aggregations
- `Count()`: Count rows
- `Sum(column)`: Sum values
- `Mean(column)`: Average values
- `Max(column)`: Maximum value
- `Min(column)`: Minimum value
- `Std(column)`: Standard deviation
- `Unique(column)`: Unique values

#### Global Aggregations
```python
ds = ray.data.range(1000)

# Global aggregations (no grouping)
total = ds.sum("id")
average = ds.mean("id")
maximum = ds.max("id")
minimum = ds.min("id")
std_dev = ds.std("id")
```

#### Custom Aggregations

```python
from ray.data.aggregate import AggregateFnV2

class Variance(AggregateFnV2):
    def __init__(self, on):
        super().__init__(
            name=f"var({on})",
            zero_factory=lambda: {"sum": 0, "sum_sq": 0, "count": 0},
            on=on,
            ignore_nulls=True
        )
    
    def aggregate_block(self, agg, block):
        values = block[self.on]
        agg["sum"] += values.sum()
        agg["sum_sq"] += (values ** 2).sum()
        agg["count"] += len(values)
        return agg
    
    def combine(self, agg1, agg2):
        return {
            "sum": agg1["sum"] + agg2["sum"],
            "sum_sq": agg1["sum_sq"] + agg2["sum_sq"],
            "count": agg1["count"] + agg2["count"]
        }
    
    def finalize(self, agg):
        mean = agg["sum"] / agg["count"]
        return (agg["sum_sq"] / agg["count"]) - (mean ** 2)

# Use custom aggregation
ds.groupby("group").aggregate(Variance("value"))
```

### Sorting and Shuffling APIs

#### `Dataset.sort(key, *, descending=False)`
Sort dataset by column(s).

```python
ds = ray.data.from_items([{"score": 85}, {"score": 92}, {"score": 78}])
sorted_ds = ds.sort("score", descending=True)

# Multi-column sort
ds.sort(["category", "score"])
```

#### `Dataset.random_shuffle(*, seed=None, num_blocks=None)`
Randomly shuffle the dataset.

```python
shuffled = ds.random_shuffle(seed=42)
```

#### `Dataset.repartition(num_blocks, *, shuffle=False)`
Change the number of blocks.

```python
# Increase parallelism
ds = ds.repartition(100)

# Reduce blocks with shuffling for even distribution
ds = ds.repartition(10, shuffle=True)
```

### Join Operations

#### `Dataset.join(other, key, *, join_type="inner")`
Join with another dataset.

**Arguments:**
- `other` (Dataset): Dataset to join with
- `key` (str | List[str]): Join key(s)
- `join_type` (str): "inner", "left", "right", "outer"

```python
customers = ray.data.from_items([
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
])

orders = ray.data.from_items([
    {"customer_id": 1, "product": "laptop"},
    {"customer_id": 2, "product": "mouse"}
])

# Inner join
result = customers.join(orders, left_key="id", right_key="customer_id")

# Left join to keep all customers
result = customers.join(orders, left_key="id", right_key="customer_id", 
                       join_type="left")
```

#### `Dataset.union(*others)`
Concatenate datasets.

```python
ds1 = ray.data.range(100)
ds2 = ray.data.range(100, 200)
combined = ds1.union(ds2)
```

#### `Dataset.zip(other)`
Zip datasets element-wise.

```python
ds1 = ray.data.from_items([{"a": 1}, {"a": 2}])
ds2 = ray.data.from_items([{"b": 3}, {"b": 4}])
zipped = ds1.zip(ds2)  # [{"a": 1, "b": 3}, {"a": 2, "b": 4}]
```

### Consumption APIs

#### `Dataset.take(limit=20)`
Take first N rows as a list.

```python
first_10 = ds.take(10)
```

#### `Dataset.take_all()`
Materialize entire dataset.

```python
all_data = ds.take_all()  # Use carefully with large datasets
```

#### `Dataset.take_batch(batch_size=20, *, batch_format="default")`
Take first batch in specified format.

```python
batch = ds.take_batch(100, batch_format="pandas")  # Returns DataFrame
batch = ds.take_batch(100, batch_format="pyarrow")  # Returns Arrow Table
```

#### `Dataset.iter_rows(*, prefetch_batches=1)`
Iterate over individual rows.

```python
for row in ds.iter_rows():
    print(row)
```

#### `Dataset.iter_batches(*, batch_size=None, batch_format="default", prefetch_batches=1)`
Iterate over batches.

```python
for batch in ds.iter_batches(batch_size=1000, batch_format="pandas"):
    # Process pandas DataFrame
    result = batch.groupby("category").mean()
```

#### `Dataset.to_pandas(*, limit=None)`
Convert to single Pandas DataFrame.

```python
df = ds.to_pandas()  # Use with small datasets only
```

#### `Dataset.to_arrow(*, limit=None)`
Convert to single Arrow Table.

```python
table = ds.to_arrow()
```

#### `Dataset.to_torch(*, label_column=None, batch_size=1)`
Create PyTorch DataLoader.

```python
dataloader = ds.to_torch(label_column="target", batch_size=32)
for batch in dataloader:
    # batch is dict with torch.Tensor values
    pass
```

#### `Dataset.to_tf(*, label_column=None, batch_size=1)`
Create TensorFlow dataset.

```python
tf_dataset = ds.to_tf(label_column="target", batch_size=32)
for batch in tf_dataset:
    # Train TensorFlow model
    pass
```

### Data Output APIs

#### `Dataset.write_csv(path, *, filesystem=None, **write_args)`
Write to CSV files.

```python
ds.write_csv("output/")
ds.write_csv("s3://bucket/output/", filesystem=s3_fs)
```

#### `Dataset.write_parquet(path, *, filesystem=None, **write_args)`
Write to Parquet files.

```python
ds.write_parquet("output/")

# With compression
ds.write_parquet("output/", compression="snappy")
```

#### `Dataset.write_json(path, *, filesystem=None, **write_args)`
Write to JSON files.

```python
ds.write_json("output/")
```

#### Additional Write APIs
- `write_numpy(path)`: Write to NumPy files
- `write_tfrecords(path)`: Write TensorFlow Records
- `write_mongo(uri, database, collection)`: Write to MongoDB
- `write_sql(table, connection_factory)`: Write to SQL databases

### Metadata and Inspection APIs

#### `Dataset.schema()`
Get dataset schema.

```python
schema = ds.schema()
print(schema)
```

#### `Dataset.count()`
Count total rows.

```python
num_rows = ds.count()
```

#### `Dataset.num_blocks()`
Get number of blocks.

```python
num_blocks = ds.num_blocks()
```

#### `Dataset.size_bytes()`
Get dataset size in bytes.

```python
size = ds.size_bytes()
```

#### `Dataset.stats()`
Get execution statistics.

```python
print(ds.stats())
```

### Advanced APIs

#### `Dataset.limit(limit)`
Limit number of rows.

```python
small_ds = ds.limit(1000)
```

#### `Dataset.streaming_split(n, *, equal=False)`
Split into multiple datasets for parallel consumption.

```python
splits = ds.streaming_split(3)
# Process splits in parallel
futures = [process_split.remote(split) for split in splits]
```

#### `Dataset.train_test_split(*, test_size=0.2, shuffle=True, seed=None)`
Split for ML training.

```python
train_ds, test_ds = ds.train_test_split(test_size=0.2, shuffle=True, seed=42)
```

#### `Dataset.materialize()`
Force dataset execution and caching.

```python
materialized = ds.materialize()  # Compute and cache results
```

---

## Compute Strategies

Ray Data supports different execution strategies for transformations:

### ActorPoolStrategy
Use stateful actors for expensive initialization.

```python
from ray.data import ActorPoolStrategy

class ExpensiveModel:
    def __init__(self):
        # Expensive model loading
        self.model = load_large_model()
    
    def __call__(self, batch):
        return self.model.predict(batch)

# Use actor pool to amortize initialization cost
ds.map_batches(
    ExpensiveModel,
    compute=ActorPoolStrategy(size=4),  # 4 actors
    batch_size=100
)
```

### Task-based Strategy (Default)
Use stateless tasks for simple transformations.

```python
ds.map_batches(simple_function)  # Uses tasks by default
```

---

## AsyncIO and Concurrency Guidelines

### Async Actors
Ray supports async/await in actors for concurrent I/O operations.

```python
@ray.remote
class AsyncProcessor:
    async def process_batch(self, batch):
        # Concurrent I/O operations
        tasks = [self.process_item(item) for item in batch]
        results = await asyncio.gather(*tasks)
        return results
    
    async def process_item(self, item):
        # Simulate async I/O (API call, database query, etc.)
        await asyncio.sleep(0.1)
        return item * 2

# Use async actor with Ray Data
async_actor = AsyncProcessor.remote()

def process_with_async_actor(batch):
    # This will be called from Ray Data
    future = async_actor.process_batch.remote(batch)
    return ray.get(future)

ds.map_batches(process_with_async_actor)
```

### Concurrency in Transformations
Control parallelism with concurrency parameters.

```python
# Control task concurrency
ds.map_batches(
    process_function,
    concurrency=8  # Max 8 concurrent tasks
)

# Control actor concurrency
ds.map_batches(
    ProcessorClass,
    compute=ActorPoolStrategy(size=4),
    concurrency=(2, 4)  # Min 2, max 4 actors
)
```

### Async I/O Patterns
Best practices for async I/O in data processing:

```python
import asyncio
import aiohttp

@ray.remote
class AsyncAPIProcessor:
    def __init__(self):
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def process_batch(self, batch):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for item in batch:
                task = self.fetch_data(session, item["url"])
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle results and errors
            processed = []
            for item, result in zip(batch, results):
                if isinstance(result, Exception):
                    item["error"] = str(result)
                    item["data"] = None
                else:
                    item["data"] = result
                    item["error"] = None
                processed.append(item)
            
            return processed
    
    async def fetch_data(self, session, url):
        async with session.get(url) as response:
            return await response.json()

# Use async processor
processor = AsyncAPIProcessor.remote()

def async_batch_processor(batch):
    future = processor.process_batch.remote(batch)
    return ray.get(future)

ds.map_batches(async_batch_processor, batch_size=10)
```

---

## Best Practices

### Performance Optimization

#### 1. Optimize Parallelism
```python
# Auto-detect parallelism (recommended)
ds = ray.data.read_parquet("data/", parallelism=-1)

# Manual tuning for specific workloads
# Rule of thumb: 2-4x number of CPU cores
ds = ray.data.read_parquet("data/", parallelism=32)
```

#### 2. Choose Appropriate Batch Sizes
```python
# For CPU-intensive tasks: smaller batches
ds.map_batches(cpu_intensive_fn, batch_size=100)

# For I/O or memory-intensive tasks: larger batches
ds.map_batches(io_intensive_fn, batch_size=1000)

# For GPU inference: match GPU memory capacity
ds.map_batches(gpu_inference, batch_size=32, num_gpus=1)
```

#### 3. Use Column Pruning
```python
# Only read needed columns
ds = ray.data.read_parquet("data/", columns=["id", "features", "label"])
```

#### 4. Leverage Predicate Pushdown
```python
# Use expression-based filtering for better performance
ds = ds.filter(expr="score > 0.8 AND category == 'valid'")
```

#### 5. Optimize Data Formats
```python
# Use Apache Arrow for zero-copy operations
ds.map_batches(process_fn, batch_format="pyarrow", zero_copy_batch=True)
```

### Memory Management

#### 1. Control Memory Usage
```python
# Set memory limits for transformations
ds.map_batches(
    memory_intensive_fn,
    memory=2_000_000_000,  # 2GB per task
    batch_size=100
)
```

#### 2. Use Streaming for Large Datasets
```python
# Process data in streaming fashion
for batch in ds.iter_batches(batch_size=1000):
    process_batch(batch)
    # Each batch is garbage collected after processing
```

#### 3. Repartition for Memory Efficiency
```python
# Reduce memory pressure by increasing blocks
ds = ds.repartition(num_blocks=100)
```

### Data Processing Patterns

#### 1. ETL Pipeline Example
```python
def etl_pipeline():
    # Extract
    raw_data = ray.data.read_json("raw_data/")
    
    # Transform
    cleaned = (raw_data
        .filter(expr="status == 'valid'")
        .map_batches(clean_data, batch_format="pandas")
        .map_batches(feature_engineering)
        .drop_columns(["temp_col1", "temp_col2"])
    )
    
    # Load
    cleaned.write_parquet("processed_data/")
    
    return cleaned

def clean_data(df):
    # Pandas operations
    df = df.dropna()
    df["normalized_score"] = df["score"] / df["score"].max()
    return df

def feature_engineering(batch):
    # Feature creation
    batch["feature_1"] = batch["col_a"] * batch["col_b"]
    batch["feature_2"] = np.log1p(batch["col_c"])
    return batch
```

#### 2. ML Preprocessing Pipeline
```python
def ml_preprocessing_pipeline(train_path, test_path):
    # Load data
    train_ds = ray.data.read_parquet(train_path)
    test_ds = ray.data.read_parquet(test_path)
    
    # Feature preprocessing
    def normalize_features(batch):
        # Assume normalization parameters computed beforehand
        for col in ["feature1", "feature2", "feature3"]:
            batch[col] = (batch[col] - MEANS[col]) / STDS[col]
        return batch
    
    # Apply preprocessing
    train_processed = train_ds.map_batches(normalize_features)
    test_processed = test_ds.map_batches(normalize_features)
    
    return train_processed, test_processed
```

#### 3. Batch Inference Pipeline
```python
@ray.remote(num_gpus=1)
class ModelInference:
    def __init__(self, model_path):
        # Load model on GPU
        import torch
        self.device = torch.device("cuda")
        self.model = torch.load(model_path).to(self.device)
        self.model.eval()
    
    def predict_batch(self, batch):
        import torch
        
        # Convert to tensor
        features = torch.tensor(batch["features"]).to(self.device)
        
        with torch.no_grad():
            predictions = self.model(features)
        
        # Add predictions to batch
        batch["predictions"] = predictions.cpu().numpy()
        return batch

def run_batch_inference(data_path, model_path, output_path):
    # Load data
    ds = ray.data.read_parquet(data_path)
    
    # Run inference
    predictions = ds.map_batches(
        ModelInference,
        fn_constructor_args=[model_path],
        compute=ActorPoolStrategy(size=2),  # 2 GPU actors
        batch_size=64,
        num_gpus=1
    )
    
    # Save results
    predictions.write_parquet(output_path)
```

### Error Handling and Debugging

#### 1. Graceful Error Handling
```python
def robust_processing(batch):
    try:
        return expensive_operation(batch)
    except Exception as e:
        # Log error and return empty result
        print(f"Error processing batch: {e}")
        return {"error": [str(e)] * len(batch["id"])}

ds.map_batches(robust_processing)
```

#### 2. Monitoring and Debugging
```python
# Enable verbose logging
import logging
logging.getLogger("ray.data").setLevel(logging.DEBUG)

# Use stats for performance monitoring
result = ds.map_batches(process_fn).materialize()
print(result.stats())

# Profile memory usage
print(f"Dataset size: {ds.size_bytes() / 1e9:.2f} GB")
print(f"Number of blocks: {ds.num_blocks()}")
```

### Production Deployment

#### 1. Resource Management
```python
# Configure Ray cluster resources
ray.init(
    num_cpus=64,
    num_gpus=8,
    object_store_memory=50_000_000_000  # 50GB object store
)

# Set resource requirements for workloads
ds.map_batches(
    gpu_intensive_fn,
    num_gpus=0.5,  # Share GPU between tasks
    memory=4_000_000_000,  # 4GB memory per task
    batch_size=32
)
```

#### 2. Fault Tolerance Configuration
```python
# Configure retries for fault tolerance
ds.map_batches(
    potentially_failing_fn,
    max_retries=3,
    retry_exceptions=True
)
```

#### 3. Data Locality Optimization
```python
# Use placement groups for data locality
from ray.util.placement_group import placement_group

pg = placement_group([{"CPU": 4, "GPU": 1}] * 4)

ds.map_batches(
    process_fn,
    scheduling_strategy=PlacementGroupSchedulingStrategy(
        placement_group=pg,
        placement_group_bundle_index=0
    )
)
```

#### 4. Monitoring in Production
```python
# Use Ray dashboard for monitoring
# Access at http://localhost:8265

# Custom metrics collection
import time

def monitored_processing(batch):
    start_time = time.time()
    result = process_batch(batch)
    processing_time = time.time() - start_time
    
    # Log metrics to your monitoring system
    logger.info(f"Processed {len(batch)} rows in {processing_time:.2f}s")
    
    return result
```

This comprehensive overview covers all major Ray Data APIs, patterns, and best practices for building scalable data processing pipelines. The key to success with Ray Data is understanding the distributed execution model, choosing appropriate parallelism levels, and leveraging the right compute strategies for your workload.