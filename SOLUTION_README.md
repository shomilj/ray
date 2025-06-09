# Solution: Non-blocking Repartition for Partitioned Dataset Writes

## Problem
The original issue identified that `repartition(key=...)` uses hash-shuffle which is a blocking operation because it enforces the invariant that all records with the same key must reside in a single block. However, this invariant is not always necessary, particularly when writing partitioned datasets where rows with the same key can be spread across multiple files within the same partition.

## Solution
Added a new `strict_mode` parameter to the `repartition()` method that controls whether all rows with the same key must be in a single block.

### Changes Made

1. **Dataset API Enhancement** (`python/ray/data/dataset.py`):
   - Added `strict_mode: bool = True` parameter to the `repartition()` method
   - When `strict_mode=True` (default): Maintains backward compatibility with existing behavior
   - When `strict_mode=False`: Allows rows with the same key to be spread across multiple blocks

2. **Logical Operator Update** (`python/ray/data/_internal/logical/operators/all_to_all_operator.py`):
   - Updated `Repartition` class to accept and store the `strict_mode` parameter

3. **Planner Update** (`python/ray/data/_internal/planner/plan_all_to_all_op.py`):
   - Modified `_plan_hash_shuffle_repartition` to pass `strict_mode` to the physical operator

4. **Hash Shuffle Implementation** (`python/ray/data/_internal/execution/operators/hash_shuffle.py`):
   - Added `StreamingConcat` aggregation class for non-strict mode
   - Updated `HashShuffleOperator` to choose between `Concat` (strict) and `StreamingConcat` (non-strict) based on the mode
   - Modified `HashShuffleAggregator.finalize` to handle both single blocks and generators

### Usage Examples

#### Writing Partitioned Datasets (Primary Use Case)
```python
import ray
import pandas as pd

# Create dataset with partition columns
df = pd.DataFrame({
    'partition_col': [1, 1, 2, 2, 3, 3] * 1000,
    'value': range(6000)
})

ds = ray.data.from_pandas(df)

# Repartition with non-strict mode for better performance
ds_repartitioned = ds.repartition(
    num_blocks=10,
    keys=['partition_col'],
    strict_mode=False  # Allows streaming and better parallelism
)

# Write partitioned dataset - rows with same key can be in multiple files
ds_repartitioned.write_parquet(
    '/path/to/output',
    partition_cols=['partition_col']
)
```

#### Traditional Groupby Operations (Strict Mode Required)
```python
# For operations that need all rows with same key together
ds_grouped = ds.repartition(
    num_blocks=3,
    keys=['partition_col'],
    strict_mode=True  # Default - ensures single block per key
)

# Now safe to do grouped operations
result = ds_grouped.groupby('partition_col').map_groups(process_group)
```

### Benefits

1. **Performance**: Non-strict mode enables streaming processing without blocking
2. **Parallelism**: Better parallelism as data doesn't need to be consolidated
3. **Memory Efficiency**: Reduced memory pressure as blocks can be processed as they arrive
4. **Backward Compatibility**: Default behavior remains unchanged

### Testing
Added comprehensive test suite in `python/ray/data/tests/test_repartition_strict_mode.py`:
- Test strict mode behavior (default)
- Test non-strict mode behavior
- Test integration with partitioned parquet writes
- Test streaming characteristics

### Example Script
See `example_non_strict_repartition.py` for a complete working example demonstrating both modes and their use cases.