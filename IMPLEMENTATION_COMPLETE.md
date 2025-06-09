# Implementation Complete: Non-blocking Repartition for Partitioned Datasets

## Problem Solved
The GitHub issue identified that `repartition(key=...)` uses hash-shuffle which is a blocking operation because it enforces that all records with the same key must reside in a single block. However, this invariant is unnecessary when writing partitioned datasets where rows with the same key can be spread across multiple files within the same partition.

## Solution Implemented
Added a `strict_mode` parameter to control this behavior:
- **strict_mode=True** (default): Maintains existing behavior for backward compatibility
- **strict_mode=False**: Allows non-blocking operation where rows with same key can be in multiple blocks

## Files Modified

### 1. `python/ray/data/dataset.py`
- Added `strict_mode: bool = True` parameter to `repartition()` method
- Updated docstring to explain the new parameter

### 2. `python/ray/data/_internal/logical/operators/all_to_all_operator.py`
- Updated `Repartition` class to accept and store `strict_mode` parameter

### 3. `python/ray/data/_internal/planner/plan_all_to_all_op.py`
- Modified `_plan_hash_shuffle_repartition` to pass `strict_mode` to physical operator

### 4. `python/ray/data/_internal/execution/operators/hash_shuffle.py`
- Added `StreamingConcat` aggregation class for non-strict mode
- Updated `HashShuffleOperator` to choose aggregation based on mode
- Modified `HashShuffleAggregator.finalize` to handle generators

### 5. Test Files Created/Modified
- `python/ray/data/tests/test_repartition_strict_mode.py` - New comprehensive test suite
- `python/ray/data/tests/test_all_to_all.py` - Added test for non-strict mode
- `example_non_strict_repartition.py` - Example demonstrating usage

## Usage Example

```python
import ray
import pandas as pd

# Create dataset
df = pd.DataFrame({
    'partition_col': [1, 1, 2, 2, 3, 3] * 1000,
    'value': range(6000)
})
ds = ray.data.from_pandas(df)

# Non-strict repartition for better performance when writing partitioned data
ds_repartitioned = ds.repartition(
    num_blocks=10,
    keys=['partition_col'],
    strict_mode=False  # NEW: Allows streaming, non-blocking operation
)

# Write partitioned dataset - rows with same key can be in multiple files
ds_repartitioned.write_parquet(
    '/path/to/output',
    partition_cols=['partition_col']
)
```

## Key Benefits

1. **Performance**: Non-blocking operation enables streaming processing
2. **Parallelism**: Better parallelism as data doesn't need consolidation
3. **Memory Efficiency**: Reduced memory pressure from avoiding large concatenations
4. **Backward Compatibility**: Default behavior unchanged

## Technical Details

### StreamingConcat vs Concat
- **Concat** (strict mode): Accumulates all blocks with same key, then yields single concatenated block
- **StreamingConcat** (non-strict mode): Immediately queues blocks for output, yields them as generator

### Hash Shuffling Behavior
- Both modes use same hash partitioning to ensure rows with same key go to same partition
- Difference is only in how blocks are aggregated within each partition
- Non-strict mode can yield multiple blocks per partition instead of one

## Validation
The implementation correctly:
- Maintains data integrity in both modes
- Preserves backward compatibility with default strict_mode=True
- Allows efficient writing of partitioned datasets with strict_mode=False
- Handles edge cases (empty blocks, generators, etc.)

This solution directly addresses the use case mentioned in the issue: writing partitioned datasets where having all rows with the same key in a single block is unnecessary overhead.