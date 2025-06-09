# Solution Summary: Non-blocking Repartition for Partitioned Datasets

## Overview
This solution addresses the GitHub issue by adding a `strict_mode` parameter to the `repartition()` method, allowing users to control whether all records with the same key must reside in a single block.

## Key Changes

1. **API Enhancement**: Added `strict_mode: bool = True` parameter to `Dataset.repartition()`
   - `strict_mode=True` (default): Maintains existing behavior for backward compatibility
   - `strict_mode=False`: Allows rows with same key to be in multiple blocks

2. **Implementation**: 
   - Created `StreamingConcat` aggregation for non-strict mode that yields blocks immediately without concatenation
   - Updated `HashShuffleOperator` to choose between `Concat` (strict) and `StreamingConcat` (non-strict) based on mode

3. **Use Case**: Optimized for writing partitioned datasets where rows with the same partition key can be spread across multiple files within the same partition directory

## Example Usage

```python
# For writing partitioned datasets (primary use case)
ds = ray.data.from_pandas(df)
ds_repartitioned = ds.repartition(
    num_blocks=20,
    keys=['partition_col'],
    strict_mode=False  # Non-blocking, better parallelism
)
ds_repartitioned.write_parquet(path, partition_cols=['partition_col'])

# For grouped operations (requires strict mode)
ds_grouped = ds.repartition(
    num_blocks=10,
    keys=['group_col'],
    strict_mode=True  # Default - ensures all rows with same key in one block
)
result = ds_grouped.groupby('group_col').map_groups(process_group)
```

## Benefits
- **Performance**: Non-blocking operation allows streaming processing
- **Parallelism**: Better resource utilization as data doesn't need consolidation
- **Memory**: Reduced memory pressure from avoiding large block concatenations
- **Compatibility**: Default behavior unchanged, existing code continues to work

## Testing
- Added unit tests in `test_all_to_all.py` and `test_repartition_strict_mode.py`
- Created example script demonstrating use cases
- Verified data integrity is maintained in both modes