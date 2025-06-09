import os
import pandas as pd
import pyarrow.parquet as pq
import pytest
import ray
from ray.data.context import DataContext


def test_repartition_strict_mode_false(ray_start_regular_shared, tmp_path):
    """Test that repartition with strict_mode=False allows multiple blocks per key."""
    # Create a dataset with multiple rows per key
    num_keys = 5
    rows_per_key = 100
    df = pd.DataFrame({
        "key": [i for i in range(num_keys) for _ in range(rows_per_key)],
        "value": list(range(num_keys * rows_per_key))
    })
    
    ds = ray.data.from_pandas(df).repartition(20, shuffle=True)
    
    # Repartition with strict_mode=False
    ds_non_strict = ds.repartition(
        num_blocks=10, 
        keys=["key"], 
        strict_mode=False
    )
    
    # Check that we have the expected number of blocks
    assert ds_non_strict.num_blocks() == 10
    
    # Check that rows with the same key can be in multiple blocks
    blocks_per_key = {}
    for i, block in enumerate(ds_non_strict.iter_batches(batch_size=None)):
        if isinstance(block, pd.DataFrame):
            keys_in_block = block["key"].unique()
        else:
            keys_in_block = block["key"].to_pandas().unique()
        
        for key in keys_in_block:
            if key not in blocks_per_key:
                blocks_per_key[key] = set()
            blocks_per_key[key].add(i)
    
    # In non-strict mode, some keys should appear in multiple blocks
    assert any(len(blocks) > 1 for blocks in blocks_per_key.values()), \
        "In non-strict mode, some keys should appear in multiple blocks"
    
    # Verify all data is preserved
    result_df = ds_non_strict.to_pandas().sort_values(["key", "value"])
    expected_df = df.sort_values(["key", "value"])
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), 
                                  expected_df.reset_index(drop=True))


def test_repartition_strict_mode_true(ray_start_regular_shared):
    """Test that repartition with strict_mode=True (default) keeps all rows with same key in single block."""
    # Create a dataset with multiple rows per key
    num_keys = 5
    rows_per_key = 100
    df = pd.DataFrame({
        "key": [i for i in range(num_keys) for _ in range(rows_per_key)],
        "value": list(range(num_keys * rows_per_key))
    })
    
    ds = ray.data.from_pandas(df).repartition(20, shuffle=True)
    
    # Repartition with strict_mode=True (default)
    ds_strict = ds.repartition(
        num_blocks=num_keys,
        keys=["key"]
    )
    
    # Check that we have the expected number of blocks
    assert ds_strict.num_blocks() == num_keys
    
    # Check that each key appears in exactly one block
    blocks_per_key = {}
    for i, block in enumerate(ds_strict.iter_batches(batch_size=None)):
        if isinstance(block, pd.DataFrame):
            keys_in_block = block["key"].unique()
        else:
            keys_in_block = block["key"].to_pandas().unique()
        
        for key in keys_in_block:
            if key not in blocks_per_key:
                blocks_per_key[key] = set()
            blocks_per_key[key].add(i)
    
    # In strict mode, each key should appear in exactly one block
    assert all(len(blocks) == 1 for blocks in blocks_per_key.values()), \
        "In strict mode, each key should appear in exactly one block"


def test_write_parquet_with_non_strict_repartition(ray_start_regular_shared, tmp_path):
    """Test that non-strict repartition works well with partitioned parquet writes."""
    # Create a dataset with partition columns
    num_partitions = 10
    rows_per_partition = 100
    df = pd.DataFrame({
        "partition_col": [i for i in range(num_partitions) for _ in range(rows_per_partition)],
        "value": list(range(num_partitions * rows_per_partition)),
        "data": ["x" * 100] * (num_partitions * rows_per_partition)  # Some data
    })
    
    ds = ray.data.from_pandas(df)
    
    # Repartition with non-strict mode before writing
    # This allows better parallelism as rows don't need to be in single blocks
    ds_repartitioned = ds.repartition(
        num_blocks=20,  # More blocks than partitions for better parallelism
        keys=["partition_col"],
        strict_mode=False
    )
    
    # Write partitioned dataset
    ds_repartitioned.write_parquet(
        str(tmp_path),
        partition_cols=["partition_col"]
    )
    
    # Verify the output
    for i in range(num_partitions):
        partition_path = os.path.join(tmp_path, f"partition_col={i}")
        assert os.path.exists(partition_path), f"Partition {i} should exist"
        
        # Read the partition and verify data
        partition_df = pd.concat([
            pq.read_table(os.path.join(partition_path, f)).to_pandas()
            for f in os.listdir(partition_path) if f.endswith('.parquet')
        ])
        
        assert len(partition_df) == rows_per_partition
        assert all(partition_df["value"] == list(range(i * rows_per_partition, 
                                                       (i + 1) * rows_per_partition)))


def test_non_strict_mode_performance_characteristics(ray_start_regular_shared):
    """Test that non-strict mode can process data in a streaming fashion."""
    # Create a larger dataset
    num_keys = 10
    rows_per_key = 1000
    df = pd.DataFrame({
        "key": [i for i in range(num_keys) for _ in range(rows_per_key)],
        "value": list(range(num_keys * rows_per_key))
    })
    
    ds = ray.data.from_pandas(df).repartition(50, shuffle=True)
    
    # Non-strict repartition should be able to start producing output
    # before all input is processed
    ds_non_strict = ds.repartition(
        num_blocks=20,
        keys=["key"],
        strict_mode=False
    )
    
    # Take a small sample quickly without materializing the entire dataset
    # This demonstrates the streaming nature
    sample = ds_non_strict.take(100)
    assert len(sample) == 100
    
    # Verify the full dataset is still correct
    assert ds_non_strict.count() == num_keys * rows_per_key


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__]))