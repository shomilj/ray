#!/usr/bin/env python3
"""
Example demonstrating non-strict repartition for writing partitioned datasets.

This shows how to use strict_mode=False to avoid the blocking behavior
when writing partitioned datasets where rows with the same key can be
spread across multiple files within the same partition.
"""

import ray
import pandas as pd
import time
import os


def main():
    # Initialize Ray
    ray.init()
    
    # Create a sample dataset with partition columns
    print("Creating sample dataset...")
    num_partitions = 100
    rows_per_partition = 10000
    
    data = []
    for partition_id in range(num_partitions):
        for i in range(rows_per_partition):
            data.append({
                'partition_key': partition_id,
                'timestamp': pd.Timestamp.now(),
                'value': i,
                'data': f'sample_data_{partition_id}_{i}'
            })
    
    df = pd.DataFrame(data)
    print(f"Created DataFrame with {len(df)} rows")
    
    # Convert to Ray Dataset
    ds = ray.data.from_pandas(df)
    
    # Example 1: Traditional strict mode (blocking)
    print("\n--- Example 1: Strict mode (default) ---")
    start_time = time.time()
    
    # This ensures all rows with the same partition_key are in a single block
    # This is a blocking operation that must wait for all data before proceeding
    ds_strict = ds.repartition(
        num_blocks=num_partitions,
        keys=['partition_key'],
        strict_mode=True  # Default
    )
    
    print(f"Strict repartition took: {time.time() - start_time:.2f} seconds")
    print(f"Number of blocks: {ds_strict.num_blocks()}")
    
    # Example 2: Non-strict mode (streaming)
    print("\n--- Example 2: Non-strict mode ---")
    start_time = time.time()
    
    # This allows rows with the same partition_key to be in multiple blocks
    # This can be more efficient and allows streaming processing
    ds_non_strict = ds.repartition(
        num_blocks=num_partitions * 2,  # Can use more blocks for better parallelism
        keys=['partition_key'],
        strict_mode=False  # New parameter
    )
    
    print(f"Non-strict repartition took: {time.time() - start_time:.2f} seconds")
    print(f"Number of blocks: {ds_non_strict.num_blocks()}")
    
    # Example 3: Writing partitioned dataset with non-strict mode
    print("\n--- Example 3: Writing partitioned dataset ---")
    output_path = "/tmp/partitioned_dataset"
    
    # Clean up any existing data
    import shutil
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    start_time = time.time()
    
    # When writing partitioned datasets, we don't need all rows with the same
    # partition key in a single block. Multiple files per partition are fine.
    ds_non_strict.write_parquet(
        output_path,
        partition_cols=['partition_key']
    )
    
    print(f"Writing partitioned dataset took: {time.time() - start_time:.2f} seconds")
    
    # Verify the output
    print("\nVerifying output...")
    for i in range(min(5, num_partitions)):  # Check first 5 partitions
        partition_path = os.path.join(output_path, f"partition_key={i}")
        if os.path.exists(partition_path):
            files = [f for f in os.listdir(partition_path) if f.endswith('.parquet')]
            print(f"Partition {i}: {len(files)} files")
    
    # Example 4: Use case comparison
    print("\n--- Use Case Comparison ---")
    print("Strict mode (default) is needed when:")
    print("  - Performing grouped operations (groupby, aggregations)")
    print("  - All rows with same key must be processed together")
    print("  - Example: ds.groupby('key').map_groups(process_group)")
    print()
    print("Non-strict mode is beneficial when:")
    print("  - Writing partitioned datasets")
    print("  - Rows with same key can be in different output files")
    print("  - Better parallelism and streaming processing is desired")
    print("  - Example: ds.repartition(keys=['partition_col'], strict_mode=False).write_parquet(...)")
    
    ray.shutdown()


if __name__ == "__main__":
    main()