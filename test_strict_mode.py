import pandas as pd
import ray

# Quick test of the new functionality
ray.init()

# Create test data
df = pd.DataFrame({
    'key': [1, 1, 2, 2, 3, 3],
    'value': [10, 20, 30, 40, 50, 60]
})

ds = ray.data.from_pandas(df)

# Test that strict_mode parameter is accepted
try:
    ds_strict = ds.repartition(3, keys=['key'], strict_mode=True)
    print('✓ strict_mode=True works')
    
    ds_non_strict = ds.repartition(3, keys=['key'], strict_mode=False)
    print('✓ strict_mode=False works')
    
    print('✓ Basic functionality test passed')
except Exception as e:
    print(f'✗ Error: {e}')
    import traceback
    traceback.print_exc()

ray.shutdown()