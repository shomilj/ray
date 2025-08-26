import ray
import os

print(f"RAY_ENABLE_UV_RUN_RUNTIME_ENV = {os.environ.get('RAY_ENABLE_UV_RUN_RUNTIME_ENV', 'not set')}")

# Initialize Ray with a runtime_env that uses requirements.txt
runtime_env = {
    "pip": "./requirements.txt"
}

ray.init(runtime_env=runtime_env)

@ray.remote
def check_packages():
    import requests
    import numpy as np
    import pandas as pd
    return f"requests: {requests.__version__}, numpy: {np.__version__}, pandas: {pd.__version__}"

try:
    result = ray.get(check_packages.remote())
    print(f"Runtime env test result: {result}")
    print("Runtime env with requirements.txt test completed successfully")
except Exception as e:
    print(f"Error with runtime env: {type(e).__name__}: {e}")
finally:
    ray.shutdown()