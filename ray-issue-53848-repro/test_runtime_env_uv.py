import ray
import os

print(f"RAY_ENABLE_UV_RUN_RUNTIME_ENV = {os.environ.get('RAY_ENABLE_UV_RUN_RUNTIME_ENV', 'not set')}")

# Initialize Ray with a runtime_env that uses uv packages
runtime_env = {
    "uv": ["requests==2.31.0", "numpy==1.26.0"]
}

ray.init(runtime_env=runtime_env)

@ray.remote
def check_packages():
    import requests
    import numpy as np
    return f"requests version: {requests.__version__}, numpy version: {np.__version__}"

try:
    result = ray.get(check_packages.remote())
    print(f"Runtime env test result: {result}")
    print("Runtime env with uv test completed successfully")
except Exception as e:
    print(f"Error with runtime env: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
finally:
    ray.shutdown()