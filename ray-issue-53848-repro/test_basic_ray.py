import ray
import os

# Print environment variable status
print(f"RAY_ENABLE_UV_RUN_RUNTIME_ENV = {os.environ.get('RAY_ENABLE_UV_RUN_RUNTIME_ENV', 'not set')}")

# Initialize Ray
ray.init()

@ray.remote
def hello_world():
    return "Hello from Ray!"

# Test basic Ray functionality
result = ray.get(hello_world.remote())
print(f"Basic test result: {result}")

ray.shutdown()
print("Basic Ray test completed successfully")