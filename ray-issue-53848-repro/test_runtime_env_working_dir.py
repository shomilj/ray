import ray
import os
import tempfile
import shutil

print(f"RAY_ENABLE_UV_RUN_RUNTIME_ENV = {os.environ.get('RAY_ENABLE_UV_RUN_RUNTIME_ENV', 'not set')}")

# Create a temporary working directory with some Python files
with tempfile.TemporaryDirectory() as tmpdir:
    # Create a simple module in the temporary directory
    module_path = os.path.join(tmpdir, "mymodule.py")
    with open(module_path, "w") as f:
        f.write("""
def get_message():
    return "Hello from mymodule!"
""")
    
    # Create a requirements.txt in the working dir
    req_path = os.path.join(tmpdir, "requirements.txt")
    with open(req_path, "w") as f:
        f.write("requests==2.31.0\n")
    
    # Initialize Ray with a runtime_env that uses working_dir
    runtime_env = {
        "working_dir": tmpdir,
        "pip": os.path.join(tmpdir, "requirements.txt")
    }
    
    ray.init(runtime_env=runtime_env)
    
    @ray.remote
    def test_working_dir():
        import mymodule
        import requests
        return f"Module says: {mymodule.get_message()}, requests version: {requests.__version__}"
    
    try:
        result = ray.get(test_working_dir.remote())
        print(f"Working dir test result: {result}")
        print("Runtime env with working_dir test completed successfully")
    except Exception as e:
        print(f"Error with runtime env: {type(e).__name__}: {e}")
    finally:
        ray.shutdown()