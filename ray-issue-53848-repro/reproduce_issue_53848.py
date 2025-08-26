#!/usr/bin/env python3
"""
Reproduction script for Ray issue #53848
This demonstrates the issue with RAY_ENABLE_UV_RUN_RUNTIME_ENV in Ray 2.48.0

The issue occurs when Ray tries to use UV for runtime environments but fails
because it attempts to install UV using pip in a virtualenv that doesn't have pip.
"""

import ray
import os
import sys

def test_uv_runtime_env():
    """Test using UV-based runtime environment"""
    print("=" * 60)
    print("Testing UV runtime environment")
    print(f"RAY_ENABLE_UV_RUN_RUNTIME_ENV = {os.environ.get('RAY_ENABLE_UV_RUN_RUNTIME_ENV', 'not set')}")
    print(f"Ray version: {ray.__version__}")
    print("=" * 60)
    
    # Use UV field for runtime environment
    runtime_env = {
        "uv": ["requests==2.31.0", "numpy==1.26.0"]
    }
    
    try:
        ray.init(runtime_env=runtime_env)
        
        @ray.remote
        def check_packages():
            import requests
            import numpy as np
            return f"requests: {requests.__version__}, numpy: {np.__version__}"
        
        result = ray.get(check_packages.remote())
        print(f"✓ Success: {result}")
        return True
        
    except Exception as e:
        print(f"✗ Failed with {type(e).__name__}: {e}")
        # Extract the key error details
        if "No module named pip" in str(e):
            print("\nKey issue: virtualenv created without pip module")
        return False
    finally:
        ray.shutdown()

def test_pip_runtime_env():
    """Test using traditional pip-based runtime environment"""
    print("\n" + "=" * 60)
    print("Testing pip runtime environment (traditional)")
    print("=" * 60)
    
    runtime_env = {
        "pip": ["requests==2.31.0", "numpy==1.26.0"]
    }
    
    try:
        ray.init(runtime_env=runtime_env)
        
        @ray.remote
        def check_packages():
            import requests
            import numpy as np
            return f"requests: {requests.__version__}, numpy: {np.__version__}"
        
        result = ray.get(check_packages.remote())
        print(f"✓ Success: {result}")
        return True
        
    except Exception as e:
        print(f"✗ Failed with {type(e).__name__}: {e}")
        return False
    finally:
        ray.shutdown()

def main():
    print("Ray Issue #53848 Reproduction")
    print("This reproduces the UV runtime environment issue in Ray 2.48.0")
    print()
    
    # Test 1: With UV enabled (should fail)
    print("\n--- Test 1: UV enabled (expected to fail) ---")
    os.environ["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] = "1"
    uv_success = test_uv_runtime_env()
    
    # Test 2: With UV disabled (should work)
    print("\n--- Test 2: UV disabled (expected to work) ---")
    os.environ["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] = "0"
    uv_disabled_success = test_uv_runtime_env()
    
    # Test 3: Traditional pip (should fail due to missing pip in virtualenv)
    print("\n--- Test 3: Traditional pip runtime_env ---")
    pip_success = test_pip_runtime_env()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"UV with RAY_ENABLE_UV_RUN_RUNTIME_ENV=1: {'✓ Passed' if uv_success else '✗ Failed'}")
    print(f"UV with RAY_ENABLE_UV_RUN_RUNTIME_ENV=0: {'✓ Passed' if uv_disabled_success else '✗ Failed'}")
    print(f"Traditional pip runtime_env: {'✓ Passed' if pip_success else '✗ Failed'}")
    
    print("\nISSUE: Ray 2.48.0 fails to properly set up UV runtime environments")
    print("because it tries to install UV using pip in a virtualenv without pip.")
    print("\nWORKAROUND: Set RAY_ENABLE_UV_RUN_RUNTIME_ENV=0")

if __name__ == "__main__":
    main()