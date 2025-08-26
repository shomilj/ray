#!/usr/bin/env python3
"""
Minimal reproduction for Ray issue #53848
Shows that UV runtime environment fails in Ray 2.48.0
"""

import ray
import os

print("=" * 70)
print("Ray Issue #53848 - Minimal Reproduction")
print(f"Ray version: {ray.__version__}")
print("=" * 70)

# The issue manifests when using the 'uv' field in runtime_env
# Ray tries to install uv using pip in a virtualenv that doesn't have pip

runtime_env = {
    "uv": ["requests==2.31.0"]  # Using 'uv' field triggers the UV runtime env
}

print("\nTrying to use UV runtime environment...")
print("This will fail with 'No module named pip' error")

try:
    ray.init(runtime_env=runtime_env)
    print("✓ Ray initialized successfully")
    
    # We need to actually run a remote function to trigger the runtime env setup
    @ray.remote
    def test_import():
        import requests
        return f"requests version: {requests.__version__}"
    
    print("Running remote function to trigger runtime env setup...")
    result = ray.get(test_import.remote())
    print(f"✓ Success: {result}")
    
except Exception as e:
    print(f"\n✗ Failed as expected!")
    print(f"Error: {type(e).__name__}")
    error_msg = str(e)
    if "No module named pip" in error_msg:
        print("\n--- Key Issue Found ---")
        print("Ray creates a virtualenv and tries to install UV using:")
        print("  python -m pip install uv")
        print("But the virtualenv doesn't have pip installed!")
        print("\nThis is why users need to set RAY_ENABLE_UV_RUN_RUNTIME_ENV=0")
        
        # Show the failing command
        import re
        cmd_match = re.search(r"Command '(\[.*?\])'", error_msg)
        if cmd_match:
            print(f"\nFailing command: {cmd_match.group(1)}")
finally:
    ray.shutdown()

print("\n" + "=" * 70)
print("WORKAROUND: Don't use 'uv' field in runtime_env, use 'pip' instead")