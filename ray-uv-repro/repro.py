import os
import time
import ray

use_uv = os.environ.get("USE_UV", "0") == "1"
print(f"USE_UV={use_uv}")
print(f"ray.__version__={ray.__version__}")

# Use a runtime_env that requires building an env with dependencies
if use_uv:
	runtime_env = {
		"uv": {
			"packages": [
				"numpy==1.26.4",
				"requests==2.31.0",
			],
			"uv_pip_install_options": ["--no-cache"],
		}
	}
	msg = "Initializing Ray with runtime_env.uv..."
else:
	runtime_env = {
		"pip": [
			"numpy==1.26.4",
			"requests==2.31.0",
		],
	}
	msg = "Initializing Ray with runtime_env.pip..."

print(msg, flush=True)
ray.init(runtime_env=runtime_env, ignore_reinit_error=True)

@ray.remote
def ping():
	import numpy as _np
	return ("ok", str(_np.__version__))

print("Submitting task...", flush=True)
obj = ping.remote()

start = time.time()
try:
	res = ray.get(obj, timeout=60)
	print("Task result:", res)
	print("SUCCESS")
	exit(0)
except ray.exceptions.GetTimeoutError:
	print("HANG: ray.get timed out after 60s", flush=True)
	exit(2)
except Exception as e:
	print("ERROR:", repr(e), flush=True)
	raise