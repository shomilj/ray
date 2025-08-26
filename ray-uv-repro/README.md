# Ray + uv runtime_env repro

This is a minimal scaffold to exercise Ray's uv-based runtime_env path and compare to pip-based runtime_env.

## Setup

```bash
cd /workspace/ray-uv-repro
source .venv/bin/activate
```

## Run (Ray 2.48.0)

```bash
uv pip install "ray[default]==2.48.0"
USE_UV=1 python repro.py    # uv backend
USE_UV=0 python repro.py    # pip backend
```

## Run (Ray 2.47.0)

```bash
uv pip install -U "ray[default]==2.47.0"
USE_UV=1 python repro.py
USE_UV=0 python repro.py
```

If the regression in [ray-project/ray#53848](https://github.com/ray-project/ray/issues/53848) is present, the `USE_UV=1` run should hang or error while `USE_UV=0` succeeds.

Current result in this environment:
- 2.48.0: both uv and pip succeed
- 2.47.0: both uv and pip succeed (no hang triggered with this minimal script)

To explore further, try increasing dependency work:
```bash
# Heavier deps
export USE_UV=1
python - <<'PY'
import ray, os
pkgs = [
    "numpy==1.26.4",
    "pandas==2.2.2",
    "pyarrow==15.0.2",
    "requests==2.31.0",
]
runtime_env = {"uv": {"packages": pkgs}}
ray.init(runtime_env=runtime_env)
@ray.remote
def f():
    import pandas as pd; import pyarrow as pa; return (pd.__version__, pa.__version__)
print(ray.get(f.remote()))
PY
```