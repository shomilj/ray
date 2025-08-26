# Ray Issue #53848 Reproduction

This repository contains a minimal reproduction for [Ray issue #53848](https://github.com/ray-project/ray/issues/53848).

## Issue Summary

In Ray 2.48.0, when using the `uv` field in `runtime_env`, Ray fails to set up the runtime environment with the error:
```
No module named pip
```

This happens because Ray tries to install the `uv` package using pip in a virtualenv that doesn't have pip installed.

## Root Cause

1. When you specify `runtime_env={"uv": ["package-list"]}`, Ray uses its UV runtime environment feature
2. Ray creates a virtualenv for the runtime environment
3. Ray then tries to install `uv` into that virtualenv using: `python -m pip install uv`
4. However, the virtualenv doesn't have pip installed, causing the failure

## Reproduction Steps

### Prerequisites

1. Install uv:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Create a virtual environment and install Ray 2.48.0:
   ```bash
   uv venv .venv
   source .venv/bin/activate
   uv pip install "ray==2.48.0"
   ```

### Run the Reproduction

```bash
python minimal_repro.py
```

This will show:
```
======================================================================
Ray Issue #53848 - Minimal Reproduction
Ray version: 2.48.0
======================================================================

Trying to use UV runtime environment...
This will fail with 'No module named pip' error
✓ Ray initialized successfully
Running remote function to trigger runtime env setup...

✗ Failed as expected!
Error: RuntimeEnvSetupError

--- Key Issue Found ---
Ray creates a virtualenv and tries to install UV using:
  python -m pip install uv
But the virtualenv doesn't have pip installed!
```

## Workaround

The workaround mentioned in the Slack thread is to set:
```bash
export RAY_ENABLE_UV_RUN_RUNTIME_ENV=0
```

However, this doesn't actually disable the UV runtime environment when you use the `uv` field. Instead, you should use the `pip` field in your runtime_env:

```python
# Don't use this (triggers UV runtime env):
runtime_env = {"uv": ["requests==2.31.0"]}

# Use this instead:
runtime_env = {"pip": ["requests==2.31.0"]}
```

## Files in this Repository

- `minimal_repro.py` - Minimal script that reproduces the issue
- `test_runtime_env_uv.py` - Test using UV runtime environment
- `test_runtime_env_pip.py` - Test using traditional pip runtime environment
- `test_runtime_env_requirements.py` - Test using requirements.txt
- `test_runtime_env_working_dir.py` - Test with working_dir
- `reproduce_issue_53848.py` - Comprehensive reproduction script

## Environment Details

- Ray version: 2.48.0
- Python version: 3.11+
- UV version: 0.8.13

## Related Links

- [Ray Issue #53848](https://github.com/ray-project/ray/issues/53848) (if accessible)
- [UV Documentation](https://docs.astral.sh/uv/)