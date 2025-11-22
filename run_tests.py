import os
import sys
import subprocess
import shutil
from pathlib import Path

def cleanup():
    """Removes cache directories."""
    caches = [
        Path("__pycache__"),
        Path(".pytest_cache"),
        Path("tests/__pycache__")
    ]
    for cache in caches:
        if cache.exists():
            try:
                shutil.rmtree(cache)
                # print(f"Removed {cache}")
            except Exception as e:
                print(f"Failed to remove {cache}: {e}")

def run_tests():
    """Runs pytest with cache disabled."""
    print("🚀 Running tests...")
    
    # Set environment variable to stop .pyc generation
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    
    # Run pytest with cacheprovider disabled
    cmd = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider"]
    
    try:
        result = subprocess.run(cmd, env=env)
        return result.returncode
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 1
    finally:
        print("🧹 Cleaning up...")
        cleanup()

if __name__ == "__main__":
    sys.exit(run_tests())
