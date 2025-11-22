import subprocess
import sys
import time

def run_step(command, step_name):
    print(f"Starting: {step_name}...")
    start_time = time.time()
    
    # Run the command and wait for it to finish
    # We use sys.executable to ensure we use the same Python interpreter
    result = subprocess.run(command)
    
    duration = time.time() - start_time
    
    if result.returncode != 0:
        print(f"\n{step_name} FAILED (took {duration:.2f}s)")
        print("Stopping pipeline.")
        sys.exit(result.returncode)
    else:
        print(f"{step_name} COMPLETED (took {duration:.2f}s)\n")

def main():
    # Step 1: Run Tests
    # We call run_tests.py because it already handles cache cleanup and pytest configuration
    run_step([sys.executable, "run_tests.py"], "Tests")
    
    # Step 2: Run Main ETL Script
    run_step([sys.executable, "main.py"], "ETL Process")

if __name__ == "__main__":
    main()
