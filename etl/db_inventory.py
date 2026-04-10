"""Runs signal_backtest and captures all output to a file for inspection."""
import subprocess, sys, os

out_path = "/tmp/backtest_output.txt"
with open(out_path, "w") as f:
    result = subprocess.run(
        [sys.executable, "etl/signal_backtest.py", "--mode", "all", "--days", "90"],
        stdout=f, stderr=subprocess.STDOUT, cwd=os.getcwd()
    )

# Print so GitHub Actions captures it in step logs
with open(out_path) as f:
    print(f.read())

sys.exit(result.returncode)
