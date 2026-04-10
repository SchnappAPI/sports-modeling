"""Temporary: run signal_backtest via the db_inventory workflow."""
import subprocess, sys
result = subprocess.run(
    [sys.executable, "etl/signal_backtest.py", "--mode", "all", "--days", "90"],
    capture_output=False
)
sys.exit(result.returncode)
