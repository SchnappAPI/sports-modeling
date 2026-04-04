"""
db_inventory.py — temporarily redirected to lineup_cleanup.py.
Restore after the cleanup run.
"""
import runpy
runpy.run_path("etl/lineup_cleanup.py", run_name="__main__")
