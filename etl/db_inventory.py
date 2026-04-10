import os, subprocess, sys
out = "/tmp/backfill_check.txt"
with open(out, "w") as f:
    subprocess.run([sys.executable, "/tmp/check_odds_backfill.py"],
                   stdout=f, stderr=subprocess.STDOUT)
with open(out) as f:
    print(f.read())
