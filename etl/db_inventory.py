import os, subprocess, sys
out = "/tmp/odds_check_out.txt"
with open(out, "w") as f:
    subprocess.run([sys.executable, "/tmp/odds_check.py"],
                   stdout=f, stderr=subprocess.STDOUT)
with open(out) as f:
    print(f.read())
