import os, subprocess, sys
out = "/tmp/unmapped_out.txt"
with open(out, "w") as f:
    subprocess.run([sys.executable, "/tmp/unmapped_check.py"],
                   stdout=f, stderr=subprocess.STDOUT)
with open(out) as f:
    print(f.read())
