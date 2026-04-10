import subprocess, sys, os
out_path = "/tmp/streak_output.txt"
with open(out_path, "w") as f:
    result = subprocess.run(
        [sys.executable, "etl/streak_analysis.py"],
        stdout=f, stderr=subprocess.STDOUT, cwd=os.getcwd()
    )
with open(out_path) as f:
    print(f.read())
sys.exit(result.returncode)
