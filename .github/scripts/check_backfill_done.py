"""Read JSON from stdin, return 'true' if all values equal 'true'."""
import json
import sys

data = json.load(sys.stdin)
values = list(data.values()) if isinstance(data, dict) else []
all_done = all(v == "true" for v in values) if values else False
print("true" if all_done else "false")
