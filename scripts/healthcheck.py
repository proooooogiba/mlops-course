import time
from pathlib import Path

path = Path("/tmp/worker-heartbeat")
raise SystemExit(0 if path.exists() and time.time() - path.stat().st_mtime < 60 else 1)
