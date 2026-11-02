"""Repository entry point; preserves the caller's working directory for relative media paths."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ua_workflows.replica_cli import main

if __name__ == '__main__':
    raise SystemExit(main())
