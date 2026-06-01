from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.arrow2.pipeline import main


if __name__ == "__main__":
    sys.argv.extend(["--pull-only", "latest_yesterday"])
    main()
