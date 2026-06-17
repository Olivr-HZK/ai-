#!/usr/bin/env python3
"""CLI wrapper for the VE template-copy click trigger service."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.video_enhancer.template_trigger_server import main


if __name__ == "__main__":
    raise SystemExit(main())
