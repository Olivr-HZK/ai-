#!/usr/bin/env python3
"""Sync VE play assets with the linked Lark cloud document."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ua_workflows.video_enhancer.play_asset_doc_sync import main


if __name__ == "__main__":
    main()
