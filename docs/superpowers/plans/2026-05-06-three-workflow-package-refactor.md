# Three Workflow Package Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the repository into a package centered on the three retained workflows and remove scripts not required by those workflows.

**Architecture:** Create `ua_workflows/` as the real application package, keep `scripts/` as three thin CLI wrappers, and move shared Guangdada, LLM, Feishu, media, and DB code into `ua_workflows/shared/`. Preserve behavior first, then clean unused files after import and CLI verification passes.

**Tech Stack:** Python 3, Playwright, SQLite, Feishu `lark_oapi`, OpenRouter/OpenAI clients, shell wrappers only where needed for user entry points.

---

### Task 1: Create Package Skeleton

**Files:**
- Create: `ua_workflows/__init__.py`
- Create: `ua_workflows/cli.py`
- Create: `ua_workflows/video_enhancer/__init__.py`
- Create: `ua_workflows/arrow2/__init__.py`
- Create: `ua_workflows/shared/__init__.py`
- Create: `ua_workflows/shared/config.py`
- Create directories: `ua_workflows/shared/db`, `ua_workflows/shared/feishu`, `ua_workflows/shared/guangdada`, `ua_workflows/shared/llm`, `ua_workflows/shared/media`

- [ ] **Step 1: Add empty package markers and shared config**

Create `ua_workflows/shared/config.py` with:

```python
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"
REPORTS_DIR = PROJECT_ROOT / "reports"


def config_path(name: str) -> Path:
    return CONFIG_DIR / name
```

- [ ] **Step 2: Run import smoke**

Run:

```bash
source .venv/bin/activate
python - <<'PY'
from ua_workflows.shared.config import PROJECT_ROOT, DATA_DIR
print(PROJECT_ROOT.name, DATA_DIR.name)
PY
```

Expected: prints `ua素材 data`.

### Task 2: Move Shared Utility Modules

**Files:**
- Move: `scripts/path_util.py` → `ua_workflows/shared/config.py` or compatibility shim
- Move: `scripts/proxy_util.py` → `ua_workflows/shared/guangdada/proxy.py`
- Move: `scripts/guangdada_login.py` → `ua_workflows/shared/guangdada/login.py`
- Move: `scripts/run_search_workflow.py` → `ua_workflows/shared/guangdada/search.py`
- Move: `scripts/workflow_guangdada_competitor_yesterday_creatives.py` → `ua_workflows/shared/guangdada/competitor_utils.py`
- Move: `scripts/guangdada_yesterday_creatives_db.py` → `ua_workflows/shared/db/guangdada_yesterday.py`
- Move: `scripts/llm_client.py` → `ua_workflows/shared/llm/client.py`
- Move: `scripts/tiktok_video_resolve.py` → `ua_workflows/shared/media/resolve.py`

- [ ] **Step 1: Move files with git-aware `mv`**

Run:

```bash
mkdir -p ua_workflows/shared/{db,feishu,guangdada,llm,media}
mv scripts/proxy_util.py ua_workflows/shared/guangdada/proxy.py
mv scripts/guangdada_login.py ua_workflows/shared/guangdada/login.py
mv scripts/run_search_workflow.py ua_workflows/shared/guangdada/search.py
mv scripts/workflow_guangdada_competitor_yesterday_creatives.py ua_workflows/shared/guangdada/competitor_utils.py
mv scripts/guangdada_yesterday_creatives_db.py ua_workflows/shared/db/guangdada_yesterday.py
mv scripts/llm_client.py ua_workflows/shared/llm/client.py
mv scripts/tiktok_video_resolve.py ua_workflows/shared/media/resolve.py
```

- [ ] **Step 2: Update imports in moved files**

Replace local imports:

```python
from path_util import DATA_DIR, CONFIG_DIR, PROJECT_ROOT
```

with:

```python
from ua_workflows.shared.config import DATA_DIR, CONFIG_DIR, PROJECT_ROOT
```

Replace:

```python
from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl
from guangdada_yesterday_creatives_db import ...
```

with:

```python
from ua_workflows.shared.guangdada.login import login
from ua_workflows.shared.guangdada.proxy import prepare_playwright_proxy_for_crawl
from ua_workflows.shared.db.guangdada_yesterday import ...
```

- [ ] **Step 3: Add short-lived compatibility shims only if required**

If a not-yet-migrated file still imports `path_util`, keep `scripts/path_util.py` temporarily:

```python
from ua_workflows.shared.config import *  # noqa: F401,F403
```

Remove it in the cleanup task after all imports are migrated.

- [ ] **Step 4: Run compile smoke**

Run:

```bash
source .venv/bin/activate
python -m compileall ua_workflows
```

Expected: no syntax errors.

### Task 3: Move Video Enhancer Workflow

**Files:**
- Move: `scripts/workflow_video_enhancer_full_pipeline.py` → `ua_workflows/video_enhancer/pipeline.py`
- Move: `scripts/test_video_enhancer_two_competitors_318.py` → `ua_workflows/video_enhancer/crawl.py`
- Move: `scripts/enrich_raw_with_dom_detail.py` → `ua_workflows/video_enhancer/dom_enrich.py`
- Move: `scripts/analyze_video_from_raw_json.py` → `ua_workflows/video_enhancer/analyze.py`
- Move: `scripts/generate_video_enhancer_ua_suggestions_from_analysis.py` → `ua_workflows/video_enhancer/suggestions.py`
- Move: `scripts/sync_raw_analysis_to_bitable_and_push_card.py` → `ua_workflows/video_enhancer/sync.py`
- Move: `scripts/push_video_enhancer_feishu_card_only.py` → `ua_workflows/video_enhancer/push_feishu.py`
- Move: `scripts/push_video_enhancer_multichannel.py` → `ua_workflows/video_enhancer/push_multichannel.py`
- Move: `scripts/cover_style_intraday.py` → `ua_workflows/video_enhancer/cover_dedupe.py`
- Move: `scripts/cover_embedding.py` → `ua_workflows/shared/media/cover_embedding.py`
- Move: `scripts/video_enhancer_pipeline_db.py` → `ua_workflows/shared/db/video_enhancer.py`
- Move: `scripts/ua_crawl_db.py` → `ua_workflows/shared/db/ua_crawl.py`
- Move: `scripts/workflow_video_enhancer_acceptance.py` → `ua_workflows/video_enhancer/acceptance.py`
- Move: `scripts/filter_step_report_util.py` → `ua_workflows/video_enhancer/filter_reports.py`
- Move: `scripts/launched_effects_db.py` → `ua_workflows/video_enhancer/launched_effects.py`
- Move: `scripts/test_dom_video_url_ai_mirror.py` → `ua_workflows/video_enhancer/dom_video_url.py`

- [ ] **Step 1: Move VE files**

Use `mv` for the files listed above.

- [ ] **Step 2: Convert subprocess script calls to module calls**

In `ua_workflows/video_enhancer/pipeline.py`, replace command paths like:

```python
"scripts/analyze_video_from_raw_json.py"
```

with module execution:

```python
"-m", "ua_workflows.video_enhancer.analyze"
```

or direct function calls where argument parsing is simple.

- [ ] **Step 3: Update imports**

Examples:

```python
from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT
from ua_workflows.shared.llm.client import print_openrouter_key_meter
from ua_workflows.video_enhancer.cover_dedupe import apply_intraday_cover_style_dedupe
from ua_workflows.shared.db.video_enhancer import init_db
```

- [ ] **Step 4: Preserve module CLIs**

Each moved file that previously had:

```python
if __name__ == "__main__":
    main()
```

keeps it, so `python -m ua_workflows.video_enhancer.analyze ...` works.

- [ ] **Step 5: Run VE import smoke**

Run:

```bash
source .venv/bin/activate
python - <<'PY'
from ua_workflows.video_enhancer.pipeline import main as ve_main
from ua_workflows.video_enhancer.analyze import is_creative_analyzable
print(ve_main.__name__, callable(is_creative_analyzable))
PY
```

Expected: `main True`.

### Task 4: Move Arrow2 Workflow

**Files:**
- Move: `scripts/workflow_arrow2_full_pipeline.py` → `ua_workflows/arrow2/pipeline.py`
- Move: `scripts/test_arrow2_first_card_fields.py` → `ua_workflows/arrow2/crawl.py`
- Move: `scripts/arrow2_pipeline_db.py` → `ua_workflows/shared/db/arrow2.py`
- Move: `scripts/arrow2_cover_style_intraday.py` → `ua_workflows/arrow2/cover_dedupe.py`
- Move: `scripts/sync_arrow2_to_bitable.py` → `ua_workflows/arrow2/sync.py`
- Move: `scripts/guangdada_detail_url.py` → `ua_workflows/shared/guangdada/detail_url.py`

- [ ] **Step 1: Move Arrow2 files**

Use `mv` for the files listed above.

- [ ] **Step 2: Update Arrow2 pipeline subprocess calls**

In `ua_workflows/arrow2/pipeline.py`, replace:

```python
str(PROJECT_ROOT / "scripts" / "test_arrow2_first_card_fields.py")
str(PROJECT_ROOT / "scripts" / "analyze_video_from_raw_json.py")
str(PROJECT_ROOT / "scripts" / "sync_arrow2_to_bitable.py")
```

with:

```python
"-m", "ua_workflows.arrow2.crawl"
"-m", "ua_workflows.video_enhancer.analyze"
"-m", "ua_workflows.arrow2.sync"
```

- [ ] **Step 3: Update cross-workflow imports**

Examples:

```python
from ua_workflows.shared.db.arrow2 import init_db
from ua_workflows.shared.guangdada.detail_url import try_build_url_spa
from ua_workflows.video_enhancer.sync import upload_image_as_attachment
from ua_workflows.video_enhancer.cover_dedupe import _cluster_clip_dedupe
```

- [ ] **Step 4: Run Arrow2 import smoke**

Run:

```bash
source .venv/bin/activate
python - <<'PY'
from ua_workflows.arrow2.pipeline import main
from ua_workflows.arrow2.crawl import parse_args
from ua_workflows.shared.db.arrow2 import get_arrow2_pipeline_items_from_raw_payload
print(main.__name__, parse_args.__name__, callable(get_arrow2_pipeline_items_from_raw_payload))
PY
```

Expected: `main parse_args True`.

### Task 5: Add Three Thin Entrypoints

**Files:**
- Create: `scripts/run_video_enhancer.py`
- Create: `scripts/run_arrow2_latest.py`
- Create: `scripts/run_arrow2_exposure.py`

- [ ] **Step 1: Create `scripts/run_video_enhancer.py`**

```python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.video_enhancer.pipeline import main

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create `scripts/run_arrow2_latest.py`**

```python
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
```

- [ ] **Step 3: Create `scripts/run_arrow2_exposure.py`**

```python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.arrow2.pipeline import main

if __name__ == "__main__":
    sys.argv.extend(["--pull-only", "exposure_top10"])
    main()
```

- [ ] **Step 4: Run help checks**

Run:

```bash
source .venv/bin/activate
.venv/bin/python scripts/run_video_enhancer.py --help
.venv/bin/python scripts/run_arrow2_latest.py --help
.venv/bin/python scripts/run_arrow2_exposure.py --help
```

Expected: all print argparse help and exit 0.

### Task 6: Clean Non-Retained Scripts

**Files:**
- Archive or delete: all scripts outside the retained package and three entrypoints
- Modify: `README.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Create archive folder**

Run:

```bash
mkdir -p archive/removed_scripts_2026_05_06
```

- [ ] **Step 2: Move non-retained scripts**

Move these groups into the archive:

```bash
mv scripts/legacy archive/removed_scripts_2026_05_06/
mv scripts/hot_rank_step*.py archive/removed_scripts_2026_05_06/ 2>/dev/null || true
mv scripts/workflow_competitor_hot_rank.py scripts/workflow_competitor_new_rank.py scripts/compute_new_rank_diff.py archive/removed_scripts_2026_05_06/ 2>/dev/null || true
mv scripts/test_*.py archive/removed_scripts_2026_05_06/ 2>/dev/null || true
mv scripts/debug*.py archive/removed_scripts_2026_05_06/ 2>/dev/null || true
mv scripts/backfill*.py archive/removed_scripts_2026_05_06/ 2>/dev/null || true
mv scripts/preview*.py archive/removed_scripts_2026_05_06/ 2>/dev/null || true
```

Before running the `test_*.py` move, confirm the three production test-named files have already been moved into `ua_workflows/`.

- [ ] **Step 3: Remove old shell wrappers**

Archive old shells:

```bash
mv scripts/daily_video_enhancer_workflow.sh archive/removed_scripts_2026_05_06/
mv scripts/daily_arrow2_workflow.sh archive/removed_scripts_2026_05_06/
mv scripts/arrow2_exposure_workflow.sh archive/removed_scripts_2026_05_06/
mv scripts/daily_ua_job.sh archive/removed_scripts_2026_05_06/ 2>/dev/null || true
```

- [ ] **Step 4: Update docs**

Update `README.md` to show only:

```bash
.venv/bin/python scripts/run_video_enhancer.py --date YYYY-MM-DD
.venv/bin/python scripts/run_arrow2_latest.py --date YYYY-MM-DD --analyze
.venv/bin/python scripts/run_arrow2_exposure.py --date YYYY-MM-DD --analyze
```

Update `AGENTS.md` current workflow section to mention `ua_workflows/` package ownership and that old workflow scripts are archived.

### Task 7: Final Verification

**Files:**
- No new files unless fixes are needed.

- [ ] **Step 1: Compile everything**

Run:

```bash
source .venv/bin/activate
python -m compileall ua_workflows scripts
```

Expected: no syntax errors.

- [ ] **Step 2: Run CLI help**

Run:

```bash
.venv/bin/python scripts/run_video_enhancer.py --help
.venv/bin/python scripts/run_arrow2_latest.py --help
.venv/bin/python scripts/run_arrow2_exposure.py --help
```

Expected: all exit 0.

- [ ] **Step 3: Run import search for stale script imports**

Run:

```bash
rg "from (path_util|llm_client|run_search_workflow|guangdada_login|proxy_util|video_enhancer_pipeline_db|arrow2_pipeline_db)|import (llm_client|path_util)" ua_workflows scripts
```

Expected: no stale imports except intentional compatibility comments.

- [ ] **Step 4: Check git diff**

Run:

```bash
git status --short
git diff --stat
```

Expected: only package moves, three entrypoints, docs, and archive/cleanup changes related to the refactor.
