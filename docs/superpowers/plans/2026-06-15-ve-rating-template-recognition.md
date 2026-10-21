# VE Rating Template Recognition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reviewer rating fields as the new VE feedback signal, keep legacy acceptance fields compatible, and generate 3-star template-recognition task artifacts.

**Architecture:** Add a small shared feedback-rating parser used by feedback training, Haopeng TopN generation, TopN backtest rendering, and template-recognition task generation. Keep the existing binary baseline model working by deriving legacy labels from ratings while exporting the richer rating fields for future models. Add a standalone template-recognition module and script that reads the VE Bitable and writes local JSON/Markdown artifacts without invoking Eagle, Video Lab, or `aigc-template-copy`.

**Tech Stack:** Python standard library, existing Feishu Bitable helpers in `feedback_training.py`, `unittest`, existing VE data/report directories.

---

### Task 1: Shared Rating Parser

**Files:**
- Create: `ua_workflows/video_enhancer/feedback_rating.py`
- Test: `tests/test_ve_feedback_rating.py`

- [ ] **Step 1: Write failing tests**

Add tests for direct ratings, Chinese star text, numeric fields, legacy fallback, reviewer field mapping, and invalid values.

```python
from __future__ import annotations

import unittest


class VeFeedbackRatingTest(unittest.TestCase):
    def test_rating_field_takes_priority_over_legacy_status(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        result = resolve_feedback_rating(
            {
                "浩鹏评分": "2星",
                "浩鹏接受情况": "采纳",
                "接受情况": "删除",
            },
            reviewer="haopeng",
        )

        self.assertEqual(result.rating, 2)
        self.assertEqual(result.rating_label, "2星")
        self.assertEqual(result.source_field, "浩鹏评分")
        self.assertEqual(result.legacy_status, "采纳")

    def test_legacy_status_maps_to_rating_when_rating_missing(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        self.assertEqual(
            resolve_feedback_rating({"浩鹏接受情况": "入素材库"}, reviewer="haopeng").rating,
            2,
        )
        self.assertEqual(
            resolve_feedback_rating({"浩鹏接受情况": "重复抓取"}, reviewer="haopeng").rating,
            1,
        )
        self.assertIsNone(
            resolve_feedback_rating({"浩鹏接受情况": "待定"}, reviewer="haopeng").rating
        )

    def test_invalid_rating_is_reported_without_legacy_override(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        result = resolve_feedback_rating(
            {"浩鹏评分": "5星", "浩鹏接受情况": "采纳"},
            reviewer="haopeng",
        )

        self.assertIsNone(result.rating)
        self.assertTrue(result.invalid_rating)
        self.assertEqual(result.source_field, "浩鹏评分")

    def test_weilan_field_mapping(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        result = resolve_feedback_rating({"尉蓝评分": 3}, reviewer="weilan")

        self.assertEqual(result.rating, 3)
        self.assertEqual(result.source_field, "尉蓝评分")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_feedback_rating.py'`

Expected: FAIL with `ModuleNotFoundError: No module named 'ua_workflows.video_enhancer.feedback_rating'`.

- [ ] **Step 3: Implement minimal parser**

Create `feedback_rating.py` with a `FeedbackRating` dataclass, reviewer field mapping, `normalize_rating_value`, `legacy_status_to_rating`, and `resolve_feedback_rating`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_feedback_rating.py'`

Expected: PASS.

### Task 2: Feedback Training Rating Export

**Files:**
- Modify: `ua_workflows/video_enhancer/feedback_training.py`
- Test: `tests/test_ve_feedback_training.py`

- [ ] **Step 1: Write failing tests**

Add tests that `normalize_record` reads `浩鹏评分`, exports rating fields, still derives binary labels for the existing baseline, and keeps `2星` out of binary training.

```python
def test_normalize_record_prefers_rating_and_exports_rating_metadata(self) -> None:
    from ua_workflows.video_enhancer.feedback_training import normalize_record

    sample = normalize_record(
        {
            "record_id": "rec_rating",
            "fields": {
                "广告ID": "ad_rating",
                "标题": "AI 模板素材",
                "核心卖点": "自拍生成节日大片",
                "浩鹏评分": "5星",
                "浩鹏接受情况": "不采纳",
            },
        }
    )

    self.assertEqual(sample.rating, 3)
    self.assertEqual(sample.rating_label, "5星")
    self.assertEqual(sample.rating_source_field, "浩鹏评分")
    self.assertEqual(sample.accept_status, "不采纳")
    self.assertEqual(sample.label, 1)


def test_two_star_is_exported_but_not_binary_labeled(self) -> None:
    from ua_workflows.video_enhancer.feedback_training import export_dataset, normalize_record

    sample = normalize_record(
        {
            "record_id": "rec_two",
            "fields": {
                "广告ID": "ad_two",
                "标题": "AI 模板素材",
                "核心卖点": "自拍生成节日大片",
                "浩鹏评分": "2星",
                "浩鹏接受情况": "采纳",
            },
        }
    )

    self.assertEqual(sample.rating, 2)
    self.assertIsNone(sample.label)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_feedback_training.py'`

Expected: FAIL because `FeedbackSample` has no rating attributes.

- [ ] **Step 3: Implement rating fields in training**

Update `FeedbackSample`, `normalize_record`, SQLite schema migration, upsert/load/export/report methods. Store `rating`, `rating_label`, `rating_source_field`, `rating_source_value`, and `legacy_accept_status`. Derive `label=1` for rating 3, `label=0` for rating 1, and `label=None` for rating 2 or missing.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_feedback_training.py'`

Expected: PASS.

### Task 3: Sync Rating Fields

**Files:**
- Modify: `ua_workflows/video_enhancer/sync.py`
- Test: `tests/test_ve_template_normalization.py`

- [ ] **Step 1: Write failing test**

Add a static field-definition test that `MAIN_FIELDS` includes `浩鹏评分` and `尉蓝评分` while retaining `接受情况`.

```python
def test_main_fields_include_rating_fields_and_legacy_acceptance(self) -> None:
    from ua_workflows.video_enhancer.sync import MAIN_FIELDS

    names = {field["field_name"] for field in MAIN_FIELDS}

    self.assertIn("浩鹏评分", names)
    self.assertIn("尉蓝评分", names)
    self.assertIn("接受情况", names)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_template_normalization.py'`

Expected: FAIL because rating fields are missing.

- [ ] **Step 3: Add field definitions**

Add `浩鹏评分` and `尉蓝评分` to `MAIN_FIELDS` as Feishu number rating/star fields with range 1 to 5. Do not write default rating values into new records.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_template_normalization.py'`

Expected: PASS for the new test.

### Task 4: Haopeng TopN Rating Prior

**Files:**
- Modify: `ua_workflows/video_enhancer/haopeng_ai_filter.py`
- Test: `tests/test_haopeng_ai_filter.py`

- [ ] **Step 1: Write failing tests**

Update tests to expect `normalize_bitable_record` to expose rating metadata, history filtering to use rating 1 to 5, and prompts to say “历史浩鹏评分偏好”.

```python
def test_normalize_bitable_record_prefers_rating_field(self) -> None:
    from ua_workflows.video_enhancer.haopeng_ai_filter import normalize_bitable_record

    row = normalize_bitable_record(
        {
            "record_id": "rec1",
            "fields": {
                "广告ID": "ad_1",
                "产品": "Glam AI",
                "抓取日期": "2026-05-28",
                "核心卖点": "自拍生成复古棒球球员卡",
                "玩法": "球员卡",
                "浩鹏评分": "5星",
                "浩鹏接受情况": "不采纳",
            },
        },
        reviewer_field="浩鹏接受情况",
    )

    self.assertEqual(row["rating"], 3)
    self.assertEqual(row["rating_source_field"], "浩鹏评分")
    self.assertEqual(row["actual_hp"], "不采纳")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_haopeng_ai_filter.py'`

Expected: FAIL because rating fields/prompt text do not exist.

- [ ] **Step 3: Implement rating-prior TopN**

Use `resolve_feedback_rating` in normalization. Pass `rating`, `rating_label`, and `status` in history payload. Change `DECISIVE_STATUSES` history logic to rating-based inclusion. Add `history_rating_counts`. Update prompt wording.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_haopeng_ai_filter.py'`

Expected: PASS.

### Task 5: TopN Push Backtest Summary

**Files:**
- Modify: `ua_workflows/video_enhancer/haopeng_topn_push.py`
- Test: `tests/test_haopeng_topn_push.py`

- [ ] **Step 1: Write failing test**

Add a test for `actual_summary` showing 3-star hits, average rating, and distribution when rows include ratings.

```python
def test_actual_summary_uses_rating_distribution_when_available(self) -> None:
    from ua_workflows.video_enhancer.haopeng_topn_push import actual_summary

    text = actual_summary(
        {"results": []},
        3,
        rows=[
            {"rating": 5, "rating_label": "5星"},
            {"rating": 3, "rating_label": "3星"},
            {"rating": 1, "rating_label": "1星"},
        ],
        force_recalculate=True,
    )

    self.assertIn("5星命中 1/3", text)
    self.assertIn("平均评分 2.00", text)
    self.assertIn("5星 1 / 3星 1 / 1星 1", text)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_haopeng_topn_push.py'`

Expected: FAIL because `actual_summary` still reports adoption counts.

- [ ] **Step 3: Implement rating summary**

Use `resolve_feedback_rating` for Bitable enrichment rows and summary rows. Prefer `row["rating"]`; fallback to legacy status via `actual_hp`. Keep old adoption summary only when no rating-compatible values exist.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_haopeng_topn_push.py'`

Expected: PASS.

### Task 6: Template Recognition Task Artifacts

**Files:**
- Create: `ua_workflows/video_enhancer/template_recognition.py`
- Create: `scripts/run_ve_template_recognition.py`
- Test: `tests/test_ve_template_recognition.py`

- [ ] **Step 1: Write failing tests**

Add tests for selecting only 3-star rows, legacy compatible 3-star rows, image/video candidate classification, JSON writing, and Markdown summary.

```python
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


class VeTemplateRecognitionTest(unittest.TestCase):
    def test_build_tasks_selects_three_star_rows_only(self) -> None:
        from ua_workflows.video_enhancer.template_recognition import build_template_tasks

        rows = [
            {
                "record_id": "rec3",
                "fields": {
                    "广告ID": "ad3",
                    "产品": "Glam AI",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "5星",
                    "视频链接": "https://example.com/a.mp4",
                    "视频时长": 8,
                    "核心卖点": "自拍跳舞模板",
                    "脚本/口播": "人物转身并跟随镜头移动",
                },
            },
            {
                "record_id": "rec2",
                "fields": {
                    "广告ID": "ad2",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "2星",
                    "封面图链接": "https://example.com/b.png",
                },
            },
        ]

        tasks = build_template_tasks(rows, target_date="2026-06-15", reviewer="haopeng")

        self.assertEqual([task["ad_key"] for task in tasks], ["ad3"])
        self.assertEqual(tasks[0]["suggested_template_kind"], "video_template_candidate")

    def test_write_artifacts_outputs_json_and_markdown(self) -> None:
        from ua_workflows.video_enhancer.template_recognition import write_artifacts

        with tempfile.TemporaryDirectory() as tmp:
            data_path, report_path = write_artifacts(
                [{"ad_key": "ad3", "product": "Glam AI", "suggested_template_kind": "image_template_candidate"}],
                target_date="2026-06-15",
                data_dir=Path(tmp),
                reports_dir=Path(tmp),
            )

            self.assertTrue(data_path.exists())
            self.assertTrue(report_path.exists())
            self.assertIn("ad3", report_path.read_text(encoding="utf-8"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_template_recognition.py'`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement template recognition**

Implement row normalization using `cell_to_text`, `resolve_feedback_rating`, and simple heuristic classification. Implement Feishu fetch wrapper using existing `fetch_bitable_records`. Implement CLI with `--date`, `--reviewer`, `--bitable-url`, `--include-legacy`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest discover -s tests -p 'test_ve_template_recognition.py'`

Expected: PASS.

### Task 7: Documentation And Verification

**Files:**
- Modify: `docs/workflows.md`
- Modify: `docs/ve-feedback-training.md`
- Test: existing focused test suite

- [ ] **Step 1: Update docs**

Document `浩鹏评分` / `尉蓝评分`, rating-first fallback, rating-aware feedback training, TopN scoring, and the standalone template-recognition command.

- [ ] **Step 2: Run focused tests**

Run:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_ve_feedback_rating.py'
.venv/bin/python -m unittest discover -s tests -p 'test_ve_feedback_training.py'
.venv/bin/python -m unittest discover -s tests -p 'test_haopeng_ai_filter.py'
.venv/bin/python -m unittest discover -s tests -p 'test_haopeng_topn_push.py'
.venv/bin/python -m unittest discover -s tests -p 'test_ve_template_recognition.py'
.venv/bin/python -m unittest discover -s tests -p 'test_ve_template_normalization.py'
```

Expected: all pass.

- [ ] **Step 3: Run syntax and diff checks**

Run:

```bash
.venv/bin/python -m compileall ua_workflows/video_enhancer scripts/run_ve_template_recognition.py
git diff --check
```

Expected: no syntax errors and no whitespace errors.
