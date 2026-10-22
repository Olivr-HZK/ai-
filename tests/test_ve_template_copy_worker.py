from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class VeTemplateCopyWorkerTest(unittest.TestCase):
    def _write_job(self, root: Path, *, source_url: str = "https://example.com/a.mp4") -> Path:
        job = {
            "job_id": "2026-06-15_ad3_test",
            "status": "queued",
            "record_id": "rec3",
            "ad_key": "ad3",
            "task": {
                "record_id": "rec3",
                "ad_key": "ad3",
                "product": "Glam AI",
                "crawl_date": "2026-06-15",
                "rating": 3,
                "rating_label": "3星",
                "suggested_template_kind": "video_template_candidate",
                "template_reason": "素材有动态动作。",
                "core": "自拍变成电影海报",
                "hook": "前 2 秒出现强烈反差",
                "script_or_voiceover": "人物转身并跟随镜头移动",
                "template_fingerprint": "近景自拍转电影感人物海报，镜头轻微推近。",
                "aigc_template_copy_input": {
                    "source_url": source_url,
                    "source_kind": "video",
                    "record_id": "rec3",
                    "ad_key": "ad3",
                    "product": "Glam AI",
                    "suggested_template_kind": "video_template_candidate",
                },
            },
        }
        path = root / "jobs" / "job.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def test_prepare_job_uses_existing_source_and_default_model_ref(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import prepare_template_copy_job

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"fake video")
            model_dir = root / "models"
            model_dir.mkdir()
            model = model_dir / "model.jpg"
            model.write_bytes(b"fake image")
            job_path = self._write_job(root, source_url=str(source))

            prepared = prepare_template_copy_job(
                job_path,
                model_ref_dir=model_dir,
                download=False,
                dry_run=True,
            )
            updated = json.loads(job_path.read_text(encoding="utf-8"))

        self.assertEqual(prepared["status"], "prepared")
        self.assertEqual(Path(prepared["source_path"]), source)
        self.assertEqual(Path(prepared["model_path"]), model)
        self.assertIn("$aigc-template-copy", prepared["codex_prompt"])
        self.assertIn(str(source), prepared["codex_prompt"])
        self.assertIn(str(model), prepared["codex_prompt"])
        self.assertIn("Do not run image-to-video unless", prepared["codex_prompt"])
        self.assertIn("quality_failed", prepared["codex_prompt"])
        self.assertEqual(updated["status"], "prepared")
        self.assertEqual(updated["runner"]["state"], "prepared")

    def test_prepare_job_builds_video_lab_payloads_without_spending(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import (
            build_image_editing_params,
            build_image_to_video_params,
        )

        image_params = build_image_editing_params(
            model_path="/tmp/model.jpg",
            prompt="仅以用户上传的自拍作为身份参考，生成电影海报风格人物图。",
        )
        video_params = build_image_to_video_params(
            first_frame_path="/tmp/effect.png",
            prompt="从首帧开始。人物轻微转头，镜头缓慢推近。",
            duration=5,
        )

        self.assertEqual(image_params["input_data"]["images"][0]["path"], "/tmp/model.jpg")
        self.assertEqual(image_params["generation_config"]["image"]["provider"], "cms")
        self.assertEqual(image_params["generation_config"]["image"]["model"], "gpt-image-2")
        self.assertEqual(image_params["task_config"]["execution_count"], 1)
        self.assertEqual(video_params["input_data"]["images"][0]["path"], "/tmp/effect.png")
        self.assertEqual(video_params["generation_config"]["video"]["provider"], "cms")
        self.assertEqual(video_params["generation_config"]["video"]["model"], "viduq3-turbo")
        self.assertEqual(video_params["generation_config"]["video"]["duration"], 5)

    def test_prepare_job_marks_missing_model_ref(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import TemplateCopyWorkerError, prepare_template_copy_job

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.png"
            source.write_bytes(b"fake image")
            model_dir = root / "models"
            model_dir.mkdir()
            job_path = self._write_job(root, source_url=str(source))

            with self.assertRaises(TemplateCopyWorkerError) as ctx:
                prepare_template_copy_job(job_path, model_ref_dir=model_dir, download=False, dry_run=True)
            updated = json.loads(job_path.read_text(encoding="utf-8"))

        self.assertEqual(ctx.exception.code, "missing_model_ref")
        self.assertEqual(updated["status"], "failed")
        self.assertEqual(updated["runner"]["state"], "missing_model_ref")

    def test_parse_skill_result_reads_codex_marker_json(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import parse_skill_result

        result = parse_skill_result(
            "done\n"
            "VE_TEMPLATE_COPY_RESULT_JSON: "
            '{"status":"completed","quality_verdict":"passed","score":94,'
            '"output_paths":["/tmp/effect.png"],"reason":"usable"}\n'
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["quality_verdict"], "passed")
        self.assertEqual(result["score"], 94)
        self.assertEqual(result["output_paths"], ["/tmp/effect.png"])

    def test_codex_exec_command_places_approval_flag_before_exec(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import build_codex_exec_command

        cmd = build_codex_exec_command(
            codex_executable="/usr/local/bin/codex",
            output_path=Path("/tmp/last.md"),
        )

        self.assertLess(cmd.index("--ask-for-approval"), cmd.index("exec"))
        self.assertNotIn("--ask-for-approval", cmd[cmd.index("exec") :])
        self.assertIn("--output-last-message", cmd)

    def test_codex_exec_command_can_pin_model_before_exec(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import build_codex_exec_command

        cmd = build_codex_exec_command(
            codex_executable="/usr/local/bin/codex",
            output_path=Path("/tmp/last.md"),
            codex_model="gpt-5.5",
        )

        self.assertLess(cmd.index("--model"), cmd.index("exec"))
        self.assertEqual(cmd[cmd.index("--model") + 1], "gpt-5.5")

    def test_codex_exec_command_normalizes_human_model_alias(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import build_codex_exec_command

        cmd = build_codex_exec_command(
            codex_executable="/usr/local/bin/codex",
            output_path=Path("/tmp/last.md"),
            codex_model="gpt5.5",
        )

        self.assertEqual(cmd[cmd.index("--model") + 1], "gpt-5.5")

    def _write_fake_codex(self, root: Path, payload: dict[str, object]) -> Path:
        script = root / "fake_codex.py"
        marker_payload = json.dumps(payload, ensure_ascii=False)
        script.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "args = sys.argv\n"
            "out = args[args.index('--output-last-message') + 1]\n"
            f"message = 'fake complete\\nVE_TEMPLATE_COPY_RESULT_JSON: {marker_payload}\\n'\n"
            "open(out, 'w', encoding='utf-8').write(message)\n"
            "print('fake codex executed')\n",
            encoding="utf-8",
        )
        os.chmod(script, 0o755)
        return script

    def _write_fake_recognition_codex(self, root: Path, payload: dict[str, object]) -> Path:
        script = root / "fake_recognition_codex.py"
        marker_payload = json.dumps(payload, ensure_ascii=False)
        script.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "args = sys.argv\n"
            "out = args[args.index('--output-last-message') + 1]\n"
            f"message = 'fake recognition complete\\nVE_TEMPLATE_RECOGNITION_RESULT_JSON: {marker_payload}\\n'\n"
            "open(out, 'w', encoding='utf-8').write(message)\n"
            "print('fake recognition codex executed')\n",
            encoding="utf-8",
        )
        os.chmod(script, 0o755)
        return script

    def test_execute_prepared_job_marks_completed_from_skill_result(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import (
            execute_prepared_template_copy_job,
            prepare_template_copy_job,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"fake video")
            model_dir = root / "models"
            model_dir.mkdir()
            (model_dir / "model.jpg").write_bytes(b"fake image")
            job_path = self._write_job(root, source_url=str(source))
            prepare_template_copy_job(job_path, model_ref_dir=model_dir, download=False)
            fake_codex = self._write_fake_codex(
                root,
                {
                    "status": "completed",
                    "quality_verdict": "passed",
                    "score": 94,
                    "mode": "video_template",
                    "task_ids": ["image-task", "video-task"],
                    "output_paths": [str(root / "effect.png"), str(root / "effect.mp4")],
                    "reason": "usable as APP template inspiration",
                },
            )

            result = execute_prepared_template_copy_job(job_path, codex_bin=str(fake_codex))
            updated = json.loads(job_path.read_text(encoding="utf-8"))

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["bitable_status"], "已完成")
        self.assertEqual(updated["status"], "completed")
        self.assertEqual(updated["runner"]["state"], "completed")
        self.assertEqual(updated["runner"]["skill_result"]["score"], 94)
        self.assertTrue(Path(updated["runner"]["artifacts"]["skill_result_path"]).exists())

    def test_execute_prepared_job_marks_quality_failed_without_video_success(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import (
            execute_prepared_template_copy_job,
            prepare_template_copy_job,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"fake video")
            model_dir = root / "models"
            model_dir.mkdir()
            (model_dir / "model.jpg").write_bytes(b"fake image")
            job_path = self._write_job(root, source_url=str(source))
            prepare_template_copy_job(job_path, model_ref_dir=model_dir, download=False)
            fake_codex = self._write_fake_codex(
                root,
                {
                    "status": "quality_failed",
                    "quality_verdict": "failed",
                    "score": 62,
                    "mode": "video_template",
                    "task_ids": ["image-task"],
                    "output_paths": [str(root / "first_frame_effect.png")],
                    "reason": "first-frame fidelity below 90; image-to-video was not run",
                },
            )

            result = execute_prepared_template_copy_job(job_path, codex_bin=str(fake_codex))
            updated = json.loads(job_path.read_text(encoding="utf-8"))

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["state"], "quality_failed")
        self.assertEqual(result["bitable_status"], "失败")
        self.assertEqual(updated["status"], "failed")
        self.assertEqual(updated["runner"]["state"], "quality_failed")
        self.assertIn("below 90", updated["runner"]["error"])

    def test_recognition_only_job_creates_reference_artifacts_without_codex(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import run_template_recognition_only

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"fake video")
            job_path = self._write_job(root, source_url=str(source))

            result = run_template_recognition_only(job_path, work_dir=root / "runs", download=False)
            updated = json.loads(job_path.read_text(encoding="utf-8"))
            self.assertTrue(Path(result["reference_screenshots"][0]).exists())
            self.assertTrue(Path(result["reference_video_segments"][0]).exists())

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["state"], "recognition_completed")
        self.assertEqual(result["bitable_status"], "已完成")
        self.assertEqual(result["recognition"]["mode"], "video_template")
        self.assertEqual(result["recognition"]["template_type"], "视频模板")
        self.assertTrue(result["reference_screenshots"])
        self.assertTrue(result["reference_video_segments"])
        self.assertNotIn("codex_exec", updated["runner"])
        self.assertEqual(updated["runner"]["state"], "recognition_completed")
        self.assertEqual(updated["runner"]["recognition"]["template_type"], "视频模板")

    def test_recognition_only_image_source_creates_screenshot_without_video_segment(self) -> None:
        from ua_workflows.video_enhancer.template_copy_worker import run_template_recognition_only

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.png"
            source.write_bytes(b"fake image")
            job_path = self._write_job(root, source_url=str(source))

            result = run_template_recognition_only(job_path, work_dir=root / "runs", download=False)

        self.assertEqual(result["recognition"]["mode"], "image_template")
        self.assertEqual(result["recognition"]["template_type"], "图片模板")
        self.assertTrue(result["reference_screenshots"])
        self.assertEqual(result["reference_video_segments"], [])

    def test_recognition_only_video_source_still_outputs_segment_when_template_is_image(self) -> None:
        from ua_workflows.video_enhancer import template_copy_worker

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"fake video")
            job_path = self._write_job(root, source_url=str(source))
            job = json.loads(job_path.read_text(encoding="utf-8"))
            job["task"]["suggested_template_kind"] = "image_template_candidate"
            job["task"]["template_reason"] = "静态卡片包装。"
            job["task"]["script_or_voiceover"] = ""
            job["task"]["template_fingerprint"] = ""
            job_path.write_text(json.dumps(job, ensure_ascii=False), encoding="utf-8")

            with patch.object(
                template_copy_worker,
                "_extract_reference_frame_at",
                side_effect=lambda source_path, target_path, *, is_video, timestamp=0.0: target_path,
            ) as screenshot, patch.object(
                template_copy_worker,
                "_extract_reference_video_segment_range",
                side_effect=lambda source_path, target_path, *, is_video, start_time=0.0, end_time=None: target_path if is_video else None,
            ) as segment:
                result = template_copy_worker.run_template_recognition_only(
                    job_path,
                    work_dir=root / "runs",
                    download=False,
                )

        self.assertEqual(result["recognition"]["mode"], "image_template")
        self.assertTrue(result["reference_video_segments"])
        self.assertTrue(screenshot.call_args.kwargs["is_video"])
        self.assertTrue(segment.call_args.kwargs["is_video"])

    def test_recognition_only_can_use_codex_skill_groups_for_multiple_artifacts(self) -> None:
        from ua_workflows.video_enhancer import template_copy_worker

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"fake video")
            job_path = self._write_job(root, source_url=str(source))
            fake_codex = self._write_fake_recognition_codex(
                root,
                {
                    "status": "completed",
                    "mode": "video_template",
                    "template_type": "视频模板",
                    "reason": "发现两个不同的成片结果片段，按 skill 规则分别输出。",
                    "groups": [
                        {
                            "kind": "video",
                            "label": "视频模板 1",
                            "timestamp": 1.2,
                            "start_time": 1.2,
                            "end_time": 4.8,
                            "reason": "第一个动态结果片段。",
                        },
                        {
                            "kind": "video",
                            "label": "视频模板 2",
                            "timestamp": 6.0,
                            "start_time": 6.0,
                            "end_time": 9.5,
                            "reason": "第二个动态结果片段。",
                        },
                    ],
                },
            )

            def touch_frame(source_path: Path, target_path: Path, *, is_video: bool, timestamp: float = 0.0) -> Path:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_text(f"frame at {timestamp}", encoding="utf-8")
                return target_path

            def touch_segment(
                source_path: Path,
                target_path: Path,
                *,
                is_video: bool,
                start_time: float = 0.0,
                end_time: float | None = None,
            ) -> Path | None:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_text(f"segment {start_time}-{end_time}", encoding="utf-8")
                return target_path if is_video else None

            with patch.object(template_copy_worker, "_extract_reference_frame_at", side_effect=touch_frame), patch.object(
                template_copy_worker,
                "_extract_reference_video_segment_range",
                side_effect=touch_segment,
            ):
                result = template_copy_worker.run_template_recognition_only(
                    job_path,
                    work_dir=root / "runs",
                    download=False,
                    use_codex_skill=True,
                    codex_bin=str(fake_codex),
                )
                updated = json.loads(job_path.read_text(encoding="utf-8"))

        self.assertEqual(result["recognition"]["template_type"], "视频模板")
        self.assertEqual(len(result["recognition"]["template_groups"]), 2)
        self.assertEqual(len(result["reference_screenshots"]), 2)
        self.assertEqual(len(result["reference_video_segments"]), 2)
        self.assertIn("recognition_prompt_path", updated["runner"]["artifacts"])
        self.assertEqual(updated["runner"]["recognition_codex"]["skill_result"]["mode"], "video_template")


if __name__ == "__main__":
    unittest.main()
