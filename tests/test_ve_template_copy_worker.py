from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
