from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


class VeTemplateTriggerTest(unittest.TestCase):
    def test_trigger_job_writes_three_star_or_higher_task(self) -> None:
        from ua_workflows.video_enhancer.template_trigger import trigger_template_copy_job

        rows = [
            {
                "record_id": "rec3",
                "fields": {
                    "广告ID": "ad3",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "3星",
                    "视频链接": "https://example.com/a.mp4",
                    "视频时长": 8,
                    "脚本/口播": "人物转身并跟随镜头移动",
                },
            }
        ]

        with tempfile.TemporaryDirectory() as tmp, patch(
            "ua_workflows.video_enhancer.template_trigger.discover_aigc_template_copy_skill",
            return_value={"installed": True, "path": "/tmp/SKILL.md"},
        ):
            job, path = trigger_template_copy_job(rows, record_id="rec3", job_dir=Path(tmp))
            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(job["status"], "queued")
        self.assertEqual(payload["record_id"], "rec3")
        self.assertEqual(payload["task"]["rating"], 3)
        self.assertEqual(payload["runner"]["type"], "aigc-template-copy")

    def test_trigger_job_rejects_below_three_star_rows(self) -> None:
        from ua_workflows.video_enhancer.template_trigger import TemplateTriggerError, trigger_template_copy_job

        rows = [
            {
                "record_id": "rec2",
                "fields": {
                    "广告ID": "ad2",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "2星",
                    "封面图链接": "https://example.com/b.png",
                },
            }
        ]

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TemplateTriggerError) as ctx:
                trigger_template_copy_job(rows, record_id="rec2", job_dir=Path(tmp))

        self.assertEqual(ctx.exception.code, "not_top_rating")

    def test_build_button_workflow_payload_posts_record_id(self) -> None:
        from ua_workflows.video_enhancer.template_trigger import build_button_workflow_payload

        payload = build_button_workflow_payload(
            trigger_url="https://example.com/trigger",
            table_name="主表",
            token="secret",
            client_token="client1",
        )
        steps = {step["id"]: step for step in payload["steps"]}
        raw_body = steps["step_call_template_trigger"]["data"]["raw_body"]

        self.assertEqual(payload["client_token"], "client1")
        self.assertEqual(steps["step_button_trigger"]["type"], "ButtonTrigger")
        self.assertIn({"value_type": "ref", "value": "$.step_button_trigger.recordId"}, raw_body)
        self.assertIn({"value_type": "text", "value": "secret"}, raw_body)

    def test_build_rating_link_workflow_payload_posts_record_id_without_triggering_job(self) -> None:
        from ua_workflows.video_enhancer.template_trigger import build_rating_link_workflow_payload

        payload = build_rating_link_workflow_payload(
            ensure_link_url="https://example.com/ensure-link",
            table_name="ai工具video photo爬取表 副本",
            rating_field_name="浩鹏评分",
            token="secret",
            client_token="client-rating",
        )
        steps = {step["id"]: step for step in payload["steps"]}
        trigger = steps["step_rating_trigger"]
        http = steps["step_ensure_template_link"]

        self.assertEqual(payload["client_token"], "client-rating")
        self.assertEqual(trigger["type"], "SetRecordTrigger")
        self.assertEqual(trigger["data"]["table_name"], "ai工具video photo爬取表 副本")
        self.assertEqual(
            trigger["data"]["field_watch_info"],
            [
                {
                    "field_name": "浩鹏评分",
                    "operator": "isGreaterEqual",
                    "value": [{"value_type": "number", "value": 3}],
                }
            ],
        )
        self.assertEqual(http["type"], "HTTPClientAction")
        self.assertEqual(http["data"]["url"], [{"value_type": "text", "value": "https://example.com/ensure-link"}])
        self.assertIn({"value_type": "ref", "value": "$.step_rating_trigger.recordId"}, http["data"]["raw_body"])
        self.assertIn({"value_type": "text", "value": '"source":"lark_base_rating_update"'}, http["data"]["raw_body"])
        self.assertIn({"value_type": "text", "value": "secret"}, http["data"]["raw_body"])

    def test_build_trigger_link_updates_filters_three_star_or_higher_target_date(self) -> None:
        from ua_workflows.video_enhancer.template_trigger import build_trigger_link_updates

        rows = [
            {
                "record_id": "rec3",
                "fields": {
                    "广告ID": "ad3",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "3星",
                    "封面图链接": "https://example.com/a.png",
                },
            },
            {
                "record_id": "rec2",
                "fields": {
                    "广告ID": "ad2",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "4星",
                    "封面图链接": "https://example.com/b.png",
                },
            },
            {
                "record_id": "rec_old",
                "fields": {
                    "广告ID": "ad_old",
                    "抓取日期": "2026-06-14",
                    "浩鹏评分": "5星",
                    "封面图链接": "https://example.com/c.png",
                },
            },
        ]

        updates = build_trigger_link_updates(
            rows,
            trigger_url="http://127.0.0.1:8765/trigger",
            target_date="2026-06-15",
            token="secret",
        )

        self.assertEqual([item["record_id"] for item in updates], ["rec3", "rec2"])
        trigger_link = updates[0]["fields"]["模板复刻触发链接"]
        self.assertEqual(trigger_link["text"], "触发模板复刻")
        self.assertIn("record_id=rec3", trigger_link["link"])
        self.assertIn("token=secret", trigger_link["link"])
        self.assertEqual(updates[0]["fields"]["模板复刻状态"], "待触发")

    def test_build_trigger_link_updates_can_preseed_links_for_unrated_rows(self) -> None:
        from ua_workflows.video_enhancer.template_trigger import build_trigger_link_updates

        rows = [
            {
                "record_id": "rec_blank",
                "fields": {
                    "广告ID": "ad_blank",
                    "抓取日期": "2026-06-15 00:00:00",
                    "封面图链接": "https://example.com/a.png",
                },
            },
            {
                "record_id": "rec_low",
                "fields": {
                    "广告ID": "ad_low",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "2星",
                    "封面图链接": "https://example.com/b.png",
                },
            },
            {
                "record_id": "rec_old",
                "fields": {
                    "广告ID": "ad_old",
                    "抓取日期": "2026-06-14",
                    "封面图链接": "https://example.com/c.png",
                },
            },
        ]

        updates = build_trigger_link_updates(
            rows,
            trigger_url="https://example.com/trigger",
            target_date="2026-06-15",
            token="secret",
            include_all_records=True,
            link_only=True,
        )

        self.assertEqual([item["record_id"] for item in updates], ["rec_blank", "rec_low"])
        fields = updates[0]["fields"]
        self.assertEqual(set(fields), {"模板复刻触发链接"})
        self.assertEqual(fields["模板复刻触发链接"]["text"], "触发模板复刻")
        self.assertIn("record_id=rec_blank", fields["模板复刻触发链接"]["link"])
        self.assertIn("token=secret", fields["模板复刻触发链接"]["link"])

    def test_ensure_trigger_link_for_record_writes_link_only_when_rating_is_high_enough(self) -> None:
        from ua_workflows.video_enhancer import template_trigger

        record = {
            "record_id": "rec3",
            "fields": {
                "广告ID": "ad3",
                "抓取日期": "2026-06-15",
                "浩鹏评分": 3,
                "封面图链接": "https://example.com/a.png",
            },
        }

        with patch.object(template_trigger, "load_project_env"), patch.object(
            template_trigger,
            "fetch_bitable_record",
            return_value=record,
        ), patch.object(template_trigger, "update_template_trigger_link", return_value={"updated": True}) as update:
            result = template_trigger.ensure_template_trigger_link_from_bitable(
                bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
                record_id="rec3",
                trigger_url="https://example.com/trigger",
                token="secret",
            )

        self.assertTrue(result["eligible"])
        self.assertTrue(result["updated"])
        update.assert_called_once_with(
            bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
            record_id="rec3",
            trigger_url="https://example.com/trigger?record_id=rec3&token=secret",
        )

    def test_ensure_trigger_link_for_record_skips_low_rating_without_writing(self) -> None:
        from ua_workflows.video_enhancer import template_trigger

        record = {
            "record_id": "rec2",
            "fields": {
                "广告ID": "ad2",
                "抓取日期": "2026-06-15",
                "浩鹏评分": 2,
                "封面图链接": "https://example.com/a.png",
            },
        }

        with patch.object(template_trigger, "load_project_env"), patch.object(
            template_trigger,
            "fetch_bitable_record",
            return_value=record,
        ), patch.object(template_trigger, "update_template_trigger_link") as update:
            result = template_trigger.ensure_template_trigger_link_from_bitable(
                bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
                record_id="rec2",
                trigger_url="https://example.com/trigger",
                token="secret",
            )

        self.assertFalse(result["eligible"])
        self.assertFalse(result["updated"])
        self.assertEqual(result["code"], "not_top_rating")
        update.assert_not_called()

    def test_trigger_from_bitable_fetches_single_record_when_record_id_is_present(self) -> None:
        from ua_workflows.video_enhancer import template_trigger

        record = {
            "record_id": "rec3",
            "fields": {
                "广告ID": "ad3",
                "抓取日期": "2026-06-15",
                "浩鹏评分": "3星",
                "封面图链接": "https://example.com/a.png",
            },
        }

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            template_trigger,
            "fetch_bitable_record",
            return_value=record,
        ) as fetch_one, patch.object(
            template_trigger,
            "fetch_bitable_records",
            side_effect=AssertionError("should not fetch all records"),
        ):
            job, _ = template_trigger.trigger_template_copy_from_bitable(
                bitable_url="https://example.feishu.cn/base/app?table=tbl",
                record_id="rec3",
                job_dir=Path(tmp),
            )

        fetch_one.assert_called_once()
        self.assertEqual(job["record_id"], "rec3")

    def test_server_optional_worker_prepares_job_only_when_enabled(self) -> None:
        from ua_workflows.video_enhancer import template_trigger_server

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            template_trigger_server,
            "prepare_template_copy_job",
            return_value={"status": "prepared"},
        ) as prepare:
            path = Path(tmp) / "job.json"
            disabled = template_trigger_server.run_optional_worker(
                path,
                auto_prepare=False,
            )
            enabled = template_trigger_server.run_optional_worker(
                path,
                auto_prepare=True,
                model_ref_dir=Path(tmp),
                work_dir=Path(tmp) / "runs",
            )

        self.assertEqual(disabled, {"skipped": True})
        self.assertEqual(enabled["status"], "prepared")
        prepare.assert_called_once()

    def test_server_optional_worker_executes_codex_when_enabled(self) -> None:
        from ua_workflows.video_enhancer import template_trigger_server

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            template_trigger_server,
            "prepare_template_copy_job",
            return_value={"status": "prepared"},
        ) as prepare, patch.object(
            template_trigger_server,
            "execute_prepared_template_copy_job",
            return_value={"status": "completed", "state": "completed", "bitable_status": "已完成"},
        ) as execute:
            path = Path(tmp) / "job.json"
            result = template_trigger_server.run_optional_worker(
                path,
                auto_prepare=True,
                auto_execute_codex=True,
                codex_bin="/tmp/fake-codex",
                codex_model="gpt5.5",
                model_ref_dir=Path(tmp),
                work_dir=Path(tmp) / "runs",
            )

        self.assertEqual(result["status"], "completed")
        prepare.assert_called_once()
        execute.assert_called_once_with(path, codex_bin="/tmp/fake-codex", codex_model="gpt5.5")

    def test_update_template_copy_status_writes_job_id_to_bitable(self) -> None:
        from ua_workflows.video_enhancer import template_trigger

        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {"code": 0, "data": {}}

        with patch.object(template_trigger, "load_project_env"), patch.object(
            template_trigger,
            "_tenant_access_token",
            return_value="tenant-token",
        ), patch.object(template_trigger.requests, "post", return_value=FakeResponse()) as post:
            result = template_trigger.update_template_copy_status(
                bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
                record_id="rec3",
                status="已提交",
                job_id="job3",
            )

        self.assertTrue(result["updated"])
        _, kwargs = post.call_args
        self.assertEqual(
            kwargs["json"],
            {
                "records": [
                    {
                        "record_id": "rec3",
                        "fields": {"模板复刻状态": "已提交", "模板复刻任务ID": "job3"},
                    }
                ]
            },
        )

    def test_server_updates_bitable_status_after_worker_prepare(self) -> None:
        from ua_workflows.video_enhancer import template_trigger_server

        handler = object.__new__(template_trigger_server.TemplateTriggerHandler)
        handler.server = SimpleNamespace(
            trigger_token="",
            bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
            reviewer="haopeng",
            include_legacy=False,
            job_dir=Path("/tmp/jobs"),
            auto_prepare=True,
            model_ref_dir=Path("/tmp/model_refs"),
            work_dir=Path("/tmp/runs"),
        )
        handler.headers = {}

        job = {"job_id": "job3", "record_id": "rec3"}
        with patch.object(
            template_trigger_server,
            "trigger_template_copy_from_bitable",
            return_value=(job, Path("/tmp/jobs/job3.json")),
        ), patch.object(
            template_trigger_server,
            "run_optional_worker",
            return_value={"status": "prepared"},
        ), patch.object(
            template_trigger_server,
            "update_template_copy_status",
            return_value={"updated": True},
        ) as update_status:
            result, status = handler._trigger({"record_id": "rec3"})

        self.assertEqual(status, 200)
        self.assertTrue(result["success"])
        self.assertEqual(result["bitable_update"], {"updated": True})
        update_status.assert_called_once_with(
            bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
            record_id="rec3",
            status="已提交",
            job_id="job3",
        )

    def test_server_updates_bitable_status_from_codex_result(self) -> None:
        from ua_workflows.video_enhancer import template_trigger_server

        handler = object.__new__(template_trigger_server.TemplateTriggerHandler)
        handler.server = SimpleNamespace(
            trigger_token="",
            bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
            reviewer="haopeng",
            include_legacy=False,
            job_dir=Path("/tmp/jobs"),
            auto_prepare=True,
            auto_execute_codex=True,
            codex_bin="/tmp/fake-codex",
            model_ref_dir=Path("/tmp/model_refs"),
            work_dir=Path("/tmp/runs"),
        )
        handler.headers = {}

        job = {"job_id": "job3", "record_id": "rec3"}
        with patch.object(
            template_trigger_server,
            "trigger_template_copy_from_bitable",
            return_value=(job, Path("/tmp/jobs/job3.json")),
        ), patch.object(
            template_trigger_server,
            "run_optional_worker",
            return_value={"status": "failed", "state": "quality_failed", "bitable_status": "失败"},
        ), patch.object(
            template_trigger_server,
            "update_template_copy_status",
            return_value={"updated": True},
        ) as update_status:
            result, status = handler._trigger({"record_id": "rec3"})

        self.assertEqual(status, 200)
        self.assertTrue(result["success"])
        self.assertEqual(result["job_status"], "failed")
        self.assertEqual(result["bitable_update"], {"updated": True})
        update_status.assert_called_once_with(
            bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
            record_id="rec3",
            status="失败",
            job_id="job3",
        )

    def test_server_ensure_link_updates_bitable_without_triggering_job(self) -> None:
        from ua_workflows.video_enhancer import template_trigger_server

        handler = object.__new__(template_trigger_server.TemplateTriggerHandler)
        handler.server = SimpleNamespace(
            trigger_token="",
            bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
            reviewer="haopeng",
            include_legacy=False,
            public_trigger_url="https://example.com/trigger",
        )
        handler.headers = {}

        with patch.object(
            template_trigger_server,
            "ensure_template_trigger_link_from_bitable",
            return_value={"eligible": True, "updated": True, "record_id": "rec3"},
        ) as ensure, patch.object(
            template_trigger_server,
            "trigger_template_copy_from_bitable",
            side_effect=AssertionError("ensure-link must not trigger copy job"),
        ):
            result, status = handler._ensure_link({"record_id": "rec3"})

        self.assertEqual(status, 200)
        self.assertTrue(result["success"])
        ensure.assert_called_once_with(
            bitable_url="https://example.feishu.cn/base/app_token?table=tbl123",
            record_id="rec3",
            trigger_url="https://example.com/trigger",
            reviewer="haopeng",
            include_legacy=False,
            token="",
        )


if __name__ == "__main__":
    unittest.main()
