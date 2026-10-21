import copy, json, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from core import Portal, Problem
from task_intake import TaskIntake


def proposal():
    return {
        "title": "Talkie 女频素材",
        "apps": [{"app_name": "Talkie Lab - AI Playground", "platform": "iOS"}],
        "category": "工具",
        "mode": "素材",
        "media": "视频",
        "ratio": "9:16",
        "channels": ["Facebook", "TikTok", "AppLovin"],
        "languages": ["英语", "土耳其语", "西班牙语", "葡萄牙语", "德语", "韩语"],
        "regions": [],
        "date": {"preset": "30天", "start": None, "end": None},
        "date_semantics": "active_in_window",
        "content": {
            "male_character": True,
            "female_audience": True,
            "exclude_talking_head": True,
        },
        "assumptions": [],
        "questions": [],
        "unsupported": [],
    }


class IntakeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Portal(self.temp.name, HERE.parents[1])
        self.intake = TaskIntake(self.app)
        self.user = {"id": "test-user"}

    def tearDown(self):
        self.temp.cleanup()

    def parse(self, p=None):
        with patch.object(self.intake, "complete", return_value=p or proposal()):
            return self.intake.parse("采集Talkie iOS过去30天的素材", self.user)

    def test_maps_verified_app_id_and_persists_auditable_fields_without_creating_job(
        self,
    ):
        x = self.parse()
        self.assertTrue(x["ready"])
        self.assertEqual(x["scope"]["apps"][0]["app_id"], "6740326134")
        self.assertEqual(x["scope"]["languages"][-1], "韩语")
        self.assertEqual(self.app.jobs(), [])
        provenance = self.intake.provenance(x["draft_id"], self.user)
        jid = self.app.create_job(x["scope"], self.user, provenance)
        self.assertEqual(
            self.app.job(jid)["scope"]["ai_intake"]["request_text"],
            "采集Talkie iOS过去30天的素材",
        )
        self.assertNotIn("ai_intake", self.app.handoff(jid))

    def test_ambiguous_platform_cannot_be_used_to_create_ai_job(self):
        p = proposal()
        p["apps"][0]["platform"] = None
        x = self.parse(p)
        self.assertFalse(x["ready"])
        self.assertTrue(any("平台" in q for q in x["questions"]))
        with self.assertRaises(Problem):
            self.intake.provenance(x["draft_id"], self.user)

    def test_unsupported_conditions_are_not_silently_relaxed(self):
        p = proposal()
        p.update(
            media="图片&视频", date_semantics="first_seen", unsupported=["取前10%"]
        )
        x = self.parse(p)
        self.assertFalse(x["ready"])
        self.assertGreaterEqual(len(x["questions"]), 3)

    def test_unknown_app_has_no_invented_id(self):
        p = proposal()
        p["apps"] = [{"app_name": "New AI Friend", "platform": "Android"}]
        x = self.parse(p)
        self.assertTrue(x["ready"])
        self.assertIsNone(x["scope"]["apps"][0]["app_id"])
        self.assertTrue(x["warnings"])

    def test_schema_rejects_hallucinated_id_and_wrong_boolean(self):
        p = proposal()
        p["apps"][0]["app_id"] = "invented"
        with self.assertRaises(Problem):
            self.parse(p)
        p = proposal()
        p["content"]["male_character"] = "true"
        with self.assertRaises(Problem):
            self.parse(p)

    def test_missing_cli_is_actionable_and_does_not_create_jobs(self):
        with patch("codex_intake.shutil.which", return_value=None):
            with self.assertRaises(Problem) as ctx:
                self.intake.complete("采集Talkie素材")
        self.assertEqual(ctx.exception.status, 503)
        self.assertIn("Codex CLI", ctx.exception.message)
        self.assertEqual(self.app.jobs(), [])

    def test_default_notes_do_not_contradict_explicit_fields(self):
        p = proposal()
        p["assumptions"] = [
            "未指定日期默认30天",
            "未指定尺寸按不限处理",
            "未指定分类按工具处理",
        ]
        x = self.intake.validate(p, "抓取Talkie iOS过去30天的9:16视频")
        self.assertEqual(x["warnings"], ["未指定分类按工具处理"])

    def test_other_user_and_expired_draft_rejected(self):
        x = self.parse()
        with self.assertRaises(Problem):
            self.intake.provenance(x["draft_id"], {"id": "other"})
        with self.app.db() as db:
            db.execute("UPDATE ai_task_drafts SET created=0")
        with self.assertRaises(Problem):
            self.intake.provenance(x["draft_id"], self.user)

    def test_bounds_rate_limit_and_provider_failure_do_not_create_jobs(self):
        for text in ["", None, "x" * 6001]:
            with self.assertRaises(Problem):
                self.intake.parse(text, self.user)
        with patch.object(
            self.intake, "complete", side_effect=Problem("provider failed", 502)
        ):
            with self.assertRaises(Problem):
                self.intake.parse("有效采集需求说明", self.user)
        for _ in range(4):
            self.parse()
        with self.assertRaises(Problem) as ctx:
            self.parse()
        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(self.app.jobs(), [])


if __name__ == "__main__":
    unittest.main()
