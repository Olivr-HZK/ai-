import concurrent.futures, copy, importlib.util, json, sys, tempfile, threading, unittest, urllib.request, urllib.error
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
PROJECT = HERE.parents[1]
sys.path.insert(0, str(HERE))
from core import Portal, Problem, normalize_scope
from server import Handler, ThreadingHTTPServer
from test_sso import make_sso, ADMIN
from task_intake import TaskIntake


class PortalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Portal(self.temp.name, PROJECT)
        self.token = self.app.login(
            "admin", self.app.credentials["admin_password"], "local"
        )
        self.user = self.app.user(self.token)

    def tearDown(self):
        self.temp.cleanup()

    def scope(self):
        return {
            "apps": [{"app_name": "Talkie Lab - AI Playground", "platform": "iOS"}],
            "date": {"preset": "30天"},
            "channels": ["TikTok"],
            "languages": ["英语"],
            "media": "视频",
        }

    def test_scope_known_app_platform_and_unlimited(self):
        x = normalize_scope(self.scope(), self.app.registry)
        self.assertEqual(x["apps"][0]["app_id"], "6740326134")
        self.assertEqual(x["regions"], [])
        self.assertEqual(x["selection"], "all_matching")
        s = self.scope()
        s["apps"].append(s["apps"][0].copy())
        with self.assertRaises(Problem):
            normalize_scope(s, self.app.registry)

    def test_dates_and_media_validation(self):
        s = self.scope()
        s["date"] = {"preset": "自定义", "start": "2026-10-20", "end": "2026-10-01"}
        with self.assertRaises(Problem):
            normalize_scope(s, self.app.registry)
        s = self.scope()
        s["media"] = "图片"
        with self.assertRaises(Problem):
            normalize_scope(s, self.app.registry)

    def test_login_invite_and_sessions(self):
        with self.assertRaises(Problem):
            self.app.login("admin", "wrong", "wrongip")
        invite = self.app.invite()["invite"]
        self.app.register(
            {
                "username": "colleague",
                "password": "test-long-password-123",
                "invite": invite,
            }
        )
        tok = self.app.login("colleague", "test-long-password-123", "memberip")
        self.assertEqual(self.app.user(tok)["role"], "member")
        self.assertIsNone(self.app.user("fake"))
        with self.assertRaises(Problem):
            self.app.register(
                {
                    "username": "nobody",
                    "password": "test-long-password-123",
                    "invite": "fake",
                }
            )

    def test_atomic_claim_and_no_auto_release_after_lease_expiry(self):
        ids = [self.app.create_job(self.scope(), self.user) for _ in range(2)]

        def claim(_):
            try:
                return self.app.claim()["id"]
            except Problem:
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            result = list(pool.map(claim, range(2)))
        self.assertEqual(sum(x is not None for x in result), 1)
        with self.app.db() as db:
            db.execute("UPDATE jobs SET lease_until=0")
        with self.assertRaises(Problem):
            self.app.claim()

    def test_stage_transition_and_evidence_completion_guard(self):
        jid = self.app.create_job(self.scope(), self.user)
        with self.assertRaises(Problem):
            self.app.advance(jid, "completed")
        with self.assertRaises(Problem):
            self.app.advance(jid, "reviewing")
        self.app.claim(jid)
        self.app.advance(jid, "configuring")
        self.app.advance(jid, "collecting", {"found": 3})
        with self.assertRaises(Problem):
            self.app.advance(jid, "collecting", {"found": 2})
        self.assertEqual(self.app.job(jid)["status"], "collecting")

    def test_handoff_keeps_normal_window_and_no_private_api(self):
        jid = self.app.create_job(self.scope(), self.user)
        p = self.app.handoff(jid)
        self.assertIn("不能套用单日首次发现截止规则", p)
        self.assertIn("不使用广大大私有接口", p)
        self.assertIn("6740326134", p)


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.app = Portal(cls.temp.name, PROJECT)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.app = cls.app
        cls.server.intake = TaskIntake(cls.app)
        cls.server.sso = make_sso(
            cls.app, "http://127.0.0.1:" + str(cls.server.server_port)
        )
        cls.url = "http://127.0.0.1:" + str(cls.server.server_port)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.temp.cleanup()

    def req(self, path, body=None, headers=None):
        h = headers or {}
        if body is not None:
            h = {**h, "Content-Type": "application/json"}
        req = urllib.request.Request(
            self.url + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers=h,
        )
        try:
            r = urllib.request.urlopen(req, timeout=5)
        except urllib.error.HTTPError as e:
            r = e
        return r.status, r.headers, r.read()

    def auth(self):
        token = self.server.sso.create_session({"sub": ADMIN}, "test-id-token")
        return {"Cookie": "guru_session=" + token}

    def test_unauthenticated_api_media_and_traversal_blocked(self):
        for path in [
            "/api/materials",
            "/api/jobs",
            "/media/00000000-0000-0000-0000-000000000000/video.mp4",
            "/.env",
            "/../../.env",
        ]:
            self.assertEqual(self.req(path)[0], 401)
        self.assertEqual(self.req("/api/health")[0], 200)

    def test_csrf_and_admin_only_worker_route(self):
        h = self.auth()
        self.assertEqual(
            self.req(
                "/api/jobs", {"apps": []}, {**h, "Origin": "https://evil.example"}
            )[0],
            403,
        )
        self.assertEqual(self.req("/api/worker/claim", {}, h)[0], 401)

    def test_range_and_head_semantics(self):
        h = self.auth()
        s, headers, body = self.req("/style.css", headers={**h, "Range": "bytes=0-9"})
        self.assertEqual(s, 206)
        self.assertEqual(len(body), 10)
        self.assertTrue(headers["Content-Range"].startswith("bytes 0-9/"))
        self.assertEqual(
            self.req("/style.css", headers={"Range": "bytes=99999999-"})[0], 416
        )

    def test_ai_intake_requires_login_and_submits_verified_draft(self):
        from unittest.mock import patch
        from test_task_intake import proposal

        self.assertEqual(
            self.req("/api/task-intake", {"text": "采集Talkie iOS素材"})[0], 401
        )
        h = self.auth()
        with patch.object(self.server.intake, "complete", return_value=proposal()):
            status, _, raw = self.req(
                "/api/task-intake", {"text": "采集Talkie iOS过去30天素材"}, h
            )
        self.assertEqual(status, 200)
        result = json.loads(raw)
        scope = result["scope"]
        scope["ai_draft_id"] = result["draft_id"]
        status, _, raw = self.req("/api/jobs", scope, h)
        self.assertEqual(status, 201)
        jid = json.loads(raw)["id"]
        self.assertIn("ai_intake", self.app.job(jid)["scope"])
        self.assertEqual(self.req("/api/jobs/" + jid + "/cancel", {}, h)[0], 200)

    def test_password_entry_retired_and_no_secret_static_route(self):
        for path in ["/api/login", "/api/register", "/api/invites"]:
            self.assertEqual(
                self.req(
                    path,
                    {
                        "username": "admin",
                        "password": self.app.credentials["admin_password"],
                    },
                )[0],
                410,
            )
        self.assertEqual(self.req("/credentials.json", headers=self.auth())[0], 404)

    def test_member_cannot_claim_and_owner_can_create_cancel(self):
        from test_sso import MEMBER

        token = self.server.sso.create_session({"sub": MEMBER}, "test-token")
        h = {"Cookie": "guru_session=" + token}
        scope = {
            "apps": [{"app_name": "Talkie Lab - AI Playground", "platform": "iOS"}],
            "date": {"preset": "30天"},
        }
        status, _, raw = self.req("/api/jobs", scope, h)
        self.assertEqual(status, 201)
        jid = json.loads(raw)["id"]
        self.assertEqual(self.req("/api/jobs/" + jid + "/claim", {}, h)[0], 403)
        self.assertEqual(self.req("/api/jobs/" + jid + "/cancel", {}, h)[0], 200)

    def test_local_logout_revokes_server_session(self):
        h = self.auth()
        self.assertEqual(self.req("/api/me", headers=h)[0], 200)
        status, headers, raw = self.req("/api/logout", {}, h)
        self.assertEqual(status, 200)
        self.assertIn("Max-Age=0", headers["Set-Cookie"])
        self.assertIn("HttpOnly", headers["Set-Cookie"])
        self.assertIn("SameSite=Lax", headers["Set-Cookie"])
        self.assertEqual(self.req("/api/me", headers=h)[0], 401)
        self.assertTrue(
            json.loads(raw)["logout_url"].startswith(self.server.sso.issuer + "/")
        )


if __name__ == "__main__":
    unittest.main()
