import base64, hashlib, json, sys, tempfile, time, unittest, uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from core import Portal, Problem, digest
from sso import SSO, ISSUER

PROJECT = HERE.parents[1]
ADMIN = "11111111-1111-4111-8111-111111111111"
MEMBER = "22222222-2222-4222-8222-222222222222"


def make_sso(app, base="http://127.0.0.1:8877"):
    cfg = {
        "issuer": ISSUER,
        "client_id": "portal-test",
        "base_url": base,
        "token_endpoint_auth_method": "none",
        "allowed_subjects": [ADMIN, MEMBER],
        "admin_subjects": [ADMIN],
        "legacy_admin_subject": ADMIN,
    }
    path = app.root / "sso.json"
    path.write_text(json.dumps(cfg))
    return SSO(app, path)


class SSOTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(cls.key.public_key()))
        cls.jwk.update(kid="test-key", use="sig", alg="RS256")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Portal(self.temp.name, PROJECT)
        self.old = self.app.login("admin", self.app.credentials["admin_password"], "ip")
        self.old_uid = self.app.user(self.old)["id"]
        self.sso = make_sso(self.app)
        self.codes = {}
        self.calls = []
        self.sso.request_json = self.network

    def tearDown(self):
        self.temp.cleanup()

    def network(self, url, data=None, headers=None):
        self.calls.append((url, data, headers))
        if url.endswith("openid-configuration"):
            return {
                "issuer": ISSUER,
                "code_challenge_methods_supported": ["S256"],
                **{
                    k: ISSUER + "/protocol/openid-connect/" + p
                    for k, p in {
                        "authorization_endpoint": "auth",
                        "token_endpoint": "token",
                        "jwks_uri": "certs",
                        "end_session_endpoint": "logout",
                    }.items()
                },
            }
        if url.endswith("/certs"):
            return {"keys": [self.jwk]}
        if url.endswith("/token"):
            form = parse_qs(data.decode())
            code = form["code"][0]
            v, nonce, sub = self.codes.pop(code)
            self.assertEqual(form["code_verifier"][0], v)
            self.assertEqual(form["redirect_uri"][0], self.sso.callback)
            return {
                "token_type": "Bearer",
                "access_token": "access-test",
                "id_token": self.signed(nonce=nonce, sub=sub),
            }
        raise AssertionError(url)

    def claims(self, **updates):
        now = int(time.time())
        x = {
            "iss": ISSUER,
            "aud": "portal-test",
            "sub": ADMIN,
            "nonce": "nonce",
            "iat": now,
            "exp": now + 300,
            "sid": "test-sid",
        }
        x.update(updates)
        return x

    def signed(self, **updates):
        return jwt.encode(
            self.claims(**updates),
            self.key,
            algorithm="RS256",
            headers={"kid": "test-key"},
        )

    def flow(self, sub=ADMIN):
        url, binding = self.sso.begin()
        q = parse_qs(urlsplit(url).query)
        with self.app.db() as db:
            r = db.execute(
                "SELECT * FROM sso_flows WHERE state=?", (digest(q["state"][0]),)
            ).fetchone()
        code = str(uuid.uuid4())
        self.codes[code] = (r["verifier"], q["nonce"][0], sub)
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(r["verifier"].encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        self.assertEqual(q["code_challenge"], [challenge])
        self.assertEqual(q["code_challenge_method"], ["S256"])
        return {"state": q["state"], "code": [code], "iss": [ISSUER]}, binding

    def test_complete_pkce_login_logout_and_preserve_owner(self):
        self.assertIsNone(self.app.user(self.old))
        q, b = self.flow()
        token = self.sso.callback_login(q, b)
        u = self.sso.user(token)
        self.assertEqual(u["id"], self.old_uid)
        self.assertEqual(u["role"], "admin")
        self.assertEqual(u["auth_provider"], "guru_sso")
        with self.app.db() as db:
            cipher = bytes(
                db.execute("SELECT encrypted_id_token FROM sso_sessions").fetchone()[0]
            )
            self.assertNotIn(b"eyJ", cipher)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM sso_identities").fetchone()[0], 1
            )
        url = self.sso.logout(token)
        self.assertIsNone(self.sso.user(token))
        p = parse_qs(urlsplit(url).query)
        self.assertIn("id_token_hint", p)
        self.sso.complete_logout(p["state"][0])
        with self.assertRaises(Problem):
            self.sso.complete_logout(p["state"][0])

    def test_state_binding_and_callback_replay(self):
        q, b = self.flow()
        for query, binding in [
            ({"state": ["fake"], "code": ["c"]}, b),
            (q, "wrong"),
            ({"state": q["state"] * 2, "code": q["code"]}, b),
        ]:
            with self.assertRaises(Problem):
                self.sso.callback_login(query, binding)
        self.sso.callback_login(q, b)
        with self.assertRaises(Problem):
            self.sso.callback_login(q, b)

    def test_state_expiry_consent_denial_and_failed_code_are_consumed(self):
        q, b = self.flow()
        with self.app.db() as db:
            db.execute("UPDATE sso_flows SET expires=0")
        with self.assertRaises(Problem):
            self.sso.callback_login(q, b)
        q, b = self.flow()
        q["error"] = ["access_denied"]
        with self.assertRaises(Problem):
            self.sso.callback_login(q, b)
        del q["error"]
        with self.assertRaises(Problem):
            self.sso.callback_login(q, b)

    def test_jwt_signature_algorithm_issuer_audience_nonce_expiry(self):
        cases = [
            {"iss": "https://evil.example"},
            {"aud": "other"},
            {"nonce": "other"},
            {"exp": int(time.time()) - 100},
            {"iat": int(time.time()) + 100},
            {"aud": ["portal-test", "other"]},
            {"azp": "other"},
            {"at_hash": "bad"},
        ]
        for change in cases:
            with self.subTest(change=change), self.assertRaises(Problem):
                self.sso.verify(self.signed(**change), "nonce", "access-test")
        forged = jwt.encode(
            self.claims(),
            "attacker-secret-with-32-plus-characters",
            algorithm="HS256",
            headers={"kid": "test-key"},
        )
        with self.assertRaises(Problem):
            self.sso.verify(forged, "nonce", "access-test")
        wrong = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        with self.assertRaises(Problem):
            self.sso.verify(
                jwt.encode(
                    self.claims(), wrong, algorithm="RS256", headers={"kid": "test-key"}
                ),
                "nonce",
                "access-test",
            )

    def test_allowlist_member_and_no_email_admin_linking(self):
        q, b = self.flow(MEMBER)
        token = self.sso.callback_login(q, b)
        self.assertEqual(self.sso.user(token)["role"], "member")
        self.assertNotEqual(self.sso.user(token)["id"], self.old_uid)
        with self.assertRaises(Problem):
            self.sso.create_session(
                self.claims(
                    sub="unknown", email="admin@example.com", preferred_username="admin"
                ),
                "hint",
            )
        self.sso.config["allowed_subjects"].remove(MEMBER)
        self.assertIsNone(self.sso.user(token))

    def test_legacy_session_cannot_bypass_sso(self):
        legacy = self.app.login("admin", self.app.credentials["admin_password"], "ip")
        self.assertIsNone(self.sso.user(legacy))

    def test_discovery_and_public_configuration_fail_closed(self):
        self.sso.request_json = lambda *a, **k: {
            "issuer": ISSUER,
            "authorization_endpoint": "https://evil.example",
        }
        with self.assertRaises(Problem):
            self.sso.metadata()
        p = self.app.root / "sso.json"
        c = json.loads(p.read_text())
        c["base_url"] = "http://0.0.0.0:8877"
        p.write_text(json.dumps(c))
        with self.assertRaises(ValueError):
            SSO(self.app, p)

    def test_jwks_rotation_unknown_kid_is_rejected(self):
        token = jwt.encode(
            self.claims(), self.key, algorithm="RS256", headers={"kid": "rotated"}
        )
        with self.assertRaises(Problem):
            self.sso.verify(token, "nonce", "access-test")
        self.assertEqual(sum(url.endswith("/certs") for url, _, _ in self.calls), 2)


class HTTPFlowTests(unittest.TestCase):
    setUpClass = classmethod(SSOTests.setUpClass.__func__)
    setUp = SSOTests.setUp
    tearDown = SSOTests.tearDown
    network = SSOTests.network
    claims = SSOTests.claims
    signed = SSOTests.signed

    # Exercise complete HTTP boundaries with a local signed provider fixture, no network account writes.
    def test_http_login_cookie_callback_business_and_logout(self):
        import http.client, threading
        from server import Handler, ThreadingHTTPServer

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.app = self.app
        server.sso = self.sso
        self.sso.base = "http://127.0.0.1:" + str(server.server_port)
        self.sso.callback = self.sso.base + "/auth/callback"
        self.sso.logged_out = self.sso.base + "/auth/logged-out"
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def request(path, body=None, cookie="", origin=None):
            c = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            headers = {"Cookie": cookie}
            if body is not None:
                headers["Content-Type"] = "application/json"
            if origin:
                headers["Origin"] = origin
            c.request(
                "POST" if body is not None else "GET",
                path,
                body=json.dumps(body) if body is not None else None,
                headers=headers,
            )
            r = c.getresponse()
            status = r.status
            h = r.getheaders()
            raw = r.read()
            c.close()
            return status, h, raw

        try:
            status, h, raw = request("/auth/login")
            self.assertEqual(status, 303)
            location = dict(h)["Location"]
            cookies = [v for k, v in h if k.lower() == "set-cookie"]
            binding_cookie = cookies[0].split(";")[0]
            self.assertIn("HttpOnly", cookies[0])
            self.assertIn("SameSite=Lax", cookies[0])
            q = parse_qs(urlsplit(location).query)
            with self.app.db() as db:
                r = db.execute(
                    "SELECT * FROM sso_flows WHERE state=?", (digest(q["state"][0]),)
                ).fetchone()
            code = "http-code"
            self.codes[code] = (r["verifier"], q["nonce"][0], ADMIN)
            from urllib.parse import urlencode

            path = "/auth/callback?" + urlencode(
                {"state": q["state"][0], "code": code, "iss": ISSUER}
            )
            status, h, raw = request(path, cookie=binding_cookie)
            self.assertEqual(status, 303)
            self.assertEqual(dict(h)["Location"], "/")
            self.assertEqual(raw, b"")
            session = next(
                v.split(";")[0]
                for k, v in h
                if k.lower() == "set-cookie" and v.startswith("guru_session=")
            )
            self.assertNotIn("eyJ", session)
            self.assertEqual(request("/api/me", cookie=session)[0], 200)
            self.assertEqual(request("/api/materials", cookie=session)[0], 200)
            status, h, _ = request(path, cookie=binding_cookie)
            self.assertEqual(status, 303)
            self.assertEqual(dict(h)["Location"], "/?auth_error=invalid")
            self.assertEqual(
                request("/api/logout", {}, session, "https://evil.example")[0], 403
            )
            status, h, raw = request("/api/logout", {}, session, self.sso.base)
            self.assertEqual(status, 200)
            self.assertEqual(request("/api/me", cookie=session)[0], 401)
            target = json.loads(raw)["logout_url"]
            state = parse_qs(urlsplit(target).query)["state"][0]
            status, h, _ = request("/auth/logged-out?state=" + state)
            self.assertEqual(status, 303)
            self.assertEqual(dict(h)["Location"], "/?signed_out=1")
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
