"""Guru OIDC boundary: code+S256, signed identity, opaque server sessions.

No password fallback, browser tokens, implicit grant or email-based account linking.
"""

from __future__ import annotations
import base64, hashlib, hmac, json, os, secrets, sqlite3, stat, threading, time, uuid
from pathlib import Path
from urllib.parse import urlencode, urlsplit, quote
from urllib.request import Request, build_opener, HTTPRedirectHandler
import jwt
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from core import Problem, digest

ISSUER = "https://auth.upupup.today/realms/guru"
ALGOS = ["RS256"]
SESSION_SECONDS = 8 * 3600
FLOW_SECONDS = 300


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class SSO:
    def __init__(self, app, config_path):
        self.app = app
        self.config_path = Path(config_path)
        self.config = json.loads(self.config_path.read_text())
        c = self.config
        self.issuer = c.get("issuer")
        self.client_id = c.get("client_id")
        self.base = c.get("base_url", "").rstrip("/")
        p = urlsplit(self.base)
        if (
            self.issuer != ISSUER
            or not isinstance(self.client_id, str)
            or not self.client_id
        ):
            raise ValueError("Guru SSO issuer/client_id配置无效")
        if (
            p.path
            or p.query
            or p.fragment
            or p.username
            or p.password
            or not p.hostname
        ):
            raise ValueError("base_url必须是无路径的精确origin")
        self.local = p.scheme == "http" and p.hostname == "127.0.0.1"
        self.method = c.get("token_endpoint_auth_method")
        if self.local:
            if self.method != "none":
                raise ValueError("本机回环客户端必须使用PKCE public client")
        elif p.scheme != "https" or self.method != "client_secret_basic":
            raise ValueError("公网必须使用HTTPS和机密客户端")
        if (
            not c.get("admin_subjects")
            or not isinstance(c.get("allowed_subjects"), list)
            or not set(c["admin_subjects"]) <= set(c["allowed_subjects"])
        ):
            raise ValueError("必须显式配置允许访问和管理员的SSO subject")
        if any(not isinstance(s, str) or not s for s in c["allowed_subjects"]):
            raise ValueError("subject清单无效")
        self.secret = None
        if self.method == "client_secret_basic":
            f = Path(c["client_secret_file"]).expanduser()
            st = f.stat()
            if (
                not stat.S_ISREG(st.st_mode)
                or st.st_mode & 0o077
                or st.st_uid != os.getuid()
            ):
                raise ValueError("Client Secret必须来自当前用户的0600文件")
            secret = json.loads(f.read_text())
            if (
                secret.get("clientId") != self.client_id
                or secret.get("tokenEndpointAuthMethod") != self.method
            ):
                raise ValueError("Client Secret文件与客户端不匹配")
            self.secret = secret["clientSecret"]
        self.callback = self.base + "/auth/callback"
        self.logged_out = self.base + "/auth/logged-out"
        self.opener = build_opener(NoRedirect())
        self.lock = threading.RLock()
        self.cached = {}
        self.cache_at = 0
        key = self.app.root / "sso-session.key"
        if not key.exists():
            try:
                fd = os.open(key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as f:
                    f.write(AESGCM.generate_key(256))
            except FileExistsError:
                pass
        st = key.stat()
        if st.st_mode & 0o077 or st.st_uid != os.getuid():
            raise ValueError("SSO会话密钥文件权限无效")
        self.cipher = AESGCM(key.read_bytes())
        with app.db() as db:
            db.executescript(
                """CREATE TABLE IF NOT EXISTS sso_identities(issuer TEXT NOT NULL,subject TEXT NOT NULL,user_id TEXT NOT NULL,display_name TEXT,PRIMARY KEY(issuer,subject),UNIQUE(user_id));
CREATE TABLE IF NOT EXISTS sso_sessions(token TEXT PRIMARY KEY,encrypted_id_token BLOB NOT NULL,sid TEXT);
CREATE TABLE IF NOT EXISTS sso_flows(state TEXT PRIMARY KEY,binding TEXT NOT NULL,verifier TEXT NOT NULL,nonce TEXT NOT NULL,expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sso_logout_flows(state TEXT PRIMARY KEY,expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sso_migrations(name TEXT PRIMARY KEY,at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS auth_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL,user_id TEXT,at REAL NOT NULL);"""
            )
            if "display_name" not in {
                r["name"] for r in db.execute("PRAGMA table_info(sso_identities)")
            }:
                db.execute("ALTER TABLE sso_identities ADD COLUMN display_name TEXT")
            if not db.execute(
                "SELECT 1 FROM sso_migrations WHERE name='retire-password-sessions-v1'"
            ).fetchone():
                db.execute("DELETE FROM sessions")
                db.execute("DELETE FROM invites")
                db.execute(
                    "INSERT INTO sso_migrations VALUES('retire-password-sessions-v1',?)",
                    (time.time(),),
                )

    def request_json(self, url, data=None, headers=None):
        # Network destinations come exclusively from pinned issuer endpoints. Never follow redirects.
        if not url.startswith(self.issuer + "/"):
            raise Problem("SSO端点校验失败", 503)
        try:
            req = Request(
                url,
                data=data,
                headers={"Accept": "application/json", **(headers or {})},
            )
            with self.opener.open(req, timeout=20) as r:
                raw = r.read(1048577)
                if len(raw) > 1048576:
                    raise ValueError("large")
                x = json.loads(raw)
                if not isinstance(x, dict):
                    raise ValueError("object")
                return x
        except Exception:
            raise Problem(
                "Guru SSO暂时不可用，请重新登录；请勿重复刷新回调页", 503
            ) from None

    def metadata(self):
        with self.lock:
            if self.cached and time.time() - self.cache_at < 300:
                return self.cached
            x = self.request_json(self.issuer + "/.well-known/openid-configuration")
            paths = {
                "authorization_endpoint": "auth",
                "token_endpoint": "token",
                "jwks_uri": "certs",
                "end_session_endpoint": "logout",
            }
            if (
                x.get("issuer") != self.issuer
                or any(
                    x.get(k) != self.issuer + "/protocol/openid-connect/" + v
                    for k, v in paths.items()
                )
                or "S256" not in x.get("code_challenge_methods_supported", [])
            ):
                raise Problem("Guru SSO发现文档不符合固定合同", 503)
            self.cached = x
            self.cache_at = time.time()
            return x

    def signing_key(self, token, force=False):
        try:
            h = jwt.get_unverified_header(token)
        except jwt.PyJWTError:
            raise Problem("SSO身份令牌无效", 401) from None
        if (
            h.get("alg") not in ALGOS
            or not isinstance(h.get("kid"), str)
            or len(h["kid"]) > 200
            or h.get("crit")
        ):
            raise Problem("SSO签名算法无效", 401)
        # A bounded JWKS cache; an unknown kid triggers one rotation refresh.
        with self.lock:
            if (
                force
                or not getattr(self, "jwks", None)
                or time.time() - getattr(self, "jwks_at", 0) > 300
            ):
                self.jwks = self.request_json(self.metadata()["jwks_uri"]).get(
                    "keys", []
                )
                self.jwks_at = time.time()
            keys = [
                k
                for k in self.jwks
                if k.get("kid") == h["kid"]
                and k.get("kty") == "RSA"
                and k.get("use", "sig") == "sig"
                and k.get("alg", "RS256") == "RS256"
            ]
            if not keys and not force:
                return self.signing_key(token, True)
            if len(keys) != 1:
                raise Problem("SSO签名密钥无效", 401)
            try:
                key = jwt.PyJWK.from_dict(keys[0], algorithm="RS256").key
                if key.key_size < 2048:
                    raise ValueError("RSA key too small")
                return key
            except (ValueError, jwt.PyJWTError):
                raise Problem("SSO签名密钥无效", 401) from None

    def verify(self, token, nonce, access_token):
        if not isinstance(token, str) or len(token) > 32768:
            raise Problem("SSO身份令牌无效", 401)
        try:
            x = jwt.decode(
                token,
                self.signing_key(token),
                algorithms=ALGOS,
                audience=self.client_id,
                issuer=self.issuer,
                leeway=30,
                options={"require": ["iss", "sub", "aud", "exp", "iat", "nonce"]},
            )
            if (
                not isinstance(x["sub"], str)
                or not x["sub"]
                or not hmac.compare_digest(str(x["nonce"]), nonce)
            ):
                raise ValueError("nonce")
            if (
                isinstance(x["aud"], list)
                and len(x["aud"]) > 1
                and x.get("azp") != self.client_id
            ):
                raise ValueError("azp")
            if (
                x.get("azp", self.client_id) != self.client_id
                or time.time() - x["iat"] > 600
            ):
                raise ValueError("azp/iat")
            if "at_hash" in x:
                hashed = (
                    base64.urlsafe_b64encode(
                        hashlib.sha256(access_token.encode()).digest()[:16]
                    )
                    .rstrip(b"=")
                    .decode()
                )
                if not hmac.compare_digest(hashed, x["at_hash"]):
                    raise ValueError("at_hash")
            return x
        except (jwt.PyJWTError, ValueError, TypeError, KeyError):
            raise Problem("SSO身份验证失败，请重新登录", 401) from None

    def begin(self):
        meta = self.metadata()
        state = secrets.token_urlsafe(32)
        binding = secrets.token_urlsafe(32)
        verifier = secrets.token_urlsafe(48)
        nonce = secrets.token_urlsafe(32)
        with self.app.db() as db:
            db.execute("DELETE FROM sso_flows WHERE expires<?", (time.time(),))
            if db.execute("SELECT COUNT(*) FROM sso_flows").fetchone()[0] >= 1000:
                raise Problem("登录请求过多，请稍后重试", 429)
            db.execute(
                "INSERT INTO sso_flows VALUES(?,?,?,?,?)",
                (
                    digest(state),
                    digest(binding),
                    verifier,
                    nonce,
                    time.time() + FLOW_SECONDS,
                ),
            )
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        return (
            meta["authorization_endpoint"]
            + "?"
            + urlencode(
                {
                    "client_id": self.client_id,
                    "response_type": "code",
                    "redirect_uri": self.callback,
                    "scope": "openid profile email",
                    "state": state,
                    "nonce": nonce,
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                }
            ),
            binding,
        )

    def callback_login(self, q, binding):
        if any(len(v) != 1 for v in q.values()) or not q.get("state") or not binding:
            raise Problem("SSO回调缺少浏览器验证信息，请重新登录", 401)
        state = q["state"][0]
        with self.app.db() as db:
            db.execute("BEGIN IMMEDIATE")
            r = db.execute(
                "SELECT * FROM sso_flows WHERE state=?", (digest(state),)
            ).fetchone()
            if (
                not r
                or r["expires"] < time.time()
                or not hmac.compare_digest(r["binding"], digest(binding))
            ):
                raise Problem("SSO登录已过期或浏览器不匹配，请重新登录", 401)
            db.execute("DELETE FROM sso_flows WHERE state=?", (digest(state),))
        if q.get("error"):
            raise Problem("Guru SSO授权被取消或拒绝，请重新登录", 401)
        if (
            not q.get("code")
            or len(q["code"][0]) > 4096
            or q.get("iss", [self.issuer])[0] != self.issuer
        ):
            raise Problem("SSO授权码或issuer无效", 401)
        form = {
            "grant_type": "authorization_code",
            "code": q["code"][0],
            "redirect_uri": self.callback,
            "client_id": self.client_id,
            "code_verifier": r["verifier"],
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if self.secret:
            basic = base64.b64encode(
                (
                    quote(self.client_id, safe="") + ":" + quote(self.secret, safe="")
                ).encode()
            ).decode()
            headers["Authorization"] = "Basic " + basic
        response = self.request_json(
            self.metadata()["token_endpoint"], urlencode(form).encode(), headers
        )
        if response.get("token_type", "").lower() != "bearer" or not isinstance(
            response.get("access_token"), str
        ):
            raise Problem("SSO令牌响应无效", 401)
        raw = response.get("id_token")
        identity = self.verify(raw, r["nonce"], response["access_token"])
        # Access and refresh tokens are deliberately discarded; only an encrypted logout hint is retained.
        return self.create_session(identity, raw)

    def create_session(self, x, id_token):
        sub = x["sub"]
        if sub not in self.config["allowed_subjects"]:
            raise Problem("你的Guru账号尚未加入此工作台，请联系管理员", 403)
        role = "admin" if sub in self.config["admin_subjects"] else "member"
        now = time.time()
        token = secrets.token_urlsafe(40)
        hashed = digest(token)
        iv = os.urandom(12)
        encrypted = iv + self.cipher.encrypt(iv, id_token.encode(), hashed.encode())
        with self.app.db() as db:
            db.execute("BEGIN IMMEDIATE")
            r = db.execute(
                "SELECT user_id FROM sso_identities WHERE issuer=? AND subject=?",
                (self.issuer, sub),
            ).fetchone()
            if r:
                uid = r["user_id"]
            elif sub == self.config.get("legacy_admin_subject"):
                old = db.execute(
                    "SELECT id FROM users WHERE username=?",
                    (self.app.credentials["admin_username"],),
                ).fetchone()
                uid = old["id"]
                db.execute(
                    "INSERT INTO sso_identities(issuer,subject,user_id) VALUES(?,?,?)",
                    (self.issuer, sub, uid),
                )
            else:
                uid = str(uuid.uuid4())
                db.execute(
                    "INSERT INTO users VALUES(?,?,?,?,?)",
                    (
                        uid,
                        "sso-"
                        + hashlib.sha256((self.issuer + sub).encode()).hexdigest()[:24],
                        "!sso-only",
                        role,
                        now,
                    ),
                )
                db.execute(
                    "INSERT INTO sso_identities(issuer,subject,user_id) VALUES(?,?,?)",
                    (self.issuer, sub, uid),
                )
            # Display names do not link accounts or grant permission.
            display = str(x.get("name") or x.get("preferred_username") or "Guru 成员")[
                :100
            ]
            db.execute(
                "UPDATE sso_identities SET display_name=? WHERE issuer=? AND subject=?",
                (display, self.issuer, sub),
            )
            db.execute("UPDATE users SET role=? WHERE id=?", (role, uid))
            db.execute(
                "INSERT INTO sessions VALUES(?,?,?)",
                (hashed, uid, now + SESSION_SECONDS),
            )
            db.execute(
                "INSERT INTO sso_sessions VALUES(?,?,?)",
                (hashed, encrypted, x.get("sid")),
            )
            db.execute(
                "INSERT INTO auth_audit(event,user_id,at) VALUES(?,?,?)",
                ("sso_login", uid, now),
            )
            db.execute(
                "DELETE FROM sso_sessions WHERE token NOT IN (SELECT token FROM sessions WHERE expires>?)",
                (now,),
            )
            db.execute("DELETE FROM sessions WHERE expires<=?", (now,))
        return token

    def user(self, token):
        if not token:
            return None
        with self.app.db() as db:
            r = db.execute(
                "SELECT u.id,u.username,i.subject,i.display_name FROM sessions s JOIN sso_sessions ss ON ss.token=s.token JOIN users u ON u.id=s.user_id JOIN sso_identities i ON i.user_id=u.id WHERE s.token=? AND s.expires>? AND i.issuer=?",
                (digest(token), time.time(), self.issuer),
            ).fetchone()
        if not r or r["subject"] not in self.config["allowed_subjects"]:
            return None
        return {
            "id": r["id"],
            "username": r["username"],
            "role": (
                "admin" if r["subject"] in self.config["admin_subjects"] else "member"
            ),
            "auth_provider": "guru_sso",
            "display_name": r["display_name"],
        }

    def logout(self, token):
        hint = None
        hashed = digest(token)
        with self.app.db() as db:
            r = db.execute(
                "SELECT encrypted_id_token FROM sso_sessions WHERE token=?", (hashed,)
            ).fetchone()
            db.execute("DELETE FROM sessions WHERE token=?", (hashed,))
            db.execute("DELETE FROM sso_sessions WHERE token=?", (hashed,))
            db.execute(
                "INSERT INTO auth_audit(event,at) VALUES('sso_logout',?)",
                (time.time(),),
            )
            if r:
                try:
                    b = bytes(r[0])
                    hint = self.cipher.decrypt(b[:12], b[12:], hashed.encode()).decode()
                except Exception:
                    pass
        # Local revocation succeeds even when the provider is offline. Metadata may use an already pinned endpoint.
        state = secrets.token_urlsafe(32)
        with self.app.db() as db:
            db.execute("DELETE FROM sso_logout_flows WHERE expires<?", (time.time(),))
            db.execute(
                "INSERT INTO sso_logout_flows VALUES(?,?)",
                (digest(state), time.time() + 300),
            )
        params = {
            "client_id": self.client_id,
            "post_logout_redirect_uri": self.logged_out,
            "state": state,
        }
        if hint:
            params["id_token_hint"] = hint
        return self.issuer + "/protocol/openid-connect/logout?" + urlencode(params)

    def complete_logout(self, state):
        with self.app.db() as db:
            db.execute("BEGIN IMMEDIATE")
            r = db.execute(
                "SELECT expires FROM sso_logout_flows WHERE state=?", (digest(state),)
            ).fetchone()
            if not r or r["expires"] < time.time():
                raise Problem("登出回调已过期", 400)
            db.execute("DELETE FROM sso_logout_flows WHERE state=?", (digest(state),))
