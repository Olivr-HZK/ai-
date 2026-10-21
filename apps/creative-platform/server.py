#!/usr/bin/env python3
from __future__ import annotations
import argparse, http.cookies, json, mimetypes, os, re, secrets, sys, time, zipfile, io
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs, unquote
from core import Portal, Problem, digest
from sso import SSO
from task_intake import TaskIntake

HERE = Path(__file__).resolve().parent


class Handler(BaseHTTPRequestHandler):
    server_version = "GuruPortal/1.0"

    def log_message(self, format, *args):
        # Do not log query strings, cookies, request bodies or credentials.
        sys.stderr.write("%s %s\n" % (self.command, urlsplit(self.path).path))

    @property
    def app(self):
        return self.server.app

    def headers_common(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "same-origin")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cache-Control", "no-store")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' data:; media-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
        )

    def json(self, obj, status=200, cookie=None):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.headers_common()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(b)

    def body(self):
        if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
            raise Problem("仅支持JSON请求", 415)
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise Problem("请求长度无效")
        if not 0 < n <= 524288:
            raise Problem("请求体过大或为空", 413)
        try:
            x = json.loads(self.rfile.read(n))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise Problem("JSON格式无效")
        if not isinstance(x, dict):
            raise Problem("请求格式无效")
        return x

    def named_cookie(self, name):
        try:
            c = http.cookies.SimpleCookie(self.headers.get("Cookie", ""))
            return c[name].value if name in c else ""
        except http.cookies.CookieError:
            return ""

    def token(self):
        return self.named_cookie("guru_session")

    def cookie(self, name, value, age, path="/"):
        secure = "; Secure" if not self.server.sso.local else ""
        return (
            f"{name}={value}; Path={path}; HttpOnly; SameSite=Lax; Max-Age={age}"
            + secure
        )

    def redirect(self, location, cookies=()):
        self.send_response(303)
        self.headers_common()
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Location", location)
        for cookie in cookies:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def require(self, admin=False):
        user = self.server.sso.user(self.token())
        if not user:
            raise Problem("请先登录", 401)
        if admin and user["role"] != "admin":
            raise Problem("仅管理员可操作", 403)
        return user

    def worker(self):
        raw = self.headers.get("Authorization", "")
        if not raw.startswith("Bearer ") or not secrets.compare_digest(
            raw[7:], self.app.worker_token
        ):
            raise Problem("采集端身份无效", 401)

    def mutate_origin(self):
        origin = self.headers.get("Origin")
        if origin and origin != self.server.sso.base:
            raise Problem("跨站请求被拒绝", 403)
        if self.headers.get("Sec-Fetch-Site") == "cross-site":
            raise Problem("跨站请求被拒绝", 403)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        self.dispatch("GET")

    def do_POST(self):
        self.dispatch("POST")

    def dispatch(self, method):
        try:
            self.route(method)
        except Problem as e:
            self.json({"error": e.message}, e.status)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            import traceback

            traceback.print_exc()
            self.json({"error": "服务暂时不可用，请查看本机服务日志"}, 500)

    def route(self, method):
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)
        q = parse_qs(parsed.query)
        if method == "GET" and path in ["/", "/app.js", "/style.css"]:
            return self.file(
                HERE / "public" / ({"/": "index.html"}.get(path, path.lstrip("/")))
            )
        if path == "/api/health" and method == "GET":
            return self.json(
                {
                    "ok": True,
                    "version": "1.1.0",
                    "auth_provider": "guru_sso",
                    "auto_crawl_available": False,
                }
            )
        if (
            not path.startswith("/api/worker/")
            and self.headers.get("Host") != urlsplit(self.server.sso.base).netloc
        ):
            raise Problem("访问地址与SSO配置不匹配", 400)
        if path == "/api/auth" and method == "GET":
            return self.json(
                {"provider": "guru_sso", "login_url": "/auth/login", "session_hours": 8}
            )
        if path == "/auth/login" and method == "GET":
            location, binding = self.server.sso.begin()
            return self.redirect(
                location, [self.cookie("guru_oidc", binding, 300, path="/auth")]
            )
        if path == "/auth/callback" and method == "GET":
            try:
                token = self.server.sso.callback_login(
                    parse_qs(parsed.query, keep_blank_values=True),
                    self.named_cookie("guru_oidc"),
                )
            except Problem as e:
                error = {403: "forbidden", 503: "unavailable"}.get(e.status, "invalid")
                return self.redirect(
                    "/?auth_error=" + error,
                    [self.cookie("guru_oidc", "", 0, path="/auth")],
                )
            # Rotate any prior session; never reuse an anonymous or password session cookie.
            old = self.token()
            if old:
                with self.app.db() as db:
                    db.execute("DELETE FROM sessions WHERE token=?", (digest(old),))
                    db.execute("DELETE FROM sso_sessions WHERE token=?", (digest(old),))
            return self.redirect(
                "/",
                [
                    self.cookie("guru_session", token, 28800),
                    self.cookie("guru_oidc", "", 0, path="/auth"),
                ],
            )
        if path == "/auth/logged-out" and method == "GET":
            if len(q.get("state", [])) != 1:
                raise Problem("登出回调无效")
            self.server.sso.complete_logout(q["state"][0])
            return self.redirect("/?signed_out=1", [self.cookie("guru_session", "", 0)])
        if method == "POST":
            self.mutate_origin()
            if path in ["/api/login", "/api/register", "/api/invites"]:
                raise Problem("本地账号入口已关闭，请使用Guru SSO", 410)
            if path == "/api/logout":
                self.require()
                self.body()
                location = self.server.sso.logout(self.token())
                return self.json(
                    {"ok": True, "logout_url": location},
                    cookie=self.cookie("guru_session", "", 0),
                )
        if path.startswith("/api/worker/"):
            self.worker()
            if method != "POST":
                raise Problem("方法不支持", 405)
            x = self.body()
            if path == "/api/worker/heartbeat":
                self.app.worker_heartbeat(x.get("name", "浏览器采集端"))
                return self.json({"ok": True})
            if path == "/api/worker/claim":
                return self.json({"job": self.app.claim(x.get("job_id"))})
            if path == "/api/worker/progress":
                self.app.advance(
                    x["job_id"],
                    x["status"],
                    x.get("counts"),
                    x.get("checkpoint"),
                    x.get("message", ""),
                )
                return self.json({"ok": True})
            if path == "/api/worker/finish":
                jid = x["job_id"]
                if not re.fullmatch(r"[a-f0-9-]{36}", jid):
                    raise Problem("任务ID无效")
                return self.json(
                    {
                        "job_id": self.app.import_delivery(
                            self.app.root / "runs" / jid, jid
                        )
                    }
                )
            raise Problem("接口不存在", 404)
        user = self.require()
        if method == "GET":
            if path == "/api/me":
                return self.json({"user": user})
            if path == "/api/overview":
                return self.json(self.app.snapshot())
            if path == "/api/registry":
                return self.json({"items": self.app.registry})
            if path == "/api/jobs":
                return self.json({"items": self.app.jobs()})
            if path == "/api/materials":
                return self.json(
                    {
                        "items": self.app.material_list(
                            q.get("decision", ["keep"])[0],
                            q.get("q", [""])[0],
                            q.get("job", [None])[0],
                        )
                    }
                )
            if re.fullmatch(r"/api/jobs/[a-f0-9-]{36}", path):
                return self.json(self.app.job(path.split("/")[-1]))
            if re.fullmatch(r"/api/jobs/[a-f0-9-]{36}/handoff", path):
                self.require(True)
                return self.json({"prompt": self.app.handoff(path.split("/")[-2])})
            if path.startswith("/media/"):
                m = re.fullmatch(r"/media/([a-f0-9-]{36})/(video.mp4|poster.png)", path)
                if not m:
                    raise Problem("文件不存在", 404)
                with self.app.db() as db:
                    r = db.execute(
                        "SELECT decision FROM materials WHERE id=?", (m[1],)
                    ).fetchone()
                if not r or r["decision"] == "exclude":
                    raise Problem("素材不存在", 404)
                return self.file(self.app.root / "materials" / m[1] / m[2])
            if path == "/api/export":
                return self.export(q.get("job", [None])[0])
        if method == "POST":
            if path == "/api/task-intake":
                return self.json(
                    self.server.intake.parse(self.body().get("text"), user)
                )
            if path == "/api/jobs":
                data = self.body()
                provenance = (
                    self.server.intake.provenance(data["ai_draft_id"], user)
                    if data.get("ai_draft_id")
                    else None
                )
                return self.json(
                    {"id": self.app.create_job(data, user, provenance)}, 201
                )
            reviewed = re.fullmatch(r"/api/materials/([a-f0-9-]{36})/review", path)
            if reviewed:
                x = self.body()
                self.app.review_material(
                    reviewed[1],
                    user,
                    x.get("decision"),
                    x.get("note"),
                    x.get("confirmed_viewed"),
                )
                return self.json({"ok": True})
            m = re.fullmatch(r"/api/jobs/([a-f0-9-]{36})/(claim|cancel|retry)", path)
            if m:
                self.body()
                job = self.app.job(m[1])
                if m[2] == "claim":
                    self.require(True)
                    return self.json(
                        {"job": self.app.claim(m[1]), "prompt": self.app.handoff(m[1])}
                    )
                if job["owner"] != user["id"] and user["role"] != "admin":
                    raise Problem("无权修改此任务", 403)
                status = "cancelled" if m[2] == "cancel" else "queued"
                self.app.advance(
                    m[1],
                    status,
                    message="用户取消任务" if status == "cancelled" else "用户重新排队",
                )
                return self.json({"ok": True})
        raise Problem("接口不存在", 404)

    def file(self, p):
        if not p.is_file():
            raise Problem("文件不存在", 404)
        size = p.stat().st_size
        start = 0
        end = size - 1
        code = 200
        requested = self.headers.get("Range")
        if requested:
            m = re.fullmatch(r"bytes=(\d*)-(\d*)", requested)
            if not m or (not m[1] and not m[2]):
                raise Problem("Range无效", 416)
            if m[1]:
                start = int(m[1])
                end = min(int(m[2]), size - 1) if m[2] else size - 1
            else:
                start = max(size - int(m[2]), 0)
            if start >= size or end < start:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.end_headers()
                return
            code = 206
        self.send_response(code)
        self.headers_common()
        self.send_header(
            "Content-Type",
            mimetypes.guess_type(p.name)[0] or "application/octet-stream",
        )
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(end - start + 1))
        if code == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if self.command == "HEAD":
            return
        with p.open("rb") as f:
            f.seek(start)
            remaining = end - start + 1
            while remaining:
                data = f.read(min(1024 * 1024, remaining))
                if not data:
                    break
                self.wfile.write(data)
                remaining -= len(data)

    def export(self, jid):
        # Stream ZIP to disk once; no large videos held in request memory.
        import hashlib

        items = self.app.material_list("keep", job=jid)
        if not items:
            raise Problem("没有可导出的已确认素材", 404)
        signature = hashlib.sha256(
            json.dumps([(i["id"], i["video_sha256"]) for i in items]).encode()
        ).hexdigest()[:20]
        folder = self.app.root / "exports"
        folder.mkdir(exist_ok=True)
        dest = folder / (signature + ".zip")
        if not dest.exists():
            temp = folder / (signature + "-" + secrets.token_hex(4) + ".tmp")
            with zipfile.ZipFile(temp, "w", compression=zipfile.ZIP_STORED) as z:
                for m in items:
                    z.write(
                        self.app.root / "materials" / m["id"] / "video.mp4",
                        "videos/" + m["ad_id"] + ".mp4",
                    )
                z.writestr(
                    "manifest.json", json.dumps(items, ensure_ascii=False, indent=2)
                )
            os.replace(temp, dest)
        return self.file(dest)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8877)
    p.add_argument("--data", default=None)
    p.add_argument("--import-run")
    p.add_argument("--sso-config", default=None)
    args = p.parse_args()
    project = HERE.parents[1]
    app = Portal(args.data or project / "data/creative_platform", project)
    if args.import_run:
        print(
            json.dumps(
                {"job_id": app.import_delivery(args.import_run)}, ensure_ascii=False
            )
        )
        return
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.app = app
    server.intake = TaskIntake(app)
    server.sso = SSO(app, args.sso_config or app.root / "sso.json")
    if server.sso.local and args.host != "127.0.0.1":
        raise SystemExit("本机SSO模式仅允许绑定127.0.0.1")
    print(
        f"Guru portal listening at http://{args.host}:{args.port} (Guru SSO enabled; password login disabled)",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
