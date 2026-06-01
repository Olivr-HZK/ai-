"""
根据 data/playable_session_debug.json 生成可打开的 Debug 页面，
用于查看会话里每条请求/响应做了什么。
"""

import json
import re
import sys
from pathlib import Path

from path_util import DATA_DIR

SESSION_FILE = DATA_DIR / "playable_session_debug.json"
OUT_HTML = DATA_DIR / "playable_debug_page.html"


def endpoint_name(url: str) -> str:
    if not url or "guangdada.net/napi" not in url:
        return url or ""
    path = url.split("?")[0]
    return path.replace("https://guangdada.net/napi/v1/", "").strip("/") or path


def response_summary(entry: dict) -> str:
    body = entry.get("body")
    if not isinstance(body, dict):
        return ""
    url = entry.get("url", "")
    data = body.get("data")
    if "creative/list" in url and isinstance(data, dict):
        lst = data.get("creative_list")
        n = len(lst) if isinstance(lst, list) else 0
        return f"creative_list: {n} 条"
    if "creative/detail-v2" in url and isinstance(data, dict):
        ad_key = (data.get("ad_key") or "")[:12]
        app = data.get("app") or {}
        name = data.get("game_name") or app.get("app_name") or app.get("name") or "-"
        return f"ad_key={ad_key}... | {name}"
    if "creative/list-condition" in url:
        return "筛选条件配置"
    if "user/login" in url:
        return "登录结果"
    if "user/jwt" in url or "user/nbs-info" in url:
        return "用户/权限"
    return ""


def request_summary(entry: dict) -> str:
    url = entry.get("url", "")
    method = entry.get("method", "")
    post = entry.get("post_data")
    if post and len(post) > 200:
        post = post[:200] + "..."
    if "creative/list" in url and post:
        try:
            p = json.loads(post)
            return f"POST 参数: {list(p.keys())}"
        except Exception:
            pass
    return f"{method} {endpoint_name(url)}" if url else ""


def redact_sensitive(obj):
    """脱敏：不把密码等写进页面"""
    if isinstance(obj, dict):
        return {k: ("***" if k in ("password", "token", "post_data") and k == "password" else redact_sensitive(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact_sensitive(x) for x in obj]
    return obj


def main():
    if not SESSION_FILE.exists():
        print(f"未找到 {SESSION_FILE}，请先运行 DEBUG=1 的记录模式。", file=sys.stderr)
        sys.exit(1)

    with open(SESSION_FILE, "r", encoding="utf-8") as f:
        session = json.load(f)

    # 脱敏：请求里的 post_data 若含 password 则打码
    for e in session:
        if e.get("type") == "request" and e.get("post_data"):
            try:
                p = json.loads(e["post_data"])
                if "password" in p:
                    p["password"] = "***"
                    e["post_data"] = json.dumps(p, ensure_ascii=False)
            except Exception:
                pass

    # 统计
    requests = [e for e in session if e.get("type") == "request"]
    responses = [e for e in session if e.get("type") == "response"]
    by_endpoint = {}
    for e in session:
        url = e.get("url", "")
        if not url:
            continue
        name = endpoint_name(url)
        by_endpoint[name] = by_endpoint.get(name, 0) + 1

    # 生成 HTML（内嵌 session JSON，用占位符避免 f-string 与 JS 语法冲突）
    session_js = json.dumps(session, ensure_ascii=False)
    summary_js = json.dumps({
        "total_entries": len(session),
        "requests": len(requests),
        "responses": len(responses),
        "by_endpoint": by_endpoint,
    }, ensure_ascii=False)

    html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>试玩广告会话 Debug</title>
  <style>
    :root { font-family: system-ui, sans-serif; font-size: 14px; }
    body { max-width: 1000px; margin: 0 auto; padding: 16px; background: #1a1a1a; color: #e0e0e0; }
    h1 { font-size: 1.25rem; margin-bottom: 8px; }
    .summary { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 8px; margin-bottom: 16px; }
    .summary .card { background: #2a2a2a; padding: 12px; border-radius: 8px; }
    .summary .card strong { display: block; font-size: 1.5rem; color: #7dd3fc; }
    .filters { margin-bottom: 12px; }
    .filters select { background: #2a2a2a; color: #e0e0e0; border: 1px solid #444; padding: 6px 10px; border-radius: 6px; }
    .entry { margin-bottom: 8px; border: 1px solid #333; border-radius: 8px; overflow: hidden; }
    .entry.req { border-left: 4px solid #f59e0b; }
    .entry.res { border-left: 4px solid #22c55e; }
    .entry-head { padding: 10px 12px; cursor: pointer; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .entry-head:hover { background: #252525; }
    .entry-body { padding: 0 12px 12px; }
    .entry-body pre { margin: 0; padding: 10px; background: #111; border-radius: 6px; overflow: auto; max-height: 320px; font-size: 12px; white-space: pre-wrap; word-break: break-all; }
    .badge { font-size: 11px; padding: 2px 6px; border-radius: 4px; }
    .badge.req { background: #f59e0b33; color: #fbbf24; }
    .badge.res { background: #22c55e33; color: #4ade80; }
    .ts { color: #888; font-size: 12px; }
    .endpoint { color: #7dd3fc; }
    .summary-txt { color: #a5b4fc; font-size: 12px; }
  </style>
</head>
<body>
  <h1>试玩广告会话 Debug</h1>
  <p style="color:#888; margin-bottom: 16px;">通过 DEBUG=1 记录下的 napi 请求/响应，便于查看「这些会话操作到底干了什么」。</p>

  <div class="summary">
    <div class="card"><strong id="total">0</strong> 总条数</div>
    <div class="card"><strong id="reqCount">0</strong> 请求</div>
    <div class="card"><strong id="resCount">0</strong> 响应</div>
  </div>

  <div class="filters">
    <label>按接口筛选：</label>
    <select id="filterEndpoint">
      <option value="">全部</option>
    </select>
  </div>

  <div id="timeline"></div>

  <script>
    const session = __SESSION_JS__;
    const summary = __SUMMARY_JS__;

    document.getElementById("total").textContent = summary.total_entries;
    document.getElementById("reqCount").textContent = summary.requests;
    document.getElementById("resCount").textContent = summary.responses;

    const endpoints = Object.keys(summary.by_endpoint || {}).sort();
    const sel = document.getElementById("filterEndpoint");
    endpoints.forEach(ep => {
      const opt = document.createElement("option");
      opt.value = ep;
      opt.textContent = ep + " (" + summary.by_endpoint[ep] + ")";
      sel.appendChild(opt);
    });

    function endpointName(url) {
      if (!url || url.indexOf("guangdada.net/napi") === -1) return url || "";
      const path = url.split("?")[0];
      return path.replace("https://guangdada.net/napi/v1/", "").replace(/\\/$/, "") || path;
    }

    function responseSummary(entry) {
      const body = entry.body;
      if (!body || typeof body !== "object") return "";
      const url = entry.url || "";
      const data = body.data;
      if (url.indexOf("creative/list") !== -1 && data && typeof data === "object") {
        const lst = data.creative_list;
        const n = Array.isArray(lst) ? lst.length : 0;
        return "creative_list: " + n + " 条";
      }
      if (url.indexOf("creative/detail-v2") !== -1 && data && typeof data === "object") {
        const adKey = (data.ad_key || "").slice(0, 12);
        const app = data.app || {};
        const name = data.game_name || app.app_name || app.name || "-";
        return "ad_key=" + adKey + "... | " + name;
      }
      if (url.indexOf("creative/list-condition") !== -1) return "筛选条件配置";
      if (url.indexOf("user/login") !== -1) return "登录结果";
      if (url.indexOf("user/jwt") !== -1 || url.indexOf("user/nbs-info") !== -1) return "用户/权限";
      return "";
    }

    function requestSummary(entry) {
      const url = entry.url || "";
      const method = entry.method || "";
      let post = entry.post_data;
      if (post && post.length > 200) post = post.slice(0, 200) + "...";
      if (url.indexOf("creative/list") !== -1 && post) {
        try {
          const p = JSON.parse(post);
          return "POST 参数: " + Object.keys(p).join(", ");
        } catch (e) {}
      }
      return method + " " + endpointName(url);
    }

    function renderEntry(entry, index) {
      const isReq = entry.type === "request";
      const ep = endpointName(entry.url || "");
      const head = isReq ? requestSummary(entry) : (responseSummary(entry) || ep || entry.url);
      const body = isReq
        ? (entry.post_data ? "<pre>" + escapeHtml(entry.post_data) + "</pre>" : "<pre>(无 body)</pre>")
        : "<pre>" + escapeHtml(JSON.stringify(entry.body, null, 2)) + "</pre>";
      return `
        <div class="entry ${isReq ? "req" : "res"}" data-index="${index}" data-endpoint="${escapeAttr(ep)}">
          <div class="entry-head">
            <span class="badge ${isReq ? "req" : "res"}">${isReq ? "请求" : "响应"}</span>
            <span class="ts">${entry.ts || ""}</span>
            <span class="endpoint">${escapeHtml(ep)}</span>
            <span class="summary-txt">${escapeHtml(head)}</span>
          </div>
          <div class="entry-body" style="display:none;">${body}</div>
        </div>`;
    }

    function escapeHtml(s) {
      if (s == null) return "";
      const div = document.createElement("div");
      div.textContent = s;
      return div.innerHTML;
    }
    function escapeAttr(s) {
      if (s == null) return "";
      return String(s).replace(/"/g, "&quot;");
    }

    function render() {
      const filter = (document.getElementById("filterEndpoint").value || "").trim();
      const list = filter
        ? session.filter(e => endpointName(e.url || "") === filter)
        : session;
      const html = list.map((e, i) => renderEntry(e, i)).join("");
      document.getElementById("timeline").innerHTML = html;
      document.querySelectorAll(".entry-head").forEach(el => {
        el.addEventListener("click", () => {
          const body = el.nextElementSibling;
          body.style.display = body.style.display === "none" ? "block" : "none";
        });
      });
    }

    document.getElementById("filterEndpoint").addEventListener("change", render);
    render();
  </script>
</body>
</html>
"""
    # 避免 JSON 里的 </script> 破坏 HTML
    session_js_safe = session_js.replace("</", "<\\/")
    summary_js_safe = summary_js.replace("</", "<\\/")
    html = html.replace("__SESSION_JS__", session_js_safe).replace("__SUMMARY_JS__", summary_js_safe)

    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"已生成 Debug 页面 → {OUT_HTML}")
    print(f"  总条数: {len(session)}，请求: {len(requests)}，响应: {len(responses)}")
    print(f"  在浏览器中打开该 HTML 文件即可查看会话操作。")


if __name__ == "__main__":
    main()
