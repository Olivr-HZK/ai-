"""
我方已投放「特效/主题」库：从飞书多维表同步说明列，在流水线中对竞品素材做命中标记。

- Layer 1：特效名/关键词在 title+body+analysis 中**子串出现**即命中（高置信）。
- Layer 2：未关键词命中时，用 `llm_client.call_embedding` 对说明 canonical 与竞品全文做
  **cosine 相似度**，≥ LAUNCHED_EFFECTS_MATCH_THRESHOLD（默认 0.65）则命中（语义近似）。

见 AGENTS.md「我方已投放特效库匹配」；`apply_launched_effects_filter` 为 Pipeline 入口。

环境：LAUNCHED_EFFECTS_ENABLED、LAUNCHED_EFFECTS_BITABLE_URL、LAUNCHED_EFFECTS_MATCH_THRESHOLD、
LAUNCHED_EFFECTS_KEYWORD=1、LAUNCHED_EFFECTS_SEMANTIC=1、FEISHU_APP_ID/SECRET
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

from path_util import DATA_DIR

load_dotenv()

DEFAULT_LAUNCHED_BITABLE = (
    "https://scnmrtumk0zm.feishu.cn/base/JhMMbPlSUaE6G7siF0RcQn6jnlg?table=tblo36ykG6Pl2X04"
)
FALLBACK_DESCRIPTIONS = DATA_DIR / "launched_effects_descriptions_only.json"
CACHE_FILE = DATA_DIR / "launched_effects_cache.json"
CACHE_TTL_SEC = 24 * 3600


def _enabled() -> bool:
    v = (os.getenv("LAUNCHED_EFFECTS_ENABLED") or "0").strip().lower()
    return v not in ("0", "false", "no", "off", "")


def _keyword_on() -> bool:
    v = (os.getenv("LAUNCHED_EFFECTS_KEYWORD") or "1").strip().lower()
    return v not in ("0", "false", "no", "off", "")


def _semantic_on() -> bool:
    v = (os.getenv("LAUNCHED_EFFECTS_SEMANTIC") or "1").strip().lower()
    return v not in ("0", "false", "no", "off", "")


def _match_threshold() -> float:
    try:
        return float((os.getenv("LAUNCHED_EFFECTS_MATCH_THRESHOLD") or "0.65").strip())
    except ValueError:
        return 0.65


def _bitable_url() -> str:
    return (os.getenv("LAUNCHED_EFFECTS_BITABLE_URL") or DEFAULT_LAUNCHED_BITABLE).strip()


def _parse_bitable_url(url: str) -> tuple[str, str]:
    p = urlparse(url.strip())
    parts = [x for x in p.path.split("/") if x]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    table_id = (parse_qs(p.query or "").get("table") or [""])[0]
    if not app_token or not table_id:
        raise RuntimeError(f"无法从 LAUNCHED_EFFECTS_BITABLE_URL 解析: {url}")
    return app_token, table_id


def get_tenant_access_token() -> str:
    feishu_id = (os.getenv("FEISHU_APP_ID") or "").strip()
    feishu_secret = (os.getenv("FEISHU_APP_SECRET") or "").strip()
    if not feishu_id or not feishu_secret:
        raise RuntimeError("需配置 FEISHU_APP_ID / FEISHU_APP_SECRET 以拉取已投放表")
    r = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": feishu_id, "app_secret": feishu_secret},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token: {data}")
    return str(data.get("tenant_access_token") or "")


def _field_text_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            if isinstance(x, dict) and "text" in x:
                out.append(str(x.get("text") or ""))
            elif isinstance(x, str):
                out.append(x)
        return " ".join(out)
    if isinstance(v, dict):
        t = v.get("type")
        if t in (1, "text", "multiline", "line"):
            return _field_text_value(v.get("value") or v.get("text"))
        return str(v.get("value") or v.get("text") or v.get("link") or "")
    return str(v)


def _record_description(fields: Any) -> str:
    if not isinstance(fields, dict):
        return ""
    for name in ("说明", "说明文本", "描述", "description", "Name"):
        if name in fields:
            t = _field_text_value(fields[name])
            if t.strip():
                return t.strip()
    for _k, val in fields.items():
        t = _field_text_value(val)
        if len(t) > 10:
            return t.strip()[:2000]
    return ""


def _list_bitable_records(app_token: str, table_id: str, token: str) -> List[Dict[str, Any]]:
    base = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}"}
    out: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        params: Dict[str, Any] = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        r = requests.get(base, headers=headers, params=params, timeout=45)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list records: {data}")
        block = data.get("data") or {}
        for it in block.get("items") or []:
            out.append(it)
        if not block.get("has_more"):
            break
        page_token = block.get("page_token")
    return out


def _should_exclude_from_match(desc: str) -> bool:
    d = (desc or "").strip()
    if len(d) < 3:
        return True
    first = d.split("\n", 1)[0].strip()
    if re.match(r"^【(特效优化|新增特效|更新提示|模型更新|添加上线|说明)", first):
        return True
    if re.match(
        r"^【[^】]+】\s*请|下线|再上线|替换封面|置顶|工单",
        first,
    ):
        return True
    return False


def _primary_block(desc: str) -> str:
    return (desc or "").split("\n\n", 1)[0].strip()


def _canonical_text_for_embedding(desc: str) -> str:
    t = re.sub(r"https?://\S+", " ", desc or "", flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:200]


def _build_keywords_for_effect(_desc: str, primary: str) -> List[str]:
    s = (primary or "")[:500]
    kws: Set[str] = set()
    for m in re.finditer(r"[A-Za-z][A-Za-z0-9_\-]{1,32}", s):
        w = m.group(0)
        if len(w) >= 3:
            kws.add(w)
    for m in re.finditer(
        r"([一-鿿]{2,8})\s*（[一-鿵a-zA-Z0-9 \-]{1,20}）|（([一-鿵]{1,8})）", s
    ):
        for g in m.groups():
            if g and len(g) >= 2:
                kws.add(g)
    for part in re.split(r"[，、,;|｜/]", s):
        p = part.strip()
        if 2 <= len(p) <= 40:
            kws.add(p)
    for m in re.finditer(
        r"（([一-鿵a-zA-Z0-9 \-]{1,20})）|\(([A-Za-z][A-Za-z0-9 \-]{1,20})\)", s
    ):
        for g in m.groups():
            if g and len(g) >= 2:
                kws.add(g)
    return [k for k in kws if k and len(k) >= 2]


def _dedupe_effects_by_canonical(
    parsed: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], int]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for p in parsed:
        k = (p.get("canonical_text") or "")[:200]
        if k and k in seen:
            continue
        if k:
            seen.add(k)
        out.append(p)
    return out, len(parsed) - len(out)


def _dedupe_keywords(candidates: List[str]) -> List[str]:
    cands = sorted(
        (c.strip() for c in candidates if c and len(c.strip()) >= 2),
        key=len,
        reverse=True,
    )
    out: List[str] = []
    for c2 in cands:
        if c2 in out:
            continue
        if any(c2 in o and c2 != o for o in out):
            continue
        if any(o in c2 and o != c2 for o in out):
            continue
        out.append(c2)
    return out[:64]


def sync_launched_effects() -> List[Dict[str, Any]]:
    """
    拉取飞书已投放表全部记录，返回 [{ "description", "record_id" }].
    失败时尝试本地 descriptions_only 降级。
    """
    app, tbl = _parse_bitable_url(_bitable_url())
    tok = get_tenant_access_token()
    rows = _list_bitable_records(app, tbl, tok)
    out: List[Dict[str, Any]] = []
    for row in rows:
        rid = str(row.get("record_id") or row.get("id") or "")
        fields = row.get("fields")
        d = _record_description(fields)
        if d:
            out.append({"description": d, "record_id": rid})
    return out


def _load_from_fallback_json() -> List[Dict[str, Any]]:
    if not FALLBACK_DESCRIPTIONS.is_file():
        return []
    try:
        arr = json.loads(FALLBACK_DESCRIPTIONS.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(arr, list):
        return []
    return [{"description": str(x), "record_id": ""} for x in arr if str(x).strip()]


def _read_cache() -> Optional[Dict[str, Any]]:
    if not CACHE_FILE.is_file():
        return None
    try:
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_prepared_effects() -> List[Dict[str, Any]]:
    """单例式加载：带 keywords / canonical；语义向量在应用筛选时现算并缓存到进程内。"""
    if hasattr(_load_prepared_effects, "_cache") and isinstance(
        getattr(_load_prepared_effects, "_cache", None), list
    ):  # type: ignore
        c = getattr(_load_prepared_effects, "_cache", None)
        if c:
            return c  # type: ignore[return-value]

    raw_list: List[Dict[str, Any]] = []
    src = "feishu"
    cached = _read_cache()
    ttl = float((os.getenv("LAUNCHED_EFFECTS_CACHE_TTL_HOURS") or "24").strip() or "24")
    use_cache = (
        isinstance(cached, dict)
        and (time.time() - float(cached.get("fetched_at") or 0)) < ttl * 3600
        and isinstance(cached.get("effects"), list)
        and len(cached.get("effects") or []) > 0
    )
    if use_cache and isinstance(cached, dict):
        for e in cached.get("effects") or []:  # type: ignore[union-attr]
            if isinstance(e, dict) and (e.get("description") or "").strip():
                raw_list.append(
                    {
                        "description": str(e.get("description") or ""),
                        "record_id": str(e.get("record_id") or ""),
                    }
                )
        src = "cache"
    if not raw_list:
        try:
            raw_list = sync_launched_effects()
        except Exception:
            raw_list = []
            src = "fallback"
        if not raw_list:
            raw_list = _load_from_fallback_json()
            src = "fallback"
        if raw_list:
            try:
                CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
                CACHE_FILE.write_text(
                    json.dumps(
                        {
                            "fetched_at": time.time(),
                            "source": src,
                            "effects": raw_list,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
            except Exception:
                pass
    out: List[Dict[str, Any]] = []
    for e in raw_list:
        desc = (e.get("description") or "").strip()
        if not desc:
            continue
        excl = _should_exclude_from_match(desc)
        primary = _primary_block(desc)
        ct = _canonical_text_for_embedding(desc) if not excl else ""
        kws = (
            _dedupe_keywords(_build_keywords_for_effect(desc, primary))
            if not excl
            else []
        )
        out.append(
            {
                "description": desc,
                "record_id": e.get("record_id") or "",
                "excluded_from_match": excl,
                "canonical_text": ct,
                "keywords": kws,
            }
        )
    setattr(_load_prepared_effects, "_cache", out)
    return out


# 进程内 embedding 缓存：canonical -> vec
_emb_vec_cache: Dict[str, List[float]] = {}


def _embed_cached(text: str) -> List[float]:
    t = (text or "").strip()
    if not t:
        return []
    if t in _emb_vec_cache:
        return _emb_vec_cache[t]
    from llm_client import call_embedding

    v = call_embedding(t[:2000])
    _emb_vec_cache[t] = v
    return v


def match_against_launched_effects(
    analysis: str, title: str, body: str, effects: Optional[List[Dict[str, Any]]] = None
) -> Optional[Dict[str, Any]]:
    effs = effects or _load_prepared_effects()
    hay = f"{title}\n{body}\n{analysis}"[:4000]
    h_lower = hay.lower()
    th = _match_threshold()

    if _keyword_on():
        for e in effs:
            if e.get("excluded_from_match"):
                continue
            for kw in e.get("keywords") or []:
                if len(kw) < 2:
                    continue
                if kw.isascii():
                    hit = kw.lower() in h_lower
                else:
                    hit = kw in hay
                if hit:
                    return {
                        "layer": "keyword",
                        "keyword": kw,
                        "summary": (e.get("description") or "")[:200],
                    }

    if not _semantic_on():
        return None

    best_sim = -1.0
    best: Optional[Dict[str, Any]] = None
    try:
        ctx_vec = _embed_cached(hay)
    except Exception:
        return None
    from llm_client import cosine_similarity

    for e in effs:
        if e.get("excluded_from_match"):
            continue
        ct = (e.get("canonical_text") or "").strip()
        if not ct or len(ct) < 4:
            continue
        if ct in _emb_vec_cache:
            ev = _emb_vec_cache[ct]
        else:
            try:
                ev = _embed_cached(ct)
            except Exception:
                continue
        sim = cosine_similarity(ctx_vec, ev)
        if sim > best_sim and sim >= th:
            best_sim = sim
            best = e
    if best is None:
        return None
    return {
        "layer": "semantic",
        "similarity": best_sim,
        "summary": (best.get("description") or "")[:200],
    }


def _effect_display_name(detail: Optional[Dict[str, Any]], fallback: str) -> str:
    if not detail:
        return (fallback or "")[:80]
    s = (detail.get("summary") or "").split("\n", 1)[0].strip()
    if len(s) > 60:
        s = s[:57] + "…"
    return s or "已投放主题"


def apply_launched_effects_filter(
    combined_results: List[Dict[str, Any]]
) -> Tuple[int, List[Dict[str, Any]]]:
    if not _enabled():
        return 0, []
    try:
        effs = _load_prepared_effects()
    except Exception as e:
        return 0, [{"error": str(e)}]
    if not effs:
        return 0, []

    details: List[Dict[str, Any]] = []
    n = 0
    for row in combined_results:
        if not isinstance(row, dict):
            continue
        ak = str(row.get("ad_key") or "")
        a = str(row.get("analysis") or "")
        if not a or a.strip().startswith("[ERROR]"):
            continue

        mtags = row.get("material_tags")
        mlist = [str(x) for x in mtags] if isinstance(mtags, list) else []
        ex_b = bool(row.get("exclude_from_bitable"))
        lem = row.get("launched_effect_match")
        has_our = any("我方已投" in x for x in mlist)
        if (has_our or lem) and not ex_b:
            row["exclude_from_bitable"] = True
            row["exclude_from_cluster"] = True
            n += 1
            details.append(
                {
                    "ad_key": ak,
                    "backfill": True,
                }
            )
            continue
        if ex_b:
            continue

        m = match_against_launched_effects(
            a, str(row.get("title") or ""), str(row.get("body") or ""), effs
        )
        if not m:
            continue
        n += 1
        row["exclude_from_bitable"] = True
        row["exclude_from_cluster"] = True
        row["launched_effect_match"] = m
        name = _effect_display_name(m, "")
        tag = f"我方已投放: {name}"
        if tag not in mlist:
            mlist.append(tag)
        row["material_tags"] = mlist
        details.append(
            {
                "ad_key": ak,
                "layer": m.get("layer"),
                "match": m,
            }
        )
    return n, details
