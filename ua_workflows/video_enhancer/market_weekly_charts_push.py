"""Push pure ranking facts for Guangdada weekly creative charts."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

import requests

from ua_workflows.shared.config import REPORTS_DIR, load_project_env


CHART_LABELS = {
    "new": "新创意榜",
    "hot": "热门榜",
    "surge": "飙升榜",
}

CHART_SECTION_LABELS = {
    "new": "新创意榜",
    "hot": "热门榜",
    "surge": "飙升榜",
}

CHART_ORDER = {"new": 0, "hot": 1, "surge": 2}


def _text(value: Any, default: str = "") -> str:
    text = str(value or "").replace("\n", " ").strip()
    return text or default


def _int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return int(float(value))
    except Exception:
        return default


def _short(value: Any, limit: int = 58) -> str:
    text = _text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


def _md(value: Any) -> str:
    return _text(value).replace("|", " ").replace("[", "［").replace("]", "］")


def _creative(item: dict[str, Any]) -> dict[str, Any]:
    creative = item.get("creative") if isinstance(item, dict) else {}
    return creative if isinstance(creative, dict) else {}


def _chart_type(creative: dict[str, Any]) -> str:
    raw = _text(creative.get("market_chart_type") or "new").lower()
    if raw in {"hot-charts", "hot_chart", "weekly_hot"}:
        return "hot"
    if raw in {"surge-charts", "rising", "rise", "surge_chart"}:
        return "surge"
    return raw if raw in CHART_LABELS else "new"


def _chart_name(chart_type: str, creative: dict[str, Any]) -> str:
    return _text(creative.get("market_chart_name") or CHART_LABELS.get(chart_type) or chart_type)


def _rank(creative: dict[str, Any]) -> int:
    return _int(creative.get("market_chart_rank") or creative.get("new_charts_rank"), 999999)


def _dedupe_key(creative: dict[str, Any]) -> str:
    for key in ("ad_key", "creative_id", "creativeId", "id", "video_url", "preview_img_url"):
        value = _text(creative.get(key))
        if value:
            return f"{key}:{value}"
    return json.dumps(creative, ensure_ascii=False, sort_keys=True)


def _top_label(creative: dict[str, Any]) -> str:
    for key in ("top_label", "top_tag", "top_percentile", "popularity_label"):
        value = _text(creative.get(key))
        if value:
            return value
    tags = creative.get("tags") or creative.get("labels")
    if isinstance(tags, list):
        for tag in tags:
            text = _text(tag)
            if "Top1" in text or "Top10" in text:
                return text
    return ""


def _material_url(row: dict[str, Any]) -> str:
    return _text(row.get("video_url") or row.get("preview_img_url") or row.get("cover_url"))


def _format_number(value: Any) -> str:
    number = _int(value, 0)
    if number >= 10000:
        return f"{number / 10000:.1f}万"
    return str(number) if number else "-"


def _display_estimate(row: dict[str, Any]) -> Any:
    return row.get("all_exposure_value")


def _popularity(row: dict[str, Any]) -> Any:
    return row.get("impression")


def _heat(row: dict[str, Any]) -> Any:
    return row.get("heat")


def _make_hit(creative: dict[str, Any]) -> dict[str, Any]:
    chart_type = _chart_type(creative)
    return {
        "chart_type": chart_type,
        "chart_name": _chart_name(chart_type, creative),
        "rank": _rank(creative),
        "top_label": _top_label(creative),
    }


def merge_chart_items(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in items:
        creative = _creative(item)
        if not creative:
            continue
        key = _dedupe_key(creative)
        chart_hit = _make_hit(creative)
        row = merged.get(key)
        if row is None:
            row = {
                "dedupe_key": key,
                "ad_key": _text(creative.get("ad_key") or creative.get("creative_id") or creative.get("id")),
                "title": _text(creative.get("title") or creative.get("body") or creative.get("advertiser_name") or key),
                "advertiser_name": _text(creative.get("advertiser_name") or creative.get("page_name")),
                "product": _text(item.get("product") or creative.get("app_name") or creative.get("product")),
                "appid": _text(item.get("appid") or creative.get("appid") or creative.get("app_id")),
                "platform": _text(creative.get("platform")),
                "creative_type": _text(creative.get("creative_type") or "video"),
                "video_url": _text(creative.get("video_url")),
                "preview_img_url": _text(creative.get("preview_img_url") or creative.get("cover_url")),
                "first_seen": creative.get("first_seen"),
                "last_seen": creative.get("last_seen"),
                "days_count": creative.get("days_count"),
                "heat": creative.get("heat"),
                "impression": creative.get("impression"),
                "all_exposure_value": creative.get("all_exposure_value"),
                "growth_delta": creative.get("weekly_heat_delta") or creative.get("growth_delta"),
                "growth_rate": creative.get("weekly_growth_rate") or creative.get("growth_rate"),
                "top_label": _top_label(creative),
                "chart_hits": [],
            }
            merged[key] = row
        if not row.get("video_url") and creative.get("video_url"):
            row["video_url"] = _text(creative.get("video_url"))
        if not row.get("preview_img_url") and (creative.get("preview_img_url") or creative.get("cover_url")):
            row["preview_img_url"] = _text(creative.get("preview_img_url") or creative.get("cover_url"))
        if chart_hit["top_label"] and not row.get("top_label"):
            row["top_label"] = chart_hit["top_label"]
        existing = {(hit["chart_type"], hit["rank"]) for hit in row["chart_hits"]}
        if (chart_hit["chart_type"], chart_hit["rank"]) not in existing:
            row["chart_hits"].append(chart_hit)

    rows = list(merged.values())
    for row in rows:
        row["chart_hits"].sort(key=lambda hit: (CHART_ORDER.get(hit["chart_type"], 99), hit["rank"]))
    return rows


def _rank_bonus(rank: int) -> int:
    if rank <= 0 or rank >= 999999:
        return 0
    return max(0, 31 - rank)


def _focus_score(row: dict[str, Any]) -> int:
    hits = row.get("chart_hits") or []
    score = max(0, len(hits) - 1) * 100
    top_label = _text(row.get("top_label"))
    if "Top1" in top_label:
        score += 80
    elif "Top10" in top_label:
        score += 40
    for hit in hits:
        rank = _int(hit.get("rank"), 999999)
        if hit.get("chart_type") == "surge" and rank <= 10:
            score += 30
        elif hit.get("chart_type") in {"new", "hot"} and rank <= 10:
            score += 20
        score += _rank_bonus(rank)
    if _text(row.get("video_url")):
        score += 10
    return score


def rank_focus_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [row for row in rows if isinstance(row, dict)],
        key=lambda row: (
            -_focus_score(row),
            min((_int(hit.get("rank"), 999999) for hit in row.get("chart_hits") or []), default=999999),
            _text(row.get("ad_key") or row.get("dedupe_key")),
        ),
    )


def _hit_text(row: dict[str, Any]) -> str:
    parts = []
    for hit in row.get("chart_hits") or []:
        text = f"{hit.get('chart_name')} #{hit.get('rank')}"
        if hit.get("top_label") and hit.get("top_label") not in text:
            text += f" / {hit.get('top_label')}"
        parts.append(text)
    return " / ".join(parts) or "-"


def _line(index: int, row: dict[str, Any]) -> list[str]:
    title = _md(_short(row.get("title") or row.get("ad_key") or "未命名素材", 54))
    url = _material_url(row)
    head = f"{index}. [{title}]({url})" if url else f"{index}. **{title}**"
    meta = [
        f"广告主：{_md(row.get('advertiser_name') or '-')}",
        f"素材：{_md(row.get('creative_type') or '-')}",
    ]
    metrics = [
        f"展示估值：{_format_number(_display_estimate(row))}",
        f"人气：{_format_number(_popularity(row))}",
        f"热度：{_format_number(_heat(row))}",
    ]
    top = _text(row.get("top_label"))
    lines = [head, f"   排名：{_md(_hit_text(row))}", "   " + "｜".join(meta), "   " + "｜".join(metrics)]
    if top:
        lines.append(f"   Top标签：{_md(top)}")
    return lines


def _section_rows(rows: list[dict[str, Any]], chart_type: str, limit: int) -> list[dict[str, Any]]:
    selected = [
        row
        for row in rows
        if any(hit.get("chart_type") == chart_type for hit in row.get("chart_hits") or [])
    ]
    return sorted(
        selected,
        key=lambda row: min(
            (_int(hit.get("rank"), 999999) for hit in row.get("chart_hits") or [] if hit.get("chart_type") == chart_type),
            default=999999,
        ),
    )[:limit]


def _chart_rank(row: dict[str, Any], chart_type: str) -> int:
    return min(
        (_int(hit.get("rank"), 999999) for hit in row.get("chart_hits") or [] if hit.get("chart_type") == chart_type),
        default=999999,
    )


def _table_material(row: dict[str, Any]) -> str:
    title = _md(_short(row.get("title") or row.get("ad_key") or "未命名素材", 36))
    url = _material_url(row)
    return f"[{title}]({url})" if url else title


def _chart_table_rows(rows: list[dict[str, Any]], chart_type: str, limit: int) -> list[dict[str, Any]]:
    table_rows: list[dict[str, Any]] = []
    for row in _section_rows(rows, chart_type, limit):
        rank = _chart_rank(row, chart_type)
        table_rows.append(
            {
                "rank": f"#{rank}" if rank < 999999 else "-",
                "material": _table_material(row),
                "advertiser": _md(_short(row.get("advertiser_name") or "-", 18)),
                "display_estimate": _format_number(_display_estimate(row)),
                "popularity": _format_number(_popularity(row)),
                "heat": _format_number(_heat(row)),
                "chart_ranks": _md(_short(_hit_text(row), 40)),
            }
        )
    return table_rows


def _focus_table_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    table_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rank_focus_rows(rows)[:limit], start=1):
        table_rows.append(
            {
                "rank": f"#{index}",
                "material": _table_material(row),
                "advertiser": _md(_short(row.get("advertiser_name") or "-", 18)),
                "display_estimate": _format_number(_display_estimate(row)),
                "popularity": _format_number(_popularity(row)),
                "heat": _format_number(_heat(row)),
                "chart_ranks": _md(_short(_hit_text(row), 40)),
            }
        )
    return table_rows


def _rows_markdown_table(table_rows: list[dict[str, Any]]) -> str:
    if not table_rows:
        return "暂无"
    lines = [
        "| 排名 | 素材 | 广告主 | 展示估值 | 人气 | 热度 | 榜单排名 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in table_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["rank"],
                    row["material"],
                    row["advertiser"],
                    row["display_estimate"],
                    row["popularity"],
                    row["heat"],
                    row["chart_ranks"],
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _chart_markdown_table(rows: list[dict[str, Any]], chart_type: str, limit: int) -> str:
    return _rows_markdown_table(_chart_table_rows(rows, chart_type, limit))


def _focus_markdown_table(rows: list[dict[str, Any]], limit: int) -> str:
    return _rows_markdown_table(_focus_table_rows(rows, limit))


def _collapsible_panel(title: str, content: str, *, expanded: bool) -> dict[str, Any]:
    return {
        "tag": "collapsible_panel",
        "expanded": expanded,
        "background_color": "grey",
        "border": {"color": "grey", "corner_radius": "8px"},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": title,
            },
            "background_color": "grey",
        },
        "elements": [{"tag": "markdown", "content": content}],
    }


def build_chart_card_payload(
    title: str,
    rows: list[dict[str, Any]],
    *,
    week_start: str,
    week_end: str,
    source_counts: dict[str, int],
    per_chart_top_n: int = 10,
    focus_top_n: int = 10,
    bitable_url: str = "",
) -> dict[str, Any]:
    count_text = (
        f"新创意榜 {int(source_counts.get('new') or 0)} 条｜"
        f"热门榜 {int(source_counts.get('hot') or 0)} 条｜"
        f"飙升榜 {int(source_counts.get('surge') or 0)} 条"
    )
    elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": (
                f"**{week_start} ~ {week_end}**\n"
                f"抓取结果：{count_text}\n"
                f"去重后：{len(rows)} 条｜展示口径：每榜 Top{per_chart_top_n}，按广大大官方排名排序。"
            ),
        }
    ]
    focus_rows = _focus_table_rows(rows, focus_top_n)
    elements.append(
        _collapsible_panel(
            f"重点关注 Top{focus_top_n} · {len(focus_rows)} 条",
            _rows_markdown_table(focus_rows),
            expanded=True,
        )
    )
    for chart_type in ("new", "hot", "surge"):
        label = CHART_SECTION_LABELS[chart_type]
        table_rows = _chart_table_rows(rows, chart_type, per_chart_top_n)
        elements.append(
            _collapsible_panel(
                f"{label} Top{per_chart_top_n} · {len(table_rows)} 条",
                _chart_markdown_table(rows, chart_type, per_chart_top_n),
                expanded=False,
            )
        )
    if bitable_url.strip():
        url = bitable_url.strip()
        elements.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "查看大盘周榜池"},
                "type": "primary",
                "behaviors": [{"type": "open_url", "default_url": url, "pc_url": url, "ios_url": url, "android_url": url}],
            }
        )
    return {
        "msg_type": "interactive",
        "card": {
            "schema": "2.0",
            "config": {"enable_forward": True, "width_mode": "fill"},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": "blue"},
            "body": {"elements": elements},
        },
    }


def render_markdown(
    rows: list[dict[str, Any]],
    *,
    week_start: str,
    week_end: str,
    source_counts: dict[str, int],
    focus_top_n: int = 10,
    per_chart_top_n: int = 10,
) -> str:
    focus_rows = rank_focus_rows(rows)[:focus_top_n]
    count_text = (
        f"新创意榜 {int(source_counts.get('new') or 0)} 条｜"
        f"热门榜 {int(source_counts.get('hot') or 0)} 条｜"
        f"飙升榜 {int(source_counts.get('surge') or 0)} 条"
    )
    lines = [
        f"**VE 大盘周榜｜{week_start} ~ {week_end}**",
        "",
        f"- 抓取结果：{count_text}",
        f"- 去重后：{len(rows)} 条｜本次重点关注：{len(focus_rows)} 条",
        "- 说明：纯榜单事实排序，不使用 AI 拆解或 AI 评分。",
        "",
        "**一、重点关注**",
    ]
    if not focus_rows:
        lines.append("- 暂无")
    for index, row in enumerate(focus_rows, start=1):
        lines.extend(_line(index, row))
        lines.append("")

    for chart_type in ("new", "hot", "surge"):
        label = CHART_SECTION_LABELS[chart_type]
        section = _section_rows(rows, chart_type, per_chart_top_n)
        lines.extend(["", f"**{label} Top{per_chart_top_n}**"])
        if not section:
            lines.append("- 暂无")
            continue
        for index, row in enumerate(section, start=1):
            lines.extend(_line(index, row))
            lines.append("")
    return "\n".join(lines).strip()


def load_payload(path: Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"榜单 raw 不是 JSON object: {path}")
    return payload


def load_rows_and_counts(paths: Iterable[Path]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    items: list[dict[str, Any]] = []
    counts = {"new": 0, "hot": 0, "surge": 0}
    for path in paths:
        payload = load_payload(Path(path))
        payload_items = [item for item in (payload.get("items") or []) if isinstance(item, dict)]
        chart_type = _text((payload.get("filter_report") or {}).get("chart_type") or "")
        if chart_type not in counts and payload_items:
            chart_type = _chart_type(_creative(payload_items[0]))
        if chart_type in counts:
            counts[chart_type] += len(payload_items)
        items.extend(payload_items)
    return merge_chart_items(items), counts


def build_card_payload(title: str, md_text: str, *, bitable_url: str = "") -> dict[str, Any]:
    elements: list[dict[str, Any]] = [{"tag": "markdown", "content": md_text[:12000]}]
    if bitable_url.strip():
        elements.append(
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "查看大盘周榜池"},
                        "type": "primary",
                        "multi_url": {
                            "url": bitable_url.strip(),
                            "pc_url": bitable_url.strip(),
                            "ios_url": bitable_url.strip(),
                            "android_url": bitable_url.strip(),
                        },
                    }
                ],
            }
        )
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": "blue"},
            "elements": elements,
        },
    }


def resolve_webhook(explicit: str = "") -> str:
    if explicit.strip():
        return explicit.strip()
    return (
        os.getenv("GUANGDADA_CHECK_FEISHU_WEBHOOK")
        or os.getenv("FEISHU_GUANGDADA_CHECK_WEBHOOK")
        or os.getenv("FEISHU_TEST_WEBHOOK")
        or os.getenv("VE_MARKET_WEEKLY_CHARTS_FEISHU_WEBHOOK")
        or os.getenv("VE_FLOW_REPORT_FEISHU_WEBHOOK")
        or os.getenv("FEISHU_UA_WEBHOOK")
        or ""
    ).strip()


def post_card(webhook: str, title: str, md_text: str, *, bitable_url: str = "") -> dict[str, Any]:
    response = requests.post(webhook, json=build_card_payload(title, md_text, bitable_url=bitable_url), timeout=20)
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}
    ok = response.status_code == 200 and (data.get("code") == 0 or data.get("StatusCode") == 0)
    if not ok:
        raise RuntimeError(f"大盘周榜推送失败: status={response.status_code}, resp={data}")
    return data


def post_chart_card(
    webhook: str,
    title: str,
    rows: list[dict[str, Any]],
    *,
    week_start: str,
    week_end: str,
    source_counts: dict[str, int],
    per_chart_top_n: int = 10,
    focus_top_n: int = 10,
    bitable_url: str = "",
) -> dict[str, Any]:
    payload = build_chart_card_payload(
        title,
        rows,
        week_start=week_start,
        week_end=week_end,
        source_counts=source_counts,
        per_chart_top_n=per_chart_top_n,
        focus_top_n=focus_top_n,
        bitable_url=bitable_url,
    )
    response = requests.post(webhook, json=payload, timeout=20)
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}
    ok = response.status_code == 200 and (data.get("code") == 0 or data.get("StatusCode") == 0)
    if not ok:
        raise RuntimeError(f"大盘周榜推送失败: status={response.status_code}, resp={data}")
    return data


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="推送 VE 大盘三榜周榜卡片")
    parser.add_argument("--chart-raw", action="append", required=True, help="榜单 raw JSON，可重复传")
    parser.add_argument("--week-start", required=True, help="周起始日期 YYYY-MM-DD")
    parser.add_argument("--week-end", required=True, help="周结束日期 YYYY-MM-DD")
    parser.add_argument("--focus-top-n", type=int, default=10, help="重点关注条数")
    parser.add_argument("--per-chart-top-n", type=int, default=10, help="每个分榜展示条数")
    parser.add_argument("--feishu-webhook", default="", help="飞书 webhook 覆盖")
    parser.add_argument("--bitable-url", default="", help="大盘周榜池链接；默认读 VE_MARKET_WEEKLY_CHARTS_BITABLE_URL")
    parser.add_argument("--dry-run", action="store_true", help="只写 markdown，不推送")
    parser.add_argument("--output-md", default="", help="输出 markdown 路径")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env(override=True)
    args = parse_args(argv)
    rows, counts = load_rows_and_counts([Path(path) for path in args.chart_raw])
    md_text = render_markdown(
        rows,
        week_start=args.week_start,
        week_end=args.week_end,
        source_counts=counts,
        focus_top_n=max(1, int(args.focus_top_n)),
        per_chart_top_n=max(1, int(args.per_chart_top_n)),
    )
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_md = Path(args.output_md) if args.output_md else REPORTS_DIR / f"ve_market_weekly_charts_{args.week_start}_{args.week_end}.md"
    output_md.write_text(md_text + "\n", encoding="utf-8")
    print(f"[market-weekly-charts] markdown: {output_md}")
    print(f"[market-weekly-charts] deduped rows: {len(rows)}")
    if args.dry_run:
        print(md_text)
        return 0
    webhook = resolve_webhook(args.feishu_webhook)
    if not webhook:
        print("[market-weekly-charts] 未配置 webhook，跳过推送。")
        return 0
    title = f"VE 大盘周榜｜{args.week_start} ~ {args.week_end}"
    bitable_url = (args.bitable_url or os.getenv("VE_MARKET_WEEKLY_CHARTS_BITABLE_URL") or "").strip()
    post_chart_card(
        webhook,
        title,
        rows,
        week_start=args.week_start,
        week_end=args.week_end,
        source_counts=counts,
        per_chart_top_n=max(1, int(args.per_chart_top_n)),
        focus_top_n=max(1, int(args.focus_top_n)),
        bitable_url=bitable_url,
    )
    print("[market-weekly-charts] 飞书推送成功。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
