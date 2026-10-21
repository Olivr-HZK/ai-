# 社交聊天 AI 应用日工作流

该工作流复用 VE 的广大大抓取、目标日 `first_seen` 筛选、封面去重、分析和多维表同步节点，使用独立产物前缀 `workflow_social_chat_YYYY-MM-DD`。

封面日内/历史去重后，会再按广告主执行每日精选：每个广告主最多 10 条。选择权重为封面 CLIP 差异度 90%、展示估值 10%；先用平均视觉距离选择首条，再用 max-min 距离逐条补齐，确保高曝光相似变体不会挤掉明显不同的新创意。完整候选保留在 `creative_library` 供历史去重，`daily_creative_insights`、分析和飞书只保留入选项。报告写入 `data/workflow_social_chat_YYYY-MM-DD_daily_advertiser_selection.json`。

目标 Android appid 配置在 `config/social_chat_apps.json`，爬取时不做广告主名称匹配，只按 appid、目标日期、`ad_key` 去重和重投规则处理。

```bash
SOCIAL_CHAT_BITABLE_URL='https://.../base/...?...table=...' \
  .venv/bin/python scripts/run_social_chat_daily.py --date 2026-08-05
```

默认日期为昨天（UTC+8）。可用 `--bitable-url` 临时覆盖同步目标，`--skip-cover-dedupe` 跳过封面去重，`--no-bitable-sync` 只生成本地产物。日常定时入口为 `scripts/cron_social_chat_daily.sh`。

默认维护独立数据库 `data/afizzy_competitor_pipeline.db`，可用 `--db-path` 或 `AFIZZY_COMPETITOR_DB_PATH` 覆盖。
