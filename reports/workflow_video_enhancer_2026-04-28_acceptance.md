# Video Enhancer 验收报告（2026-04-28）
## 一句话结论
本日验收**有提示项**：无严重问题，请查看下表与待办。
## 汇总

| 项目 | 值 |
| --- | --- |
| 结论 | **有提示** |
| 健康分 | **87** / 100 |
| 严重 / 需关注 / 提示 | 0 / 1 / 1 |

- **严重**：缺关键文件或无法解析，当日结果不可靠。
- **需关注**：成功率、推送表、方向卡片等异常。
- **提示**：量级波动、日志缺失等，建议人工看一眼。

## 待办清单

- [soft] analysis_failures: 本轮新分析失败 1 条，见 analysis_failed 清单。
- [warn] high_cover_removal: 封面环节剔除 37.0%（29/46），超过提示阈值 30%。

## 阶段摘要（机器可读见 JSON）

```json
{
  "raw": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-28_raw.json",
    "exists": true,
    "item_count": 29,
    "filter_post_total": 46,
    "filter_pre_total": 76,
    "filter_log_after_total": 46
  },
  "analysis": {
    "path": "/Users/oliver/guru/ua素材/data/video_analysis_workflow_video_enhancer_2026-04-28_raw.json",
    "exists": true,
    "analyzed_items": 29,
    "new_success": 28,
    "new_failed": 1,
    "pipeline_items": 29,
    "attempted_new": 29,
    "ad_key_count": 29,
    "exclude_cluster_semantic_count": 0,
    "exclude_cluster_launched_hint": 0
  },
  "cluster": {
    "path": "/Users/oliver/guru/ua素材/data/ua_suggestion_workflow_video_enhancer_2026-04-28.json",
    "exists": false,
    "skipped_llm": false,
    "llm_error": null,
    "card_count": 0
  },
  "filters": {
    "filter_step3": {
      "input": 46,
      "output": 29,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 0
    }
  },
  "dedup_report": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-28_analysis_dedup_report.json",
    "exists": true
  },
  "sync": {
    "log_path": "/Users/oliver/guru/ua素材/logs/daily_video_enhancer_workflow_2026-04-28.log",
    "exists": false,
    "sync_mentioned": null,
    "note": "该文件通常只有跑 `scripts/daily_video_enhancer_workflow.sh`（内部 tee）才会出现；若只手动执行 `python scripts/workflow_video_enhancer_full_pipeline.py`，可能无此日志，不算失败。"
  },
  "push": {
    "daily_ua_push_rows": 0,
    "should_persist_push_table": false
  },
  "history": {
    "lookback_days": 7,
    "history_pairs": [
      [
        "2026-04-21",
        52
      ],
      [
        "2026-04-22",
        53
      ],
      [
        "2026-04-23",
        44
      ],
      [
        "2026-04-26",
        30
      ],
      [
        "2026-04-27",
        51
      ]
    ],
    "today_post_total": 46,
    "history_mean_after_cnt": 46.0
  },
  "_partial": true
}
```
