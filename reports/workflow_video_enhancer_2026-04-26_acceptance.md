# Video Enhancer 验收报告（2026-04-26）
## 一句话结论
本日验收**通过**：关键检查未发现需立即处理的问题。
## 汇总

| 项目 | 值 |
| --- | --- |
| 结论 | **通过** |
| 健康分 | **97** / 100 |
| 严重 / 需关注 / 提示 | 0 / 0 / 1 |

- **严重**：缺关键文件或无法解析，当日结果不可靠。
- **需关注**：成功率、推送表、方向卡片等异常。
- **提示**：量级波动、日志缺失等，建议人工看一眼。

## 待办清单

- [warn] cluster_skipped: 方向卡片阶段 skipped_llm=True。

## 阶段摘要（机器可读见 JSON）

```json
{
  "raw": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-26_raw.json",
    "exists": true,
    "item_count": 25,
    "filter_post_total": 30,
    "filter_pre_total": 55,
    "filter_log_after_total": 30
  },
  "analysis": {
    "path": "/Users/oliver/guru/ua素材/data/video_analysis_workflow_video_enhancer_2026-04-26_raw.json",
    "exists": true,
    "analyzed_items": 25,
    "new_success": 25,
    "new_failed": 0,
    "pipeline_items": 25,
    "attempted_new": 25,
    "ad_key_count": 25,
    "exclude_cluster_semantic_count": 0,
    "exclude_cluster_launched_hint": 25
  },
  "cluster": {
    "path": "/Users/oliver/guru/ua素材/data/ua_suggestion_workflow_video_enhancer_2026-04-26.json",
    "exists": true,
    "skipped_llm": true,
    "llm_error": null,
    "card_count": 0
  },
  "filters": {
    "filter_step3": {
      "input": 30,
      "output": 25,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 25
    }
  },
  "dedup_report": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-26_analysis_dedup_report.json",
    "exists": true
  },
  "sync": {
    "log_path": "/Users/oliver/guru/ua素材/logs/daily_video_enhancer_workflow_2026-04-26.log",
    "exists": true,
    "sync_mentioned": false,
    "note": null
  },
  "push": {
    "daily_ua_push_rows": 0,
    "should_persist_push_table": false
  },
  "history": {
    "lookback_days": 7,
    "history_pairs": [
      [
        "2026-04-19",
        45
      ],
      [
        "2026-04-20",
        32
      ],
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
      ]
    ],
    "today_post_total": 30,
    "history_mean_after_cnt": 45.2
  }
}
```
