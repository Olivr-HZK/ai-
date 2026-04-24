# Video Enhancer 验收报告（2026-04-23）
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
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-23_raw.json",
    "exists": true,
    "item_count": 30,
    "filter_post_total": 44,
    "filter_pre_total": 71,
    "filter_log_after_total": 44
  },
  "analysis": {
    "path": "/Users/oliver/guru/ua素材/data/video_analysis_workflow_video_enhancer_2026-04-23_raw.json",
    "exists": true,
    "analyzed_items": 30,
    "new_success": 30,
    "new_failed": 0,
    "pipeline_items": 30,
    "attempted_new": 30,
    "ad_key_count": 30,
    "exclude_cluster_semantic_count": 0,
    "exclude_cluster_launched_hint": 30
  },
  "cluster": {
    "path": "/Users/oliver/guru/ua素材/data/ua_suggestion_workflow_video_enhancer_2026-04-23.json",
    "exists": true,
    "skipped_llm": true,
    "llm_error": null,
    "card_count": 0
  },
  "filters": {
    "filter_step3": {
      "input": 40,
      "output": 30,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 30
    }
  },
  "dedup_report": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-23_analysis_dedup_report.json",
    "exists": true
  },
  "sync": {
    "log_path": "/Users/oliver/guru/ua素材/logs/daily_video_enhancer_workflow_2026-04-23.log",
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
        "2026-04-16",
        45
      ],
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
      ]
    ],
    "today_post_total": 44,
    "history_mean_after_cnt": 45.4
  }
}
```
