# Video Enhancer 验收报告（2026-05-07）
## 一句话结论
本日验收**通过**：关键检查未发现需立即处理的问题。
## 汇总

| 项目 | 值 |
| --- | --- |
| 结论 | **通过** |
| 健康分 | **100** / 100 |
| 严重 / 需关注 / 提示 | 0 / 0 / 0 |

- **严重**：缺关键文件或无法解析，当日结果不可靠。
- **需关注**：成功率、推送表、方向卡片等异常。
- **提示**：量级波动、日志缺失等，建议人工看一眼。

## 待办清单

*本日无待办项。*

## 阶段摘要（机器可读见 JSON）

```json
{
  "raw": {
    "path": "/Users/ggbond/oliver/ai-/data/workflow_video_enhancer_2026-05-07_raw.json",
    "exists": true,
    "item_count": 40,
    "filter_post_total": 52,
    "filter_pre_total": 117,
    "filter_log_after_total": 52
  },
  "analysis": {
    "path": "/Users/ggbond/oliver/ai-/data/video_analysis_workflow_video_enhancer_2026-05-07_raw.json",
    "exists": true,
    "analyzed_items": 40,
    "new_success": 40,
    "new_failed": 0,
    "pipeline_items": 40,
    "attempted_new": 40,
    "ad_key_count": 40,
    "exclude_cluster_semantic_count": 4,
    "exclude_cluster_launched_hint": 0
  },
  "cluster": {
    "path": "/Users/ggbond/oliver/ai-/data/ua_suggestion_workflow_video_enhancer_2026-05-07.json",
    "exists": true,
    "skipped_llm": false,
    "llm_error": null,
    "card_count": 3
  },
  "filters": {
    "filter_step3": {
      "input": 50,
      "output": 40,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 0
    }
  },
  "dedup_report": {
    "path": "/Users/ggbond/oliver/ai-/data/workflow_video_enhancer_2026-05-07_analysis_dedup_report.json",
    "exists": true
  },
  "sync": {
    "log_path": "/Users/ggbond/oliver/ai-/logs/daily_video_enhancer_workflow_2026-05-07.log",
    "exists": false,
    "sync_mentioned": null,
    "note": "该文件通常只有跑 `scripts/daily_video_enhancer_workflow.sh`（内部 tee）才会出现；若只手动执行 `python scripts/workflow_video_enhancer_full_pipeline.py`，可能无此日志，不算失败。"
  },
  "push": {
    "daily_ua_push_rows": 3,
    "should_persist_push_table": true
  },
  "history": {
    "lookback_days": 7,
    "history_pairs": [],
    "today_post_total": 52,
    "history_mean_after_cnt": 0.0
  }
}
```
