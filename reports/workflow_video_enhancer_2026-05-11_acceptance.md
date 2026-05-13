# Video Enhancer 验收报告（2026-05-11）
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

- [warn] high_cover_removal: 封面环节剔除 41.9%（43/74），超过提示阈值 30%。

## 阶段摘要（机器可读见 JSON）

```json
{
  "raw": {
    "path": "/Users/ggbond/oliver/ai-/data/workflow_video_enhancer_2026-05-11_raw.json",
    "exists": true,
    "item_count": 43,
    "filter_post_total": 76,
    "filter_pre_total": 76,
    "filter_log_after_total": 76
  },
  "analysis": {
    "path": "/Users/ggbond/oliver/ai-/data/video_analysis_workflow_video_enhancer_2026-05-11_raw.json",
    "exists": true,
    "analyzed_items": 43,
    "new_success": 43,
    "new_failed": 0,
    "pipeline_items": 43,
    "attempted_new": 43,
    "ad_key_count": 43,
    "exclude_cluster_semantic_count": 2,
    "exclude_cluster_launched_hint": 0
  },
  "cluster": {
    "path": "/Users/ggbond/oliver/ai-/data/ua_suggestion_workflow_video_enhancer_2026-05-11.json",
    "exists": true,
    "skipped_llm": false,
    "llm_error": null,
    "card_count": 3
  },
  "filters": {
    "filter_step3": {
      "input": 74,
      "output": 43,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 0
    }
  },
  "dedup_report": {
    "path": "/Users/ggbond/oliver/ai-/data/workflow_video_enhancer_2026-05-11_analysis_dedup_report.json",
    "exists": true
  },
  "sync": {
    "log_path": "/Users/ggbond/oliver/ai-/logs/daily_video_enhancer_workflow_2026-05-11.log",
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
    "history_pairs": [
      [
        "2026-05-07",
        52
      ],
      [
        "2026-05-08",
        51
      ],
      [
        "2026-05-09",
        103
      ],
      [
        "2026-05-10",
        103
      ]
    ],
    "today_post_total": 76,
    "history_mean_after_cnt": 77.25
  },
  "material_report": {
    "summary": {
      "candidate_material_count": 36,
      "new_material_count": 5,
      "new_effect_count": 4,
      "new_play_cluster_count": 4,
      "new_play_representative_count": 4,
      "new_play_duplicate_material_count": 1,
      "old_effect_new_material_count": 31,
      "unknown_effect_new_material_count": 0,
      "effect_one_liner_present_count": 5,
      "effect_one_liner_coverage": 1.0,
      "candidate_effect_one_liner_present_count": 36,
      "candidate_effect_one_liner_coverage": 1.0,
      "sustained_display_count": 11,
      "sustained_signal_count": 34
    },
    "error": null
  }
}
```
