# Video Enhancer 验收报告 `2026-04-09`

- **状态**: pass
- **得分**: 100
- **计数**: {'hard': 0, 'soft': 0, 'warn': 0}

## 问题列表

- （无）

## 阶段明细

```json
{
  "raw": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-09_raw.json",
    "exists": true,
    "item_count": 16,
    "filter_post_total": 41,
    "filter_pre_total": 91,
    "filter_log_after_total": 41
  },
  "analysis": {
    "path": "/Users/oliver/guru/ua素材/data/video_analysis_workflow_video_enhancer_2026-04-09_raw.json",
    "exists": true,
    "analyzed_items": 16,
    "new_success": 7,
    "new_failed": 0,
    "pipeline_items": 16,
    "attempted_new": 7,
    "ad_key_count": 16,
    "exclude_cluster_semantic_count": 1,
    "exclude_cluster_launched_hint": 9
  },
  "cluster": {
    "path": "/Users/oliver/guru/ua素材/data/ua_suggestion_workflow_video_enhancer_2026-04-09.json",
    "exists": true,
    "skipped_llm": false,
    "llm_error": null,
    "card_count": 3
  },
  "filters": {
    "filter_step3": {
      "input": 41,
      "output": 16,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 0
    }
  },
  "sync": {
    "log_path": "/Users/oliver/guru/ua素材/logs/daily_video_enhancer_workflow_2026-04-09.log",
    "exists": true,
    "sync_mentioned": true
  },
  "push": {
    "daily_ua_push_rows": 6,
    "should_persist_push_table": true
  },
  "history": {
    "lookback_days": 7,
    "history_pairs": [
      [
        "2026-04-03",
        39
      ],
      [
        "2026-04-04",
        30
      ],
      [
        "2026-04-05",
        41
      ],
      [
        "2026-04-06",
        50
      ],
      [
        "2026-04-07",
        51
      ],
      [
        "2026-04-08",
        35
      ]
    ],
    "today_post_total": 41,
    "history_mean_after_cnt": 41.0
  }
}
```

- JSON: `/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-09_acceptance.json`
