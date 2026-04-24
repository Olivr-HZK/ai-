# Video Enhancer 验收报告（2026-04-19）

## 一句话结论

本日验收通过：关键文件齐全、结构正常，未发现需要立即处理的问题。

## 汇总

| 项目 | 值 |
| --- | --- |
| 结论 | **通过** |
| 健康分 | **100** / 100 |
| 严重 / 需关注 / 提示 | 0 / 0 / 0 |

三类含义：**严重**＝缺文件或文件坏了，当天结果先别当真；**需关注**＝成功率、同步、入库等对不上；**提示**＝波动或提醒你看一眼日志。

## 健康分怎么扣

- 从 **100 分**起算，**扣到不低于 0 分**（再差也不会显示成负数）。
- **严重**问题：每条扣 **25** 分（一般是缺文件、JSON 坏了，当天结果不可靠）。
- **需关注**问题：每条扣 **10** 分（例如新分析失败太多、推送表该有却没有等）。
- **提示**问题：每条扣 **3** 分（例如抓取量波动、日志里有个 Traceback 等，需要人看一眼）。
- **本日**：严重 **0** 条 → 扣 **0**；需关注 **0** 条 → 扣 **0**；提示 **0** 条 → 扣 **0**；合计扣 **0** → **得分 100**。

## 待办清单

*本日无待办项。*
## 流水线（按先后顺序）

下面按「实际跑的顺序」写：从抓取 → 封面 → 分析 → 已上线比对 → 方向卡片 → 同步 → 推送 → 和前几天比。

1. **抓取与截断**：进入原始清单 **36** 条。 按规则先 **64** 条 → 截断后 **45** 条。
2.    · 数据库里记的「当日截断后合计」：**45** 条（可与上面对照）。
3. **封面去重**：进 **42** 条 → 出 **36** 条（本环节是否跳过：**否**）。
4. **灵感分析**：一共 **36** 条有分析文字；本轮新跑的：成功 **1**、失败 **0**。
5. **和「我方已上线特效」比对**：命中 **0** 条（这些会少进主表等，避免重复展示）。
6. **方向卡片**：共 **3** 个方向（已走大模型）。
7. **飞书多维表同步**：未找到按日 tee 的日志文件（**只手动跑 Python 全流程时通常没有**，只有跑 `daily_video_enhancer_workflow.sh` 或自己重定向到 `logs/daily_video_enhancer_workflow_日期.log` 才会有）。本项无法从文件里帮你核对同步，**不算失败**。
8. **推送表（当天行数）**：**3** 行；按规则今天该不该写入：**是**。
9. **和前几天比**：今天截断后 **45** 条；近几天平均大约 **43.6**。

## 给程序看的原始数据

各阶段完整字段在同目录 JSON：**`/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-19_acceptance.json`**（一般人看上面人话即可）

```json
{
  "raw": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-19_raw.json",
    "exists": true,
    "item_count": 36,
    "filter_post_total": 45,
    "filter_pre_total": 64,
    "filter_log_after_total": 45
  },
  "analysis": {
    "path": "/Users/oliver/guru/ua素材/data/video_analysis_workflow_video_enhancer_2026-04-19_raw.json",
    "exists": true,
    "analyzed_items": 36,
    "new_success": 1,
    "new_failed": 0,
    "pipeline_items": null,
    "attempted_new": 1,
    "ad_key_count": 36,
    "exclude_cluster_semantic_count": 0,
    "exclude_cluster_launched_hint": 24
  },
  "cluster": {
    "path": "/Users/oliver/guru/ua素材/data/ua_suggestion_workflow_video_enhancer_2026-04-19.json",
    "exists": true,
    "skipped_llm": false,
    "llm_error": null,
    "card_count": 3
  },
  "filters": {
    "filter_step3": {
      "input": 42,
      "output": 36,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 0
    }
  },
  "sync": {
    "log_path": "/Users/oliver/guru/ua素材/logs/daily_video_enhancer_workflow_2026-04-19.log",
    "exists": false,
    "note": "该文件通常只有跑 `scripts/daily_video_enhancer_workflow.sh`（内部用 tee 写入）才会出现；若你**只手动执行** `python scripts/workflow_video_enhancer_full_pipeline.py`，**不会生成**此日志，属正常，验收无法从文件里检查同步是否报错。"
  },
  "push": {
    "daily_ua_push_rows": 3,
    "should_persist_push_table": true
  },
  "history": {
    "lookback_days": 7,
    "history_pairs": [
      [
        "2026-04-12",
        41
      ],
      [
        "2026-04-13",
        54
      ],
      [
        "2026-04-14",
        47
      ],
      [
        "2026-04-15",
        31
      ],
      [
        "2026-04-16",
        45
      ]
    ],
    "today_post_total": 45,
    "history_mean_after_cnt": 43.6
  }
}
```