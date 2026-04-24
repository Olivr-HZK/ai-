# Video Enhancer 验收报告（2026-04-21）

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

1. **抓取与截断**：进入原始清单 **39** 条。 按规则先 **129** 条 → 截断后 **52** 条。
2.    · 数据库里记的「当日截断后合计」：**52** 条（可与上面对照）。
3. **封面去重**：进 **50** 条 → 出 **39** 条（本环节是否跳过：**否**）。
4. **灵感分析**：一共 **37** 条有分析文字；本轮新跑的：成功 **37**、失败 **1**。
5. **和「我方已上线特效」比对**：命中 **31** 条（这些会少进主表等，避免重复展示）。
6. **和前几天比**：今天截断后 **52** 条；近几天平均大约 **40.0**。

## 给程序看的原始数据

各阶段完整字段在同目录 JSON：**`/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-21_acceptance.json`**（一般人看上面人话即可）

```json
{
  "raw": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-21_raw.json",
    "exists": true,
    "item_count": 39,
    "filter_post_total": 52,
    "filter_pre_total": 129,
    "filter_log_after_total": 52
  },
  "analysis": {
    "path": "/Users/oliver/guru/ua素材/data/video_analysis_workflow_video_enhancer_2026-04-21_raw.json",
    "exists": true,
    "analyzed_items": 37,
    "new_success": 37,
    "new_failed": 1,
    "pipeline_items": 39,
    "attempted_new": 38,
    "ad_key_count": 37,
    "exclude_cluster_semantic_count": 0,
    "exclude_cluster_launched_hint": 31
  },
  "filters": {
    "filter_step3": {
      "input": 50,
      "output": 39,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 31
    }
  },
  "history": {
    "lookback_days": 7,
    "history_pairs": [
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
      ],
      [
        "2026-04-19",
        45
      ],
      [
        "2026-04-20",
        32
      ]
    ],
    "today_post_total": 52,
    "history_mean_after_cnt": 40.0
  }
}
```