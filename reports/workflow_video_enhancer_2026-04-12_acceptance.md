# Video Enhancer 验收报告（2026-04-12）

## 一句话结论

本日验收**有警告**：严重 **0** 条，需跟进 **0** 条，提示 **1** 条。多为波动或阈值提示，可按清单逐项确认。

## 汇总

| 项目 | 值 |
| --- | --- |
| 结论 | **有警告** |
| 健康分 | **97** / 100 |
| 严重 / 需关注 / 提示 | 0 / 0 / 1 |

三类含义：**严重**＝缺文件或文件坏了，当天结果先别当真；**需关注**＝成功率、同步、入库等对不上；**提示**＝波动或提醒你看一眼日志。

## 健康分怎么扣

- 从 **100 分**起算，**扣到不低于 0 分**（再差也不会显示成负数）。
- **严重**问题：每条扣 **25** 分（一般是缺文件、JSON 坏了，当天结果不可靠）。
- **需关注**问题：每条扣 **10** 分（例如新分析失败太多、推送表该有却没有等）。
- **提示**问题：每条扣 **3** 分（例如抓取量波动、日志里有个 Traceback 等，需要人看一眼）。
- **本日**：严重 **0** 条 → 扣 **0**；需关注 **0** 条 → 扣 **0**；提示 **1** 条 → 扣 **3**；合计扣 **3** → **得分 97**。

## 待办清单

### 提示 · 找不到当日工作流日志，无法检查同步是否报错

未找到当日 workflow 日志，无法自动校验多维表同步子进程（排查代号：`workflow_log_missing`）

## 流水线（按先后顺序）

下面按「实际跑的顺序」写：从抓取 → 封面 → 分析 → 已上线比对 → 方向卡片 → 同步 → 推送 → 和前几天比。

1. **抓取与截断**：进入原始清单 **22** 条。 按规则先 **58** 条 → 截断后 **41** 条。
2.    · 数据库里记的「当日截断后合计」：**41** 条（可与上面对照）。
3. **封面去重**：进 **36** 条 → 出 **22** 条（本环节是否跳过：**否**）。
4. **灵感分析**：一共 **22** 条有分析文字；本轮新跑的：成功 **22**、失败 **0**。
5. **和「我方已上线特效」比对**：命中 **0** 条（这些会少进主表等，避免重复展示）。
6. **方向卡片**：共 **3** 个方向（已走大模型）。
7. **飞书多维表同步**：当日总日志找不到，只能从这里粗看「同步那一步有没有报错」。
8. **推送表（当天行数）**：**3** 行；按规则今天该不该写入：**是**。
9. **和前几天比**：今天截断后 **41** 条；近几天平均大约 **44.67**。

## 给程序看的原始数据

各阶段完整字段在同目录 JSON：**`/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-12_acceptance.json`**（一般人看上面人话即可）

```json
{
  "raw": {
    "path": "/Users/oliver/guru/ua素材/data/workflow_video_enhancer_2026-04-12_raw.json",
    "exists": true,
    "item_count": 22,
    "filter_post_total": 41,
    "filter_pre_total": 58,
    "filter_log_after_total": 41
  },
  "analysis": {
    "path": "/Users/oliver/guru/ua素材/data/video_analysis_workflow_video_enhancer_2026-04-12_raw.json",
    "exists": true,
    "analyzed_items": 22,
    "new_success": 22,
    "new_failed": 0,
    "pipeline_items": 22,
    "attempted_new": 22,
    "ad_key_count": 22,
    "exclude_cluster_semantic_count": 0,
    "exclude_cluster_launched_hint": 0
  },
  "cluster": {
    "path": "/Users/oliver/guru/ua素材/data/ua_suggestion_workflow_video_enhancer_2026-04-12.json",
    "exists": true,
    "skipped_llm": false,
    "llm_error": null,
    "card_count": 3
  },
  "filters": {
    "filter_step3": {
      "input": 36,
      "output": 22,
      "skipped": false
    },
    "filter_step4": {
      "marked_count": 0
    }
  },
  "sync": {
    "log_path": "/Users/oliver/guru/ua素材/logs/daily_video_enhancer_workflow_2026-04-12.log",
    "exists": false
  },
  "push": {
    "daily_ua_push_rows": 3,
    "should_persist_push_table": true
  },
  "history": {
    "lookback_days": 7,
    "history_pairs": [
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
      ],
      [
        "2026-04-09",
        41
      ],
      [
        "2026-04-10",
        50
      ]
    ],
    "today_post_total": 41,
    "history_mean_after_cnt": 44.67
  }
}
```