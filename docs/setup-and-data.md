# 环境与数据

## Python 与环境

```bash
cd /path/to/repo
python3 -m venv .venv
source .venv/bin/activate   # 可选；cron 直接使用 .venv/bin/python
pip install -r requirements.txt
playwright install chromium
```

- 依赖列表：`requirements.txt`（Playwright、OpenAI、`lark-oapi`、`sentence-transformers`、Torch 等）。
- **cron** 脚本不 `activate`，直接调用 `**./venv/bin/python`**（路径相对于仓库根）。

## `.env`

在项目根提供 `**.env`**（勿提交密钥）。常用项包括：


| 类别            | 示例变量                                                                 |
| ------------- | -------------------------------------------------------------------- |
| 广大大           | `GUANGDADA_EMAIL`、`GUANGDADA_PASSWORD`                               |
| LLM           | `OPENROUTER_API_KEY` 等                                               |
| VE 多维表        | `VIDEO_ENHANCER_BITABLE_URL`、可选 `VIDEO_ENHANCER_CLUSTER_BITABLE_URL` |
| VE 筛选口径       | `INTRADAY_EFFECT_FILTER_ENABLED`、`INTRADAY_EFFECT_SIMILARITY_THRESHOLD`、`OLD_EFFECT_BITABLE_FILTER_ENABLED`、`OLD_EFFECT_SIMILARITY_THRESHOLD`、`OLD_EFFECT_LOOKBACK_DAYS`、`EFFECT_EMBEDDING_DUP_FILTER_ENABLED`、`EFFECT_EMBEDDING_INTRADAY_HARD_THRESHOLD`、`EFFECT_EMBEDDING_CROSSDAY_HARD_THRESHOLD`、`EFFECT_EMBEDDING_LOOKBACK_DAYS`、`EMBEDDING_DUP_CANDIDATE_ENABLED`、`EMBEDDING_DUP_CANDIDATE_THRESHOLD`、`DAILY_PLAY_CLUSTER_TEXT_THRESHOLD`、`DAILY_PLAY_CLUSTER_EMBEDDING_THRESHOLD`、`DAILY_PLAY_CLUSTER_EMBEDDING_ENABLED`、`VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_ENABLED` |
| 飞书写入          | `FEISHU_APP_ID`、`FEISHU_APP_SECRET`                                  |
| Arrow2 SQLite | `ARROW2_SQLITE_PATH`（可选，默认 `data/arrow2_pipeline.db`）                |


具体以各模块 `load_dotenv` 与报错提示为准。

## `data/` 目录


| 路径                                | 说明                                                     |
| --------------------------------- | ------------------------------------------------------ |
| `data/video_enhancer_pipeline.db` | VE：`daily_creative_insights`、`creative_library`、用量与验收等 |
| `data/arrow2_pipeline.db`         | Arrow2 主库与日快照（默认路径）                                    |
| `data/haopeng_topn_experiments/`  | VE 浩鹏二次 AI 筛选 TopN JSON，本地产物不提交                  |
| `data/remote_snapshots/ve/`       | 从 Mac mini 拉取的 VE-only 生产库快照，本地产物不提交             |
| `data/*.json`                     | 各工作流离线 raw / analysis / 报告                             |
| `logs/`                           | cron 与手工重定向的运行日志（可 gitignore）                          |


`data/` 与 `reports/` 在运行时自动创建目录。

## VE 玩法资产数据

- 本地资产库：`config/ve_play_assets.json`，作为运行兜底和版本化快照。
- 协作源：飞书云文档，详见 [ve-play-assets.md](./ve-play-assets.md)；分析进程启动时会尝试拉取云文档并覆盖本地 JSON，失败则继续使用本地版本。
- 内部主题来源：Google Sheet「AI产品热点排期表 / 特效上线记录」，当前用于补充 aliases、关键词、子标签与案例，不作为运行时必需依赖。
- SQLite 自动迁移：VE 库的 `daily_creative_insights` 与 `creative_library` 会自动补齐 `play_asset_id`、`play_asset_name`、`play_asset_subtag_ids`、`play_asset_subtag_names`、`play_asset_novelty_label`、`play_asset_match_source`、`play_asset_classification_reason` 等字段，保留分析阶段 AI 玩法判断。

## 换机迁移历史库（SQLite）

封面跨日、`play_fingerprint` / `effect_one_liner` 等逻辑依赖 `**creative_library` / `daily_*` / arrow2 表** 中已有数据。

1. **停掉**旧机与新机正在访问同一 `.db` 的 Python / cron。
2. 将旧机 `**data/video_enhancer_pipeline.db`、`data/arrow2_pipeline.db`**（及你在用的其它 `data/*.db`）**整文件拷贝**到新机同名路径覆盖。
3. 避免两台机器同时对同一 **飞书多维表** 并行写入同一业务日，以免造成重复行或错乱。

参考：[cron-schedules.md](./cron-schedules.md)。

## 从 Mac mini 拉取 VE 生产快照

需要只读排查远端生产库时，使用固定入口：

```bash
scripts/sync_ve_data_from_remote.sh
sqlite3 data/remote_snapshots/ve/data/video_enhancer_pipeline.db 'PRAGMA quick_check;'
sqlite3 data/remote_snapshots/ve/data/video_enhancer_pipeline.db "SELECT MAX(target_date) FROM daily_creative_insights;"
```

脚本默认远端为 `ggbond@10.125.46.30:/Users/ggbond/oliver/ai-`，可用 `VE_REMOTE_HOST`、`VE_REMOTE_ROOT`、`VE_REMOTE_SNAPSHOT_DIR` 覆盖。同步只拉 VE 相关快照和产物，远端先执行 `sqlite3 .backup`，本地落到 `data/remote_snapshots/ve/`；不拉 Arrow2 数据。

## `archive/`

历史上移除的脚本与旧 shell 在 `archive/removed_scripts_2026_05_06/` 等处；现行入口以根目录 README 与 [workflows.md](./workflows.md) 为准。
