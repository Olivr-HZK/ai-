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
- **cron** 脚本不 `activate`，直接调用 **`./venv/bin/python`**（路径相对于仓库根）。

## `.env`

在项目根提供 **`.env`**（勿提交密钥）。常用项包括：

| 类别 | 示例变量 |
|------|----------|
| 广大大 | `GUANGDADA_EMAIL`、`GUANGDADA_PASSWORD` |
| LLM | `OPENROUTER_API_KEY` 等 |
| VE 多维表 | `VIDEO_ENHANCER_BITABLE_URL`、可选 `VIDEO_ENHANCER_CLUSTER_BITABLE_URL` |
| 飞书写入 | `FEISHU_APP_ID`、`FEISHU_APP_SECRET` |
| Arrow2 SQLite | `ARROW2_SQLITE_PATH`（可选，默认 `data/arrow2_pipeline.db`） |

具体以各模块 `load_dotenv` 与报错提示为准。

## `data/` 目录

| 路径 | 说明 |
|------|------|
| `data/video_enhancer_pipeline.db` | VE：`daily_creative_insights`、`creative_library`、用量与验收等 |
| `data/arrow2_pipeline.db` | Arrow2 主库与日快照（默认路径） |
| `data/*.json` | 各工作流离线 raw / analysis / 报告 |
| `logs/` | cron 与手工重定向的运行日志（可 gitignore） |

`data/` 与 `reports/` 在运行时自动创建目录。

## 换机迁移历史库（SQLite）

封面跨日、`effect_one_liner` 等逻辑依赖 **`creative_library` / `daily_*` / arrow2 表** 中已有数据。

1. **停掉**旧机与新机正在访问同一 `.db` 的 Python / cron。
2. 将旧机 **`data/video_enhancer_pipeline.db`、`data/arrow2_pipeline.db`**（及你在用的其它 `data/*.db`）**整文件拷贝**到新机同名路径覆盖。
3. 避免两台机器同时对同一 **飞书多维表** 并行写入同一业务日，以免造成重复行或错乱。

参考：[cron-schedules.md](./cron-schedules.md)。

## `archive/`

历史上移除的脚本与旧 shell 在 `archive/removed_scripts_2026_05_06/` 等处；现行入口以根目录 README 与 [workflows.md](./workflows.md) 为准。
