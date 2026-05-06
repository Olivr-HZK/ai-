# Three Workflow Package Refactor Design

## Goal

将项目从“脚本堆叠”重构为围绕三个保留工作流的 Python 包结构：

- Video Enhancer 日流程
- Arrow2 `latest_yesterday` 每日最新
- Arrow2 `exposure_top10` 展示估值

旧定时任务兼容不是目标。验收标准是新的三个整合入口能在项目根目录稳定运行，且非三条工作流所需脚本被清理。

## Target Structure

```text
ua素材/
├── ua_workflows/
│   ├── __init__.py
│   ├── cli.py
│   ├── video_enhancer/
│   │   ├── __init__.py
│   │   ├── pipeline.py
│   │   ├── crawl.py
│   │   ├── analyze.py
│   │   ├── sync.py
│   │   └── push.py
│   ├── arrow2/
│   │   ├── __init__.py
│   │   ├── pipeline.py
│   │   ├── crawl.py
│   │   ├── cover_dedupe.py
│   │   └── sync.py
│   └── shared/
│       ├── __init__.py
│       ├── config.py
│       ├── db/
│       ├── feishu/
│       ├── guangdada/
│       ├── llm/
│       └── media/
├── scripts/
│   ├── run_video_enhancer.py
│   ├── run_arrow2_latest.py
│   └── run_arrow2_exposure.py
├── config/
├── data/
└── reports/
```

`scripts/` 只保留三个薄入口。实际逻辑迁入 `ua_workflows/`。入口运行方式：

```bash
.venv/bin/python scripts/run_video_enhancer.py --date 2026-05-05
.venv/bin/python scripts/run_arrow2_latest.py --date 2026-05-05 --analyze
.venv/bin/python scripts/run_arrow2_exposure.py --date 2026-05-05 --analyze
```

## Keep Scope

必须保留并迁移的功能闭包：

- VE：爬取、DOM 补全、封面 CLIP 去重、灵感分析、语义/已投放过滤、飞书同步、飞书卡片、企微/Sheet 推送、验收。
- Arrow2：统一 detail-v2 爬虫、raw 入库、封面 CLIP 去重、可选灵感分析、飞书同步。
- 共享：广大大登录/搜索、代理、LLM、媒体解析、Feishu 附件/字段同步、SQLite 入库、路径配置。

必须保留的配置：

- `config/ai_product.json`
- `config/arrow2_competitor.json`
- `config/iso3166_alpha3_zh.json`
- `config/产品手册_AI工具类_表格 2.csv`（可选但用于 VE prompt 上下文）

## Cleanup Scope

清理以下非目标工作流内容：

- `scripts/legacy/`
- hot rank / new rank：`hot_rank_step*.py`、`workflow_competitor_hot_rank.py`、`workflow_competitor_new_rank.py`、`compute_new_rank_diff.py`
- 非生产链路测试/调试：除原生产依赖的 VE/Arrow2 爬虫和 DOM 工具外的 `test_*.py`、`debug*.py`
- 一次性维护脚本：`backfill*.py`、`preview*.py`、规则抽取实验、手工导出、手工清库等
- 旧 UA / playable / 周榜 / 非三条入口的辅助流程

清理方式优先使用 git 友好的文件移动与删除。若某文件可能还有参考价值但不在目标闭包内，归档到 `archive/removed_scripts_2026_05_06/`；确认无价值的临时文件可直接删除。

## Import Strategy

当前代码大量依赖从 `scripts/*.py` 启动时产生的 `scripts/` 路径。重构后统一为包化 import：

- 包内使用 `from ua_workflows... import ...`
- 薄入口在启动时只负责把项目根加入 `sys.path`
- 子进程调用替换为直接函数调用；确需进程隔离的 LLM/Playwright 步骤可先保留函数边界，避免 shell 路径依赖
- 所有路径通过 `ua_workflows.shared.config.PROJECT_ROOT`、`DATA_DIR`、`CONFIG_DIR`、`REPORTS_DIR` 获取

## Risks

- `arrow2_cover_style_intraday` 依赖 `cover_style_intraday` 的私有函数，迁移时需要一起调整引用。
- `sync_arrow2_to_bitable` 复用 VE 同步脚本内的 Feishu 工具函数，应先抽到 `shared/feishu/` 再迁移两边调用。
- `analyze_video_from_raw_json.py` 同时服务 VE 和 Arrow2，迁移时不能拆断 `--arrow2` 路径的入库逻辑。
- Playwright 脚本和 `run_search_workflow` 依赖当前 cwd 与 `.env` 加载，入口必须固定从项目根运行。
- 外部依赖如 `yt-dlp`、本地 embedding 模型、Feishu credentials 不属于结构重构本身，但验证时要区分 import 错误和外部服务错误。

## Verification

基础验证：

```bash
source .venv/bin/activate
python -m compileall ua_workflows scripts
.venv/bin/python scripts/run_video_enhancer.py --help
.venv/bin/python scripts/run_arrow2_latest.py --help
.venv/bin/python scripts/run_arrow2_exposure.py --help
```

轻量运行验证：

```bash
.venv/bin/python scripts/run_arrow2_exposure.py --date 2026-05-05 --skip-sync --skip-cover
.venv/bin/python scripts/run_arrow2_latest.py --date 2026-05-05 --skip-sync --skip-cover
```

VE 全链路涉及登录、LLM、Feishu、企微/Sheet，优先验证 `--help`、import、输入输出路径和可跳过外部服务的 dry-run/skip 参数；实际生产运行由用户按日期触发。
