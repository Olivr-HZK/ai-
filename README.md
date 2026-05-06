UA 素材 · 三工作流版

本项目现在只保留三条生产工作流：

1. **Video Enhancer**：竞品素材抓取、灵感分析、日报推送。
2. **Arrow2 latest_yesterday**：Arrow2 竞品每日最新素材。
3. **Arrow2 exposure_top10**：Arrow2 竞品展示估值素材。

核心代码位于 `ua_workflows/`，`scripts/` 只保留三个整合入口。

## 环境准备

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## 项目结构

```text
ua素材/
├── ua_workflows/
│   ├── video_enhancer/
│   ├── arrow2/
│   └── shared/
├── scripts/
│   ├── run_video_enhancer.py
│   ├── run_arrow2_latest.py
│   ├── run_arrow2_exposure.py
│   ├── test_video_enhancer_crawl.py
│   ├── test_arrow2_latest_crawl.py
│   └── test_arrow2_exposure_crawl.py
├── config/
├── data/
├── reports/
└── archive/
```

## 运行方式

在项目根目录下执行：

```bash
.venv/bin/python scripts/run_video_enhancer.py --date 2026-05-05
.venv/bin/python scripts/run_arrow2_latest.py --date 2026-05-05 --analyze
.venv/bin/python scripts/run_arrow2_exposure.py --date 2026-05-05 --analyze
```

不需要分析时可去掉 `--analyze`。Arrow2 可用 `--skip-sync` 跳过飞书同步。

## 爬取冒烟测试

三个测试脚本只验证爬取，一个产品，不入库、不同步：

```bash
.venv/bin/python scripts/test_video_enhancer_crawl.py --headless
.venv/bin/python scripts/test_arrow2_latest_crawl.py --headless
.venv/bin/python scripts/test_arrow2_exposure_crawl.py --headless
```

需要看浏览器时改用 `--headed`，需要指定产品时传 `--product`：

```bash
.venv/bin/python scripts/test_arrow2_latest_crawl.py --headed --product "Arrows - Puzzle Escape"
```

## 必要配置

- `config/ai_product.json`：Video Enhancer 竞品配置。
- `config/arrow2_competitor.json`：Arrow2 竞品与 `pull_specs` 配置。
- `config/iso3166_alpha3_zh.json`：国家代码映射。
- `.env`：广大大登录、OpenRouter/OpenAI、Feishu、企业微信、Google Sheet 等密钥。

## 验证

```bash
source .venv/bin/activate
python -m compileall ua_workflows scripts
.venv/bin/python scripts/run_video_enhancer.py --help
.venv/bin/python scripts/run_arrow2_latest.py --help
.venv/bin/python scripts/run_arrow2_exposure.py --help
```

## 归档说明

旧 UA、hot rank/new rank、playable、调试探针、一次性 backfill/preview/test 脚本已归档到 `archive/removed_scripts_2026_05_06/`。后续维护应优先修改 `ua_workflows/` 中的三条主工作流。
