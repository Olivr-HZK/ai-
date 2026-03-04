# UA AI 产品素材 Skill（`skills/ua_analytics`）

本 skill **可独立运行**，不依赖项目根目录的 `scripts/` 或根目录 `config/`。所有依赖已内置于本目录：

- `config/ai_product.json`：产品分类与 appid 配置（已复制到本目录）
- `.env`：广大大账号与 API Key（已复制模板，请按需修改）
- `run_search_workflow.py`：Playwright 广大大搜索工作流（本目录副本）
- `guangdada_login.py`：广大大登录逻辑（本目录副本）
- `data/`、`reports/`、`ua_downloads/`：由 `path_util` 解析（项目内用根目录，单独运行时用本目录下同名目录）

## 目录结构

- `path_util.py`：统一路径；在项目内运行时用项目根下的 config/data/reports，否则用本目录下的对应目录
- `ua_crawl_db.py`：SQLite 数据库封装
  - 表 `ai_products_crawl`：每次关键词爬取结果
  - 表 `ad_creative_analysis`：按 `ad_key` 存每条广告 + 中文标题/正文 + LLM 分析
- `batch_crawl_ai_products_dated.py`：
  - 从 `config/ai_product.json` 读取分类 + 产品 + appid
  - 调用本目录 `run_search_workflow.py` 执行搜索（7天 + 素材 + 展示估值）
  - 将结果写入 `data/ai_products_ua.db` 与 `data/ai_products_ua_YYYYMMDD.json`
- `analyze_creatives_with_llm.py`：
  - 读取 `ai_products_crawl`
  - 只对「有视频」的广告按 `ad_key` 去重分析
  - 标题、正文先翻译为中文（`title_zh`、`body_zh`）
  - 调 OpenRouter：Gemini → Kimi 2.5 → Qwen2.5 VL → 回退 OpenAI
  - 输出：创意拆解 + Hook + 情感，存入 `ad_creative_analysis.llm_analysis`
- `import_json_to_db.py`：把已有的 `ai_products_ua_YYYYMMDD.json` 导入数据库

## 基本使用

**方式一：在项目根目录运行（使用项目 config/data）**

```bash
cd /Users/oliver/guru/ua素材
source .venv/bin/activate

python skills/ua_analytics/batch_crawl_ai_products_dated.py
python skills/ua_analytics/analyze_creatives_with_llm.py --date 2026-02-26
```

**方式二：仅拷贝本目录独立运行（openclaw 等）**

```bash
cd path/to/skills/ua_analytics
pip install -r requirements.txt
playwright install chromium

# 编辑 .env 填入 GUANGDADA_EMAIL、GUANGDADA_PASSWORD、OPENROUTER_API_KEY
# 按需编辑 config/ai_product.json

python batch_crawl_ai_products_dated.py
python analyze_creatives_with_llm.py --days 7
```

或对最近 7 天（按日期去重）：

```bash
python skills/ua_analytics/analyze_creatives_with_llm.py --days 7
```

只看有多少条待分析（不调大模型）：

```bash
python skills/ua_analytics/analyze_creatives_with_llm.py --days 7 --dry-run
```

## 依赖

- Python 3.10+
- 虚拟环境 `.venv` 中安装：
  - `playwright>=1.40.0`
  - `python-dotenv>=1.0.0`
  - `openai>=1.0.0`
- 环境变量：
  - `GUANGDADA_EMAIL` / `GUANGDADA_PASSWORD`
  - `OPENROUTER_API_KEY`（推荐）或 `OPENAI_API_KEY`

