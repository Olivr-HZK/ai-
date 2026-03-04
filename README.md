UA 素材 · 广大大爬取与日报

使用 Playwright 模拟登录并爬取广大大（guangdada.net）广告创意数据，支持批量抓取、下载素材、生成 UA 日报，并将 AI 工具 UA 素材同步到飞书多维表格。

## 环境准备

```bash
# 1. 创建并激活虚拟环境
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 2. 安装依赖
pip install -r requirements.txt

# 3. 安装 Playwright 浏览器（Chromium）
playwright install chromium
```

## 配置

- 复制 `.env.example` 为 `.env`，填入广大大账号与（可选）OpenRouter/OpenAI API Key。
- 如需同步到飞书多维表格，还需在 `.env` 中配置：`FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`BITABLE_APP_TOKEN`、`BITABLE_TABLE_ID`。
- 切勿将 `.env`、`guangdada_auth.json` 提交到 git。

## 项目结构

```
ua素材/
├── config/                 # 配置与输入
│   ├── operation.json      # 广大大页面选择器（搜索、筛选等）
│   ├── ai_product.json     # AI 产品列表（用于 AI 产品 UA 流程）
│   └── twitter_input.json  # 竞品游戏列表（用于游戏 UA 流程）
├── data/                   # 爬取与流程产出
│   ├── batch_ua_results.json
│   ├── ai_products_ua_results.json
│   ├── hot_charts_yizhi_creatives.json
│   ├── 每周益智人气榜.csv
│   └── （session_log、keyword_result 等运行时生成）
├── reports/                # 生成的日报
│   ├── ua_report_daily.md
│   └── ai_products_report_daily.md
├── ua_downloads/           # 下载的 UA 素材（图/视频）
├── scripts/                # 所有 Python 脚本
│   ├── path_util.py        # 统一路径配置
│   ├── guangdada_login.py  # 广大大登录
│   ├── scrape_guangdada.py
│   ├── run_search_workflow.py
│   ├── batch_fetch_ua.py / batch_fetch_ai_products.py
│   ├── download_ua_assets.py
│   ├── generate_ua_report.py / generate_ai_products_report.py
│   └── …
├── requirements.txt
├── .env.example
└── README.md
```

**运行方式**：在项目根目录下执行，例如：

```bash
python scripts/scrape_guangdada.py
python scripts/batch_fetch_ua.py
python scripts/generate_ua_report.py
```

## 常用流程

### 1. 广大大爬取（登录 + 筛选 + 采集）

```bash
# 自动模式（需已配置 .env 中的账号密码）
python scripts/scrape_guangdada.py

# 记录模式：手动登录与筛选，脚本记录 API 到 data/session_log.json
RECORD_MODE=1 python scripts/scrape_guangdada.py
```

### 2. 关键词搜索工作流（单关键词 → 素材结果）

```bash
# 根据 config/operation.json 执行搜索，结果写入 data/keyword_result.json
python scripts/run_search_workflow.py <关键词>
```

### 3. 批量抓取游戏 UA 素材

```bash
# 从 config/twitter_input.json 读竞品列表，批量搜索并写入 data/batch_ua_results.json
python scripts/batch_fetch_ua.py
```

### 4. 批量抓取 AI 产品 UA 素材

```bash
# 从 config/ai_product.json 读产品列表，按产品关键词搜索并写入 SQLite + 每日 JSON
python scripts/batch_crawl_ai_products_dated.py

# 或从数据库中汇总一段时间内的结果，写入 data/ai_products_ua_results.json
python scripts/batch_fetch_ai_products.py
```

### 5. 下载 UA 素材到 ua_downloads/

```bash
python scripts/download_ua_assets.py
```

### 6. 生成 UA 日报（大模型）

```bash
# 游戏 UA 日报 → reports/ua_report_daily.md
python scripts/generate_ua_report.py

# AI 产品 UA 日报 → reports/ai_products_report_daily.md
python scripts/generate_ai_products_report.py
```

### 7. 益智周榜爬取与导出

```bash
# 爬取益智周榜 → data/hot_charts_yizhi_creatives.json
python scripts/scrape_guangdada_hot_charts.py

# 导出为 CSV → data/每周益智人气榜.csv
python scripts/export_hot_charts_csv.py
```

### 8. 试玩广告素材爬虫（益智 / 7天 / 素材 → 卡片信息与试玩 URL）

```bash
# 登录 → 侧边栏试玩广告 → 分类益智 → 日期7天 → 筛选素材 → 点击卡片采集信息与试玩 URL
# 结果 → data/playable_ads_material_cards.json
python scripts/playable_ads_material_crawl.py

# 调试模式（有头浏览器）、限制采集卡片数
DEBUG=1 MAX_PLAYABLE_CARDS=20 python scripts/playable_ads_material_crawl.py
```

## 若登录选择器不匹配

广大大若改版，可用 Playwright 生成选择器：

```bash
playwright codegen https://www.guangdada.net/user/login
```

将生成的选择器同步到 `scripts/guangdada_login.py` 或 `scripts/scrape_guangdada.py` 中。

## AI 工具 UA 每日自动工作流

AI 工具 UA 的完整链路包括：爬取 → 入库 → LLM 分析 → 同步飞书多维表格，由 `scripts/daily_ua_job.sh` 串联。

### 1. 手动执行一次每日工作流

```bash
cd /Users/oliver/guru/ua素材
source .venv/bin/activate

bash scripts/daily_ua_job.sh
```

脚本内部包含三个步骤，日志输出在 `logs/weekly_ua_job.log` 中：

1. **步骤 1/3：批量爬取 AI 工具 UA 素材**
   - 入口：`scripts/batch_crawl_ai_products_dated.py`
   - 配置：`config/ai_product.json`
   - 产出：
     - 数据库：`data/ai_products_ua.db` 表 `ai_products_crawl`
     - JSON：`data/ai_products_ua_YYYYMMDD.json`

2. **步骤 2/3：广告创意分析（翻译 + 拆解）**
   - 入口：`scripts/analyze_creatives_with_llm.py --date <当天日期>`
   - 数据来源：表 `ai_products_crawl` 中当天的记录
   - 产出：表 `ad_creative_analysis`，写入标题/正文中文翻译与 LLM 分析结果。

3. **步骤 3/3：同步最新广告创意到飞书多维表格**
   - 入口：`scripts/daily_sync_latest_creative_to_bitable.py`
   - 数据来源：`ad_creative_analysis` 中最新 `crawl_date` 的记录
   - 产出：将最新一批创意写入指定的飞书多维表格。

### 2. 配置 crontab 每日自动运行

在服务器上执行：

```bash
crontab -e
```

增加一行（每天 10:30 运行一次完整工作流）：

```bash
30 10 * * * /bin/bash /Users/oliver/guru/ua素材/scripts/daily_ua_job.sh
```

随后可通过：

```bash
tail -f logs/weekly_ua_job.log
```

实时查看每日任务执行进度。
