# VE 玩法资产库联动

玩法资产库的协作入口是飞书云文档：

https://www.feishu.cn/docx/HrxAdmiN6o7S4BxSNpXcT8h2n1n

同事可以直接在文档里编辑每个玩法块的 YAML 字段。项目运行时会默认尝试从这个文档拉取最新内容，写回 `config/ve_play_assets.json`；如果飞书拉取失败，会继续使用本地 JSON 兜底，不阻塞日报、同步和看板。

## 常用命令

从飞书云文档拉到本地：

```bash
.venv/bin/python scripts/sync_ve_play_assets_doc.py pull-doc
```

把本地 JSON 覆盖推送到飞书云文档：

```bash
.venv/bin/python scripts/sync_ve_play_assets_doc.py push-doc
```

把某天未命中现有资产的素材追加成「待沉淀新玩法草稿」：

```bash
.venv/bin/python scripts/sync_ve_play_assets_doc.py append-drafts --date 2026-05-13
```

只渲染文档 Markdown 预览：

```bash
.venv/bin/python scripts/sync_ve_play_assets_doc.py render --output /tmp/ve_play_assets_doc.md
```

## 编辑规则

- `asset_id` 和 `tag_id` 尽量保持稳定，避免历史归类断裂。
- 新增玩法时先设 `status: draft`，补全定义、关键词、代表素材后再改成 `active`。
- `include_keywords` 决定玩法命中，`exclude_keywords` 用来避免相近玩法误命中。
- `subtags` 是玩法变种，日报的新变种判断依赖 `tag_id` 组合。
- 代表素材只需要填 ad_key；看板和多维表会从本地库补封面和链接。
