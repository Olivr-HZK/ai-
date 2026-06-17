# VE 模板复刻点击触发部署

本文记录 `3星及以上素材 -> 多维表按钮/链接点击 -> 本地任务落盘 -> 本地 Codex 调用 aigc-template-copy skill -> 结构化结果回写` 的部署方式。

## 当前临时部署

当前临时入口由两部分组成：

- 本机触发服务：`127.0.0.1:8765`
- Cloudflare Quick Tunnel：临时 `trycloudflare.com` URL，指向本机触发服务

启动本机服务：

```bash
cd /Users/oliver/guru/ua素材
mkdir -p data
test -s data/ve_template_trigger_token.local || .venv/bin/python - <<'PY' > data/ve_template_trigger_token.local
import secrets
print(secrets.token_urlsafe(24))
PY
chmod 600 data/ve_template_trigger_token.local

VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" \
  .venv/bin/python scripts/run_ve_template_trigger_server.py \
  --host 127.0.0.1 \
  --port 8765 \
  --auto-prepare \
  --auto-execute-codex \
  --codex-model gpt-5.5
```

`--auto-execute-codex` 会在 job 准备好后继续调用 `codex exec`，由本地 Codex 使用 `$aigc-template-copy` 处理。测试或只想落盘时可以去掉这个参数；这样只会生成 prompt/参数文件，不会调用 Codex，也不会创建 Video Lab 任务。

如果需要临时常驻，使用 detached screen：

```bash
cd /Users/oliver/guru/ua素材
screen -dmS ve-template-trigger zsh -lc 'cd /Users/oliver/guru/ua素材 && env VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" VE_TEMPLATE_COPY_CODEX_MODEL="gpt-5.5" .venv/bin/python scripts/run_ve_template_trigger_server.py --host 127.0.0.1 --port 8765 --auto-prepare --auto-execute-codex --codex-model gpt-5.5 --model-ref-dir /Users/oliver/guru/ua素材/data/template_model_refs >> data/ve_template_trigger_server.log 2>&1'
```

查看/停止：

```bash
screen -ls
screen -S ve-template-trigger -X quit
```

另开一个终端启动临时 tunnel：

```bash
cd /Users/oliver/guru/ua素材
cloudflared tunnel \
  --url http://127.0.0.1:8765 \
  --no-autoupdate \
  --logfile data/ve_template_trigger_cloudflared.log
```

从 cloudflared 输出中取 `https://xxx.trycloudflare.com`，保存并更新 Base Workflow：

```bash
cd /Users/oliver/guru/ua素材
printf '%s\n' 'https://xxx.trycloudflare.com' > data/ve_template_trigger_public_url.txt

VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" \
  .venv/bin/python scripts/upsert_ve_template_button_workflow.py \
  --trigger-url "$(cat data/ve_template_trigger_public_url.txt)"
```

验证：

```bash
curl -sS http://127.0.0.1:8765/healthz
curl -sS "$(cat data/ve_template_trigger_public_url.txt)/healthz"
```

如果 Quick Tunnel URL 变化，只需要重新保存新 URL，再跑一次 `upsert_ve_template_button_workflow.py`。脚本会查找标题为 `VE 模板复刻按钮触发` 的 Workflow；存在则更新，不存在则创建并启用。

`--auto-prepare` 只会把 job 继续准备到本地 worker 目录：下载源素材、选择 `data/template_model_refs/` 下的默认模特图、生成 Codex prompt 和 Video Lab 参数 JSON；不会直接创建 Video Lab 生成任务，也不会消耗额度。

`--auto-execute-codex` 会继续执行本地 Codex。Codex prompt 会要求 skill 最后一行输出：

当前临时服务固定使用 `--codex-model gpt-5.5`，避免默认模型容量满导致 `Selected model is at capacity`。`gpt5.5` 是人工口语写法，代码会归一为 `gpt-5.5`，但部署命令仍建议写正式模型名。

```text
VE_TEMPLATE_COPY_RESULT_JSON: {"status":"completed|quality_failed|blocked","quality_verdict":"passed|failed","score":90,"mode":"video_template|image_copy","task_ids":[],"output_paths":[],"reason":"..."}
```

触发服务会解析这个 JSON：

- `status=completed` 且 `quality_verdict=passed`：多维表 `模板复刻状态=已完成`
- `status=quality_failed` / `blocked` / `failed`：多维表 `模板复刻状态=失败`
- Codex 运行失败或缺少结构化结果：多维表 `模板复刻状态=失败`

## 多维表入口

当前主表已具备这些字段：

- `模板复刻状态`
- `模板复刻任务ID`
- `模板复刻触发链接`

Base Workflow 采用 `ButtonTrigger -> HTTPClientAction`：

- 按钮触发器绑定主表 `ai工具video photo爬取表`
- HTTP action POST 到 `${PUBLIC_URL}/trigger`
- 请求体带当前行 `record_id` 和本地 token

评分后自动写链接采用 `SetRecordTrigger -> HTTPClientAction`：

- 触发器监听 `浩鹏评分` 或 `尉蓝评分`
- 条件为评分 `>= 3`
- HTTP action POST 到 `${PUBLIC_URL}/ensure-link`
- `/ensure-link` 只写当前记录的 `模板复刻触发链接`，不会创建 job、不会调用 Codex、不会消耗 Video Lab

安装或更新评分链接 Workflow：

```bash
cd /Users/oliver/guru/ua素材
.venv/bin/python scripts/upsert_ve_template_rating_link_workflow.py \
  --public-url "$(cat data/ve_template_trigger_public_url.txt)" \
  --table-name "ai工具video photo爬取表" \
  --rating-field-name "浩鹏评分" \
  --reviewer haopeng \
  --token "$(cat data/ve_template_trigger_token.local)"

.venv/bin/python scripts/upsert_ve_template_rating_link_workflow.py \
  --public-url "$(cat data/ve_template_trigger_public_url.txt)" \
  --table-name "ai工具video photo爬取表" \
  --rating-field-name "尉蓝评分" \
  --reviewer weilan \
  --token "$(cat data/ve_template_trigger_token.local)"
```

也可以写普通链接字段作为批量补入口：

```bash
cd /Users/oliver/guru/ua素材
VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" \
  .venv/bin/python scripts/run_ve_template_trigger_links.py \
  --date YYYY-MM-DD \
  --trigger-url "$(cat data/ve_template_trigger_public_url.txt)/trigger" \
  --token "$(cat data/ve_template_trigger_token.local)" \
  --include-all-records \
  --link-only
```

`--include-all-records --link-only` 只负责给目标日期所有记录预写入口，不改 `模板复刻状态` / `模板复刻任务ID`。点击时触发服务仍会重新读取记录并校验评分，只有 3 星及以上才会创建模板复刻 job。

## 本地 Worker

点击触发后会落盘 `data/ve_template_copy_jobs/{job_id}.json`。可以手动消费单个 job：

```bash
cd /Users/oliver/guru/ua素材
.venv/bin/python scripts/run_ve_template_copy_worker.py \
  --job data/ve_template_copy_jobs/JOB_ID.json
```

默认行为是 dry-run 准备，不调用 Codex、不创建 Video Lab 任务。产物会落在：

- `data/ve_template_copy_runs/{job_id}/codex_prompt.txt`
- `data/ve_template_copy_runs/{job_id}/params.image-editing.json`
- `data/ve_template_copy_runs/{job_id}/params.image-to-video.json`

需要实际拉起本地 Codex 时再显式加：

```bash
.venv/bin/python scripts/run_ve_template_copy_worker.py \
  --job data/ve_template_copy_jobs/JOB_ID.json \
  --execute-codex \
  --codex-model gpt-5.5
```

`--execute-codex` 会调用 `codex exec` 读取 `codex_prompt.txt`，让本地 Codex 使用 `$aigc-template-copy` 继续处理。worker 会额外写入：

- `data/ve_template_copy_runs/{job_id}/codex_last_message.md`
- `data/ve_template_copy_runs/{job_id}/skill_result.json`

如果需要做无消耗集成测试，可以用 `--codex-bin /path/to/fake-codex` 或环境变量 `VE_TEMPLATE_COPY_CODEX_BIN` 指向假执行器，让它只输出 `VE_TEMPLATE_COPY_RESULT_JSON`。

## Mac mini 常驻部署

后续迁到 Mac mini 时，建议在远端对应 `ua素材` checkout 中部署，保持和日常 VE `.env` 一致。

远端准备：

```bash
ssh ggbond@10.125.46.30
cd /Users/ggbond/oliver/ai-
git pull
test -s data/ve_template_trigger_token.local || .venv/bin/python - <<'PY' > data/ve_template_trigger_token.local
import secrets
print(secrets.token_urlsafe(24))
PY
chmod 600 data/ve_template_trigger_token.local
```

临时常驻可先用 `tmux` 或 `screen` 跑两条进程：

```bash
VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" \
  .venv/bin/python scripts/run_ve_template_trigger_server.py \
  --host 127.0.0.1 \
  --port 8765 \
  --auto-prepare \
  --auto-execute-codex
```

```bash
mkdir -p data/runtime
: > data/runtime/cloudflared-empty.yml

cloudflared \
  --config "$PWD/data/runtime/cloudflared-empty.yml" \
  tunnel \
  --url http://127.0.0.1:8765 \
  --protocol http2 \
  --no-autoupdate \
  --loglevel info \
  > data/ve_template_trigger_cloudflared.log 2>&1
```

Mac mini 上如果 `~/.cloudflared/config.yml` 已经配置了别的 named tunnel，Quick Tunnel 也可能自动读到默认配置，导致公网 URL 命中默认 `http_status:404` 而不是本地 `8765` 服务。上面的空 config 是为了隔离 VE 临时 tunnel；启动后必须验证公网健康检查：

```bash
PUBLIC_URL="$(grep -Eo 'https://[-a-zA-Z0-9]+\.trycloudflare\.com' data/ve_template_trigger_cloudflared.log | tail -n 1)"
printf '%s\n' "$PUBLIC_URL" > data/ve_template_trigger_public_url.txt
curl -fsS "$PUBLIC_URL/healthz"
```

拿到新 URL 后，在远端或本机任一有 `lark-cli` 权限的环境运行：

```bash
VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" \
  .venv/bin/python scripts/upsert_ve_template_rating_link_workflow.py \
  --bitable-url 'https://scnmrtumk0zm.feishu.cn/base/CivwbJ2HkazcKTsKnbGclA5RnWc?table=tblrZZvVuFcjL0kE&view=vewGH7cmSs' \
  --public-url "$PUBLIC_URL" \
  --table-name 'ai工具video photo爬取表' \
  --rating-field-name '浩鹏评分' \
  --reviewer haopeng

VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" \
  .venv/bin/python scripts/upsert_ve_template_rating_link_workflow.py \
  --bitable-url 'https://scnmrtumk0zm.feishu.cn/base/CivwbJ2HkazcKTsKnbGclA5RnWc?table=tblrZZvVuFcjL0kE&view=vewGH7cmSs' \
  --public-url "$PUBLIC_URL" \
  --table-name 'ai工具video photo爬取表' \
  --rating-field-name '尉蓝评分' \
  --reviewer weilan

VE_TEMPLATE_TRIGGER_TOKEN="$(cat data/ve_template_trigger_token.local)" \
  .venv/bin/python scripts/upsert_ve_template_button_workflow.py \
  --bitable-url 'https://scnmrtumk0zm.feishu.cn/base/CivwbJ2HkazcKTsKnbGclA5RnWc?table=tblrZZvVuFcjL0kE&view=vewGH7cmSs' \
  --trigger-url "$PUBLIC_URL/trigger" \
  --table-name 'ai工具video photo爬取表'
```

生产稳定版建议改成 Cloudflare named tunnel 或固定域名，这样 Workflow URL 不会因为 Quick Tunnel 重启而变化。

## 运行产物

- Job：`data/ve_template_copy_jobs/{job_id}.json`
- Token：`data/ve_template_trigger_token.local`
- 当前公网 URL：`data/ve_template_trigger_public_url.txt`
- Cloudflare 日志：`data/ve_template_trigger_cloudflared.log`

这些都属于运行产物，不应提交到 git。
