# 社交聊天竞品：浏览器日采集 → 全局前10% → 飞书

当前为手动执行，不启用定时。对 Codex 说“执行昨天的竞品日采集”，或使用 `$social-chat-browser-daily`。

目标表：[社交聊天竞品｜每日新增视频素材 Top10%](https://scnmrtumk0zm.feishu.cn/base/Adz4bj9ECaCCJrsgkj7coheYn3c?table=tblwhAqPfQoBXK3Q)。当前表为22个业务字段及视频附件；主列appid，广告主/平台单选，渠道/地区/文案语言多选，移除全局排名与两项计数列；初始化不插入示例素材。

口径由 `config/social_chat_browser_daily.json` 固定：

- 按 Asia/Shanghai 昨天首次发现；逐卡核对详情日期：明确“首次发现”字段优先，否则采用详情日期范围起点（用户已确认其含义）。保留原文和日期来源，不凭列表筛选或旧 first_seen 推断。
- 日期设置后先选最新创意；按已核实App ID搜索、选中广告主后，每一次都重新点击最新创意并保存新日志。采集期间不按展示估值排序或阈值截断。
- 完整名单21个任务（iOS10、Android11），ID映射见 `config/social_chat_browser_app_ids.json`，独立选 App、点卡、翻页及封账。
- 工具、素材、视频；渠道、地区、文案语言、尺寸不限；只排除真人口播。
- 全部任务完成采集和六帧审核后，全局合并取展示估值前10%，数量向上取整。比如101个有效候选取11个；0个取0个。并列按完整App名/平台/App ID/广告ID稳定排序，不扩张入选数。
- 分母是目标日、非真人口播且估值有效的全部候选；目标日审核不确定或估值缺失会阻塞排名，不假装为零或静默丢弃。
- 不运行旧 VE 的封面/历史去重、业务排除、每广告主Top10或差异度精选。

在项目目录执行：

```bash
.venv/bin/python scripts/run_social_chat_browser_daily.py prepare
```

这条命令**只准备任务**。读取返回目录中的 `browser-task.txt`，再由 agent 使用 `guangdada-browser-crawl` 执行真实浏览器采集。默认输出 `data/social_chat_browser_daily_latest/YYYY-MM-DD/`；用 `--date YYYY-MM-DD` 指定历史日。断点同目录恢复，不能用不同条件复用证据。

全量采集与审核后：

```bash
.venv/bin/python scripts/run_social_chat_browser_daily.py select --run-dir <任务目录>
.venv/bin/python scripts/run_social_chat_browser_daily.py sync --run-dir <任务目录>
.venv/bin/python scripts/run_social_chat_browser_daily.py sync --run-dir <任务目录> --apply
```

不带 `--apply` 只生成本地待写入包。实际写入每次重新验证证据、读取现有唯一键，创建或更新后逐字段读回；新入选确认成功才将同日旧落选记录改为“当前入选=false”。查看当前榜单请按目标日期筛选并勾选“当前入选”。历史记录不删除。

`provision --apply` 可幂等补充字段并验证字段类型和读取权限，不插入测试素材。凭据从项目 `.env` 读取；缺少权限时保留本地结果，报告飞书错误码，不切换旧业务表。

边界：只覆盖当前广大大账号可访问列表；App找不到不等于零素材。验证码需人工处理。六帧只能完成画面初筛，不能保证识别全部口播；不确定需进一步观看/审听。没有完成真实首批采集前，不把准备成功或模拟测试当作采集成功。


## 每日浏览器采集防睡眠与异常日志（2026-10-21）

项目内的 `ua_workflows.browser_collection_runtime` 只管理本地电源保护/日志，不访问广大大、飞书或付费接口。采集前执行 `.venv/bin/python -m ua_workflows.browser_collection_runtime start --run-dir <本批目录>`，必须返回 `active=true` 后才开始浏览器操作。每页操作前及至少每5分钟执行同命令的 `renew` 并核对 active；`status` 可只读检查，非活跃返回非零退出码。15分钟无续期自动释放，不能把过期视为仍受保护；继续前重新 start。完成、异常停止、用户暂停或验证码/人工等待时执行 `stop`，确认终止状态；恢复后重新 start。不使用无限期后台 caffeinate，prepare 的短暂进程保护不覆盖浏览器步骤间隔。

浏览器工具异常时，将真实 JSON（stage、app_id、page、card、error_type、message、已有 traceback、evidence_paths）通过 stdin 交给 `.venv/bin/python -m ua_workflows.browser_collection_runtime error --run-dir <本批目录>`。仅保存实际错误；工具不提供堆栈时省略 traceback，不推测接口返回或伪造调用栈。日志落在本批 runtime/，凭据和签名 URL 脱敏，权限600。浏览器步骤的日志依赖执行者记录；Python准备/校验/入表/通知命令由 collection_phase 自动记录异常并释放防睡眠保护。下载/抽帧工具失败同样记录实际工具错误与日志路径。

保护允许锁屏/息屏；不承诺唤醒尚未开始任务的睡眠电脑，也不覆盖手动强制睡眠、合盖或关机。此功能不新增收费请求、不改重试/筛选/群消息规则，不修改历史批次。

## 2026-10-23 后续真人出镜规则

新批scope固定content_review_policy版本no-real-person-v1。视频和图片出现任何真人即剔除，包括非口播、剧情/生活/影视、照片、局部手部及混剪中的短暂真人。明显AI/动漫/CG角色保留按画面审核；无法确认则review，不参与排名。reviews记录review_policy=no-real-person-v1及checks.real_person_presence=present/absent/uncertain，旧口播false不能认证新规则。图片也可因真人出镜exclude，仍须原图/多图全检；视频仍实际查看六帧，存疑须进一步复核。

用户本次只授权更新风格试分看板和后续规则；旧scope、审核、排名、入表记录、日报不追溯改写。新prepare固定新版本，恢复旧目录保留原scope及规则，不以更新当前config篡改旧证据。“真人实拍”不再作为保留素材风格选项。

## 2026-10-23 后续统一男性角色主体规则

最新版本为male-character-no-real-person-v1：视频、图片均仅保留未见真人且男性角色为明确主体；任何真人或女性主体exclude，男女混合/无明确男性主体/真人AI不确定review，不进入风格样本、排名或飞书。明显AI/动漫/CG男性不因写实外观自动排除。新review记录real_person_presence与primary_subject，旧无口播或无真人审核不自动通过。内容证据、六帧/原图哈希绑定不变；六帧初筛存疑须补看。规则详见skills/social-chat-browser-daily/references/content-review.md。

旧scope和既有完成批次仍保留原版本，恢复不改原scope/hash；后续新批默认新版。未自动重审历史或改表/发群，调度保持现有暂停/启用状态。
