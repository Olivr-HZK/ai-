# 社交竞品每日最新：插件下载后手动交接

当前默认collection_mode=plugin-handoff-v1、execution=manual。用户用广大大插件下载每日最新原ZIP，在聊天说明目标日期并交接文件；Agent接手核对、审核、排名、精选打包和飞书入表。没有包时等待交接，不启动浏览器采集、21项App状态机或抓取启动通知，不重建已停用定时任务。

流程：插件下载 → 用户交接日期＋原ZIP（可多包） → 核对原下载报告/实际文件/CRC/SHA256 → 明确重投剔除 → 视频六帧/图片全部原图实际审核＋风格打标 → 合格项合并按展示估值降序取前100条 → 审核看板＋精选ZIP → 幂等飞书写入并读回实际附件。

本地交接登记：

```bash
.venv/bin/python scripts/run_social_chat_browser_daily.py prepare --date YYYY-MM-DD --source-zip "/原包一.zip" --source-zip "/原包二.zip"
```

不传source-zip只准备等待交接。默认目录data/social_chat_plugin_handoff/YYYY-MM-DD；scope.json固定日期、内容/风格/排名规则，handoff.json登记包哈希，handoff-task.md说明后处理。WAITING_PACKAGE表示缺包；PACKAGE_RECEIVED只表示ZIP结构/CRC检查通过，绝不表示日期/业务字段已核验、媒体已审核或已入表。多包相同SHA256不重复登记；旧批次scope不自动迁移。

当前内容口径为男性角色展示、女频恋爱/伴侣陪伴相关、不要真人主持式产品推广口播；女性配角、手部、非主持真人恋爱剧情可保留。存疑不纳入。风格用character-style-v2组合标签，同次保存画面依据；旧风格不机械改名。视频六帧为画面初筛，不称完整审听。实际媒体不足100条时取通过数，不用存疑或缺失补足。App ID未观察到留空，报告估值仅是插件采集快照，未见重投标签是未知。

精选包命名Afizzy_竞品精选_YYYY-MM-DD_N条.zip，N用实际素材数量；内部排名_App名_广告ID.原格式，多图同排名文件夹，附CSV估值/风格/审核说明、说明MD与SHA256。已审核插件视频selection可用social_chat_selected_package；其他插件图片布局先规范化全部原图。保留原字节，CRC及包内逐媒体SHA256核验，重跑复用、变化另存版本。

目标飞书表沿config指定链接，平台广大大、来源每日最新，真实附件与业务字段读回；按独立插件批次键幂等，保护其他批次和手动记录。普通插件包不冒充21项App完整日采集，不套用旧全量日报或发送“正在抓取”@全体。需群播报时使用真实交接范围和既有明确授权，不重发。

操作细则见skills/social-chat-browser-daily/references/plugin-handoff.md和content-review.md。只有用户明确要求直接浏览器采集/恢复旧浏览器任务时才用浏览器备用流程；其CLI显式加--collection-mode direct-browser-v1。旧说明保存在social-chat-browser-daily-legacy.md，旧scope/断点/历史审核保持原规则。
