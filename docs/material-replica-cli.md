# 素材复刻 CLI

2026-11-04按用户要求新增统一入口`scripts/material-replica`。命令按输入类型分流，默认一次只生成一份结果，不改变现有主表监听或启动历史批次。

| 输入 | 执行链路 |
|---|---|
| PNG、JPEG、静态WebP | Video Lab CLI → `preset_image_editing` → `model-api-router / gpt-image-2` → 原图尺寸结果下载 |
| MP4、MOV、WebM、MKV | 现有Replica SDK → 上传自动分析 → 智能换Logo → 下载完整解码 |

图片使用项目已认可的`skills/social-chat-browser-daily/assets/afizzy-approved-brand.png`作第二张参考图。2026-11-05用户纠正：完整替换竞品品牌，应用图标、相邻品牌名称和字标、正文/CTA中的品牌词都改为Afizzy，不得只换图标而留下Tipsy等竞品名称。原位置、大小、透视和排版保持，人物、画风、背景、非品牌文字及原语言保持。无可识别竞品品牌时不加新标识。参考图仅取AF图标和afizzy字标，不移植背景及人物。此前“只换图标、保留竞品文字”是理解错误，已修正默认提示词；旧断点不自动重跑，用户指出的旧错误结果另行定向纠正。

先本地查看分流和生成参数，不登录、不上传、不生成：

```sh
./scripts/material-replica plan /绝对路径/原图.png /绝对路径/原视频.mp4
```

正式运行会产生生成费用。一个运行目录对应一份可恢复断点；图片、视频可以混选，所有项目独立推进，慢任务不会要求重新提交已经接受的项目。默认等待到下载完成、明确失败或两小时超时：

```sh
./scripts/material-replica run /绝对路径/原图.png /绝对路径/原视频.mp4 \
  --run-dir data/material-replica/本批次

./scripts/material-replica status --run-dir data/material-replica/本批次
./scripts/material-replica resume --run-dir data/material-replica/本批次
```

`--no-wait`只推进一轮；后续需用`resume`继续查询和下载。可以用`--timeout 3600 --interval 15`调整本次等待时间和只读轮询间隔。下载默认在运行目录`results/`；首次`run`可通过`--output-dir`指定其他目录。`--prompt-file`可提供UTF-8图片编辑提示词，`--brand-reference`可替换品牌参考图；这些参数与源SHA256冻结在断点中，续跑不能静默改为新请求。视频仍仅智能Logo，不自动增加翻译、人物图、换人或拼接。

Video Lab由其CLI正常管理OAuth登录和刷新，包装器不读取或导出令牌。默认使用`default`profile，可在首次运行加`--videolab-profile`。每次续跑核对Video Lab账号、profile、服务地址和项目，视频核对Replica账号；账号或项目变更会阻止对应分支，不自动切换项目。两个后端的故障隔离，不影响另一个已认证分支推进。

图片请求固定`provider=model-api-router`、`model=gpt-image-2`、`size=__original_image__`、`task_config.execution_count=1`。没有模型自动回退。当前真实契约将该模型的`quality`设为隐藏字段，因此不提交quality，沿平台默认。运行时由官方CLI自带的服务端validate检查模型和参数，不让可能超时的工作流发现/契约查询阻挡正常提交。CLI提交前将原图及参考图按SHA256冻结为本批私有输入，防止原文件在上传期间变化；每份任务独立UUID，意图先fsync，之后才调用`task create --submission-request-id`。

生成回执未知或进程在派发边界退出，只读`task recent`及任务列表中精确匹配原UUID的记录，不再调用create；本地或远端最近100项中无匹配仍保留未知，不推断未创建。明确失败不自动重做。下载失败可以续跑原任务的读取，不重新生成；已有未核验目标文件不覆盖。图片下载校验CLI字节回执、完整图片解码、原尺寸和本地SHA256；视频沿现有大小/SHA256及完整解码核验。

只读身份、项目及工作流/任务查询发生临时超时时最多即时重试一次，后续保留断点退避；不重试任务create或项目选择写入。已核验本地结果缺失或哈希改变时停止复用，不重新生成。

`downloaded`表示技术下载核验通过。图片保留`visual_review_required=true`，还须实际查看品牌拼写、残留竞品、人物和文字后才可认定效果合格。这个独立CLI不写飞书；主表监听器复用此模块的图片执行和断点恢复能力，另行负责同一行附件交付，详见`docs/replica-main-table.md`。Playable、动画GIF/WebP不进入此入口。

退出码：0表示本次计划/提交/查询正常；2表示有明确失败或需复查项目；3表示后端不可用、结果未知或等待尚未完成。JSON里的`all_outputs_downloaded`只在全部结果均已下载核验时为true。

本次安装先卸载6.1.3，再用用户提供的官方安装入口安装6.2.0并完成登录。真实工作流契约和不创建任务的`task create --dry-run --full`已验证，`valid=true`、`external_writes=0`。安装与参数预检阶段没有创建付费任务。随后已按用户指定完成一条Tipsy图片实测及品牌修正，成品经品牌残留检查、原尺寸核验并回填原记录；单样本不代表全部图片效果均已验收。离线测试覆盖混合素材、丢回执、崩溃、账号/源变更及结果完整性。
