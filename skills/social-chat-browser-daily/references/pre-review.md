# 先去重，再按产品一次联合审核

新scope固定`processing_policy.version=dedupe-first-joint-review-v2`、`ranking.mode=all`。四道筛选：①插件报告的明确重投标签；②日内CLIP0.9向量；③目标日前7自然日跨日向量；④按产品一次六帧主体/风格/模板联合审核。第四道结束后保留每个合格同模板组的最高估值代表，全部合格项排序、打包、入表，不截前100/前10%。旧v1及历史scope保留原规则与哈希，25号旧86条不自动重算。

## 实际入口与输入

`prepare`仍仅接收ZIP，不能把收件算作去重、审核或入表。核对原报告/重投/媒体映射后，标准化为JSON列表（或`{"records":[...]}`）。每项字段：唯一`id`、`day`目标素材日期、`kind`视频/图片、实际`app`完整名称、`os`iOS/Android、`app_id`已知才填、数值`value`展示估值、`sources`原文件path/sha256列表、`proxy`代理图path/sha256。路径为实际本地绝对路径。所有非重投已下载项都进入，不预先筛掉原`decision=exclude/review`。

视频代理为原站封面，缺封面时用3%真实帧；图片代理为第一张原图（多图限制须披露，实际审核仍看全部）。原有`frames`六项与`manifest/manifest_sha256`可携带供复用。先剔除明确重投；`relaunch=confirmed/true/重投`或插件结构化`relaunch.value=true`不会进入去重。未见重投标签仍为未知，不填假false。历史只取前7自然日的本地非重投媒体，不限已入表或审核通过；无数据日期单列，不假装覆盖完整7天。

向量缓存目录每张图为`<proxy_sha256>.json`：`image_sha256`、非空`model_fingerprint`和512维数值`vector`；全部须同一已核实本机CLIP ViT-B/32模型。复用须核对同图SHA及模型身份，缺失先用本机模型计算；不采用来源未知的裸向量，不调用OpenRouter。入口不联网计算、不获取媒体，不调用旧VE爬虫。

```bash
.venv/bin/python scripts/run_social_chat_browser_daily.py pre-review \
  --run-dir /本批目录 \
  --media-index /已核对的标准化素材与历史清单.json \
  --embedding-cache /同模型的本机向量缓存目录
```

目标日从本批scope读取；显式`--date`必须与scope相同。要求已接收原ZIP，`--apply`不适用。输出`pre-review/plan.json`：两轮逐项去重账、缺失历史日/身份、按App/系统分组的`review_queue`，状态仅`JOINT_REVIEW_PENDING`。拒绝缺目标App身份、缺估值/向量、混模型缓存或变更图像；不把这些项当零或独立新素材。

## 便携历史向量包

只迁移CLIP去重历史时，使用`social-chat-clip-vectors/v1`包：512维little-endian float32向量、代理图SHA索引、日期/App/系统/广告ID/估值和模型元数据。包内不带媒体、历史图片或机器绝对路径，也不自动合并旧VE/Arrow2库；模型指纹不同则拒绝混用。原ZIP另行保留作人工复核来源。

```bash
.venv/bin/python -m ua_workflows.social_chat_clip_vectors pack \
  --index /标准化素材索引.json --cache /向量缓存目录 \
  --date YYYY-MM-DD --output-dir data/social_chat_clip_vectors
```

缓存上一级须有生成时的model-provenance.json，向量逐个验证模型和图片哈希；包包含目标日及前7天有记录的项，文件名标明实际日期范围。新素材本机生成同模型向量后，pre-review额外传`--history-vector-pack /历史包.zip`；历史记录只读向量和身份索引，不要求历史图片/视频。它只导入目标日前7天，包中其他日期自动不参与；记录ID与本地索引冲突时拒绝身份不一致，不能静默覆盖。

读取包会校验CRC、每项SHA256、向量维度/有限值/大小及模型身份。缺日和身份不明仍按原规则列出；包不认证内容/模板/风格审核，也不替代新素材原文件与六帧证据。可执行`python -m ua_workflows.social_chat_clip_vectors inspect /历史包.zip`只读查看清单。

## 一套证据、一次人工检查

只对review_queue核验视频完整解码（源SHA匹配的既有解码回执可复用），再准备完整六帧；按产品并列查看，同一次填写`joint_review.content/template/style`及实际证据与画面理由。已有六帧先核对原媒体SHA、manifest哈希、六个采样位置与帧哈希，匹配才复用；入口输出`reuse_verified_six_frames`。没有则`extract_one_six_frame_set`，按现有frames脚本生成一套；同参数3%代理可纳入同套并补其余五帧。静态图输出`view_all_original_images`，逐原图一次审核三项。计划不认证人工查看完成，不自动填content/template/style或继承旧准入结论。

去重与旧封面逻辑同用连通聚类：A像B、B像C可能连成一组，非直接命中要保留证据，不能称确定整条视频重复。跨日命中在7天内才剔除，17号不在25号的窗口内。历史App不明会列出具体ID及缺失覆盖，目标App不明直接阻止排队。

内容或模板不确定可针对性补帧/播放，不为“一次”省略核验；仍存疑不纳入。只有内容通过项写正式风格。确认同模板的合格组只留最高估值，若没有合格项不恢复前序去重项补数。保持原素材、逐阶段理由和最终可追溯排名；第四道后所有合格项全部打包、上传并逐条读回，不截数量；旧scope仍按原规则。
