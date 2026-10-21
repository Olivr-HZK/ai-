# 状态机记录约定

## 任务与目录

`state.py` 只检查本地证据，不控制浏览器。先读 [browser-check.md](browser-check.md) 执行真实点击并产生日志；所有截图/视频/抽帧都应在任务内，便于移交。`HASH` 在下文示例中代表 init 返回的 scope_sha256，必须替换成实际值。

```text
run/
  scope.json                     init 复制的任务配置
  run.json                       固定指纹，不能手工更新绕过变更
  browser.json                   当前已登录浏览器证据（路径相对 run）
  state.json                     check 自动计算的结果
  app_checklist.json             每 App 采集清单及下一 App
  tasks/<task-id>/
    filters.json                  每项筛选的实际值 + 点击检查引用
    app.json                      App 身份、实际卡片总数 + 点击检查
    sort.json                     选择 App 之后的展示估值/热度排序检查
    collection_complete.json      seal 生成的本 App 采集完成清单
    probe.json                    首卡测试，引用真实卡片文件
    cards/0001-0001.json           逐页逐位置详情 + 点击检查
    pages/0001.json                逐页清单、翻页日志和末页依据
    frames.json                   全部原始视频的抽帧 manifest 索引
    reviews.json                  已看截图后的决定与理由
    downloads.json                保留视频的校验清单
    delivery.json                 可移交文件清单和预览验证
    sources/                      用于真实抽帧的原始视频
    evidence/clicks/              脚本生成的前后 PNG/JPEG 截图与事件 JSON
    evidence/frames/<ad-id>/       frames.py 生成的六张 PNG 和 manifest
    delivery/                     保留视频、清单、相对路径预览
    hold.json / resumes.json      暂停与恢复审计
```

所有普通记录携带 scope_sha256。除 browser.json 外，文件内路径相对 task 目录；不允许引用任务外文件。初始化：

```bash
python3 scripts/state.py init --config scope.json --run-dir run
python3 scripts/state.py check --run-dir run
```

## scope：用户要求与支持边界

从 [scope.example.json](scope.example.json) 修改；其中具体产品、日期、内容条件都只是示例。必填 filters：

- category：游戏 / 工具。
- dedupe：广告 / 素材。
- channels、regions：列表；[] 表示不限。不限时只读检查当前空选/全部状态；仅有残留时清除。
- date：以下二选一，不能混用。

```json
{"mode":"preset","preset":"30天"}
```

preset 支持 7天、30天、90天、1年；“近1年”UI 标签归一为“1年”，原截图保留实际文案。

```json
{"mode":"custom","start":"2026-09-01","end":"2026-09-30"}
```

自定义日期必须是有效 YYYY-MM-DD 且开始不晚于结束。sort 必填，支持展示估值/热度；额外可有 media_type、languages、aspect_ratio。全部已配置字段都要求逐项脚本 check，但排序要在 App 确认后单独执行。date_semantics 置于 scope 顶层，不当作可点击筛选；scope.date_anchor 固定查询日和时区。滚动窗口跨天不可无声变化。

tasks 每项必填 id、app_name、platform（iOS/Android），app_id 已知时填写。只给简称或单端名时，不假定其他端对应关系。content_checks 为可选布尔要求，未知条件不能用 false 代替。

## 浏览器、筛选与 App

browser.json（evidence 相对 run）：

```json
{"scope_sha256":"HASH","authenticated":true,"browser_session":"实际会话标识","evidence":["evidence/browser.txt"]}
```

filters.json：

```json
{
  "scope_sha256":"HASH",
  "selected":{"category":"工具","dedupe":"素材","date":{"mode":"preset","preset":"30天"},"channels":[],"regions":[]},
  "ui_checks":{
    "category":"evidence/clicks/category-001.json",
    "dedupe":"evidence/clicks/dedupe-001.json",
    "date":"evidence/clicks/date-001.json",
    "channels":"evidence/clicks/channels-001.json",
    "regions":"evidence/clicks/regions-001.json"
  },
  "evidence":["evidence/filters.txt"]
}
```

filters.json 的 selected 为实际观察的基础筛选值，必须与 scope.filters **去掉 sort 后**完全相同，ui_checks 也不放 sort。示例省略的其他可选字段如被配置仍须有值和检查。每条引用须为 browser_check.js 产生的成功日志，key=filter.<字段名>，after=该筛选实际值，前后 PNG/JPEG 图片齐全且哈希一致。纯截图、DOM文本或手填 selected=true 无法过关。

app.json：

```json
{"scope_sha256":"HASH","query_full_name":"Tipsy Chat: Live Your Story","app_name":"Tipsy Chat: Live Your Story","platform":"iOS","app_id":"6572296119","selected_app_ids":["6572296119"],"residual_keywords":[],"count_basis":"素材","card_count":1,"search_mode":"广告主","query_ui_check":"evidence/clicks/query-001.json","ui_check":"evidence/clicks/candidate-001.json","evidence":["evidence/app.txt"]}
```

先选择搜索类型“广告主”，输入完整名或已核实App ID；query_ui_check引用key=app.query的真实搜索日志。随后必须点击下方正确广告主候选，ui_check引用key=app.candidate的点击日志，并由gddCheckedAppCandidate调用候选locator.click()产生interaction={action:click,target:advertiser_candidate}。候选点击时间不得早于本次搜索；仅按回车、只提交关键词或点击搜索按钮不能替代候选选择。候选after对象必须恰好包含app_name、platform、app_id、selected_app_ids、residual_keywords、search_mode六项，search_mode为“广告主”，已选App唯一且无残留关键词。card_count 为当前列表模式对应的卡片数；count_basis 必须与 scope.filters.dedupe 相同。广告模式不能误用素材去重后的数，素材模式也不能用原始广告总数。

正确选中 App 后明确零结果：card_count=0、empty_result_verified=true，不需要 probe；仍需点击和截图 check。找不到正确候选时保持 APP_PENDING，不能构造虚假 app_id 或零结果。

## 独立排序阶段与操作顺序

登录后先 category，再其他基础筛选，再 App，最后排序。脚本按点击时间校验顺序；App 在基础筛选前点击停在 APP_PENDING，排序在 App 前点击停在 SORT_PENDING，卡片早于排序停在 PROBE_PENDING/COLLECTING。

sort.json（App 已选中之后生成）：

```json
{"scope_sha256":"HASH","selected":"展示估值","ui_check":"evidence/clicks/sort-after-app-001.json","evidence":["evidence/sort.txt"]}
```

排序日志 key=filter.sort，after 为实际选中排序值。不得把进入 App 前继承的旧排序或仅仅默认显示当作本次点击证据。

## 单卡、全量与翻页

probe.json：

```json
{"scope_sha256":"HASH","app_name":"Tipsy Chat: Live Your Story","platform":"iOS","app_id":"6572296119","detail_loaded":true,"card_file":"cards/0001-0001.json","evidence":["evidence/probe.txt"]}
```

cards/0001-0001.json：

```json
{"scope_sha256":"HASH","page":1,"position":1,"detail_loaded":true,"ad_id":"真实ID","app_name":"Tipsy Chat: Live Your Story","platform":"iOS","app_id":"6572296119","detail_url":"页面实际详情链接","video_url":"已观察的媒体地址","channel":"FB News Feed","regions":["日本","美国"],"language":"英语","dimensions":"720 x 1280","first_seen":"实际值","last_seen":"实际值","impressions_estimate":"实际值","raw_text":"详情原文","missing_fields":[],"ui_check":"evidence/clicks/card-0001-0001.json","evidence":["evidence/detail-0001-0001.txt"]}
```

点卡日志 key=card.open，after={ad_id,app_id,platform} 必须来自当前详情，不能读卡面或上一张缓存。媒体不可获取可写 media_unavailable_reason，但该视频后续抽帧关卡仍未完成；最终只能报告部分完成，不假填 frames。

pages/0001.json：

```json
{"scope_sha256":"HASH","page":1,"visible_count":1,"cards":["cards/0001-0001.json"],"has_next":false,"terminal_reason":"next_disabled","evidence":["evidence/page-0001.txt"]}
```

第2页起还需 ui_check 引用翻页日志，key=page.open、after={"page":实际页码}。页码从1连续，页内位置从1连续，实际卡片数与当前模式 UI 总数一致，ID 不重复，测试卡也在全量盘点中。中间页 has_next=true；末页必须有 next_disabled 或正确 App 下的 explicit_no_results。零结果页为第1页、0张、空 cards。

check 返回 checkpoint.next，为首个未封账页中首个未落盘位置；末页记录后可为 null，但不代表其他关卡通过。不得仅凭已点过或尝试失败推进断点。

## 本 App 完成清单与下一个 App

末页、逐卡和数量检查通过后，状态先停在 COLLECTION_SUMMARY_PENDING；执行：

```bash
python3 scripts/state.py seal --run-dir run --task tipsy-ios
```

seal 再次检查盘点，生成 collection_complete.json，包含 producer、scope 指纹、完成时间、完整 App 名、平台、App ID、广告/素材口径、排序、页数、卡片数、广告 ID 清单及末页证据。未通过卡片/分页检查或仍在 WAITING_HUMAN 时拒绝 seal。

每次 check 自动写 app_checklist.json，列出所有 App 的 collection_status、采集统计和当前状态；state.json 的 next_collection_task 指明下一 App。按 scope.tasks 的顺序串行采集：前一 App 未 seal，后续 App 为 PREVIOUS_APP_PENDING；下一 App 的分类检查记录（只读或实际修改）必须晚于前一 App 清单完成时间；其余基础筛选逐项重新只读核对，已正确不重复操作。App选择和排序仍须真实点击。

采集登记后即可进入下一 App，不必等本 App 的后续内容审核。collection_complete=true 仅表示所有 App 原始采集已完成并登记；最终 state=COMPLETED 仍要求抽帧、审核、媒体、交付均通过。不得把 App 采集完成清单当作视频审核完成声明。

## 强制真实抽帧

全量采集后将原始视频放进当前 task 的 sources，再执行：

```bash
python3 scripts/frames.py --video run/tasks/tipsy-ios/sources/AD_ID.mp4 --task-root run/tasks/tipsy-ios --output-dir run/tasks/tipsy-ios/evidence/frames/AD_ID --scope-sha256 HASH --ad-id AD_ID
```

要求 Python 3.10+、FFmpeg 与 ffprobe。脚本实际读取视频参数、在 3%/15%/30%/50%/70%/90% 各解码一张 PNG，写入 manifest.json，记录源视频哈希、时长、尺寸、解码结果及每张图路径/哈希/尺寸/请求时间点。FFmpeg 以请求时间附近取帧，不宣称精确原始 PTS；不代表已审听音轨。

不要手写或修改该 manifest。已有输出目录拒绝覆盖，重试用新目录并保留失败原因。将每条广告输出纳入 frames.json：

```json
{"scope_sha256":"HASH","items":[{"ad_id":"真实ID","manifest":"evidence/frames/AD_ID/manifest.json"}]}
```

全部原始卡片必须有对应抽帧清单。源视频哈希不符、六张不齐、图片损坏/尺寸不符、时间点不在视频内均停在 FRAMES_PENDING。零卡片则 items=[]。

## 查看截图、筛选及下载

agent 必须实际打开并查看脚本生成的六张图，可另生成拼图方便看，但不能用占位图或其他素材图替代。reviews.json：

```json
{"scope_sha256":"HASH","items":[{"ad_id":"真实ID","decision":"keep","reason":"实际观察理由","method":"sampled_frames","frames_inspected":true,"sampling":{"fractions":[0.03,0.15,0.3,0.5,0.7,0.9],"audio_reviewed":false},"checks":{"male_character":true,"female_audience_relevant":true,"real_person_talking_head":false},"evidence":["evidence/frames/AD_ID/frame-01.png","evidence/frames/AD_ID/frame-02.png","evidence/frames/AD_ID/frame-03.png","evidence/frames/AD_ID/frame-04.png","evidence/frames/AD_ID/frame-05.png","evidence/frames/AD_ID/frame-06.png"]}]}
```

每 ID 恰好一个 keep/exclude/review；全部 frames_inspected=true 才通过。完整观看可用 method=full_video；仅据元数据排除用 metadata，但仍须完成抽帧并查看。保留项必须满足全部内容条件及精确渠道/地区/语言。抽帧只能叫画面初筛，语义不确定为 review，绝不混入 keep。

downloads.json 只列全部 keep，视频可从 sources 复制进交付目录：

```json
{"scope_sha256":"HASH","items":[{"ad_id":"真实ID","path":"delivery/iOS/videos/AD_ID.mp4","sha256":"实际SHA256","width":720,"height":1280,"duration_seconds":26.4,"decode_verified":true}]}
```

源视频与交付视频哈希必须一致；不能审核 A 视频交付 B 视频。解码/参数可引用该视频真实脚本结果，不填未经验证的成功值。

## 交付与人工恢复

delivery.json：

```json
{"scope_sha256":"HASH","included_ad_ids":["真实ID"],"preview_verified":true,"portable_links_verified":true,"files":["delivery/index.html","delivery/manifest.json","delivery/iOS/videos/AD_ID.mp4"],"evidence":["evidence/delivery-preview.png"]}
```

files 应列齐交付所需视频和图片等；实际打开预览，检查播放/图片/链接和相对路径后记录。零保留也交付说明/清单。状态机检查文件存在/非空与全部 keep 已包含，预览语义仍需实际检查。

```bash
python3 scripts/state.py hold --run-dir run --task tipsy-ios --reason captcha --cursor 'page=2,next_position=23'
python3 scripts/state.py resume --run-dir run --task tipsy-ios --confirmation '用户已完成验证' --verification resume-verification.json
python3 scripts/state.py check --run-dir run --require-complete
```

resume-verification.json 包含 scope_sha256、authenticated=true、selected（含 sort 的全部条件）、ui_checks、app_name、platform、app_id、selected_app_ids、residual_keywords、ui_check 和 evidence；筛选与 App 的检查日志必须在 hold 之后新产生。真实用户确认是前提，脚本参数不能替代用户回复。恢复后再次核对页码和下一张卡片。

条件改变新建 run；旧版本记录缺点击检查/抽帧的，不能通过新版校验，也不能补造成功记录。

退出码：0=命令正常（普通 check 未必完成）；1=参数/配置/恢复验证错误；2=带 --require-complete 时仍未完成。最终必须检查 state 与 gate_failure。COMPLETED_WITH_REVIEW 表示明确保留项已交付，另有待复核；不意味着所有素材已确认合格。
