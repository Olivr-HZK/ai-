# 授权浏览器内的逐卡批量执行与就绪判断

用于每日混合媒体。读取并在授权浏览器REPL中加载 scripts/browser_batch.js；不是命令行爬虫，禁止在page.evaluate中执行，不用独立Playwright/CDP、私有接口或React隐藏状态。它只调用执行者提供的可见UI读回、浏览器工具locator点击、截图和本地保存适配器。

## 默认行为

综合App ID真实Enter提交/排序通过后，先begin PROBE_PENDING，不选择广告主候选。用同一批量函数maxCards=1验证第一张，保留card JSON和前后截图，完成PROBE。随后begin COLLECTING，从checkpoint.next_position继续；第一张不重新点。首卡已旧于目标日则直接按原始截止证据完成盘点，不再点第二张。零结果仍需显式“暂无数据”证据及原校验，函数不会把空列表当完成。

UI适配确认后，同一页默认一次执行最多10张、总预算45秒；一批内无需模型逐张决定下一步。正常返回只需检查批次统计/断点，继续下一批。同页最后一张返回PAGE_END_VERIFY_NEXT，按既有浏览器工具真实翻页并检查页码/卡片集合改变；不得自动猜测末页。下一App仍按统一状态机一次组合核对筛选、综合App ID回车提交及排序，不选择广告主候选。

每页保存后可将已观察的原素材地址交给同批唯一下载队列，最多3并发，与后续串行点卡并行。下载复用和失败预算跨轮持久保存；不重复启动重叠的后台队列。细节见 [direct-collection.md](direct-collection.md)。这不会提前完成MEDIA_PENDING，也不省略六帧/原图实际查看。

正常每批在本地保存逐卡JSON、两张原生截图与SHA256、card.identity日志、逐卡耗时、就绪观察次数和耗时，以及最后耐久checkpoint。动作日志从result.operations按页批量交给social_chat_daily_state operations（作用在run目录）；事件与card文件路径相对task目录，operations.evidence_paths已转换成相对run目录。每个batchId唯一，不覆盖之前批次截图、checkpoint、卡片或日志。

异常或close失败立即返回needs_attention，保存已完成卡片及下一位置。若checkpoint.detail_open=true，先通过浏览器核对并关闭详情，再按下一位置继续；不重新打开已保存卡片。保存失败不得宣称成功；若结果文件也写失败，异常向上抛出，按实际工具错误写runtime日志。错误进入统一RETRY_REQUIRED；验证码进入WAITING_HUMAN并stop防睡眠租约。自动循环不解决验证码、不自动重试点击。

## 必要适配器（先从当前页面真实DOM/AX确定定位，不猜旧UI）

```javascript
const result = await gddCollectVisibleBatch({
  tab, scopeSha256, task, targetDate, page,
  startPosition: actualCheckpoint.next_position,
  maxCards: 10, batchId: uniqueBatchId,
  observeList, readDetail, cardLocator, closeLocator, persist, sha256
});
```

- task含id、app_name、app_id、platform，来自固定scope；targetDate来自scope，不能在断点恢复时重新计算昨天。
- observeList：从当前可见页面读回page、app_id、platform、card_count（本页0..60）、detail_open、signature，以及captcha/login_required。signature应由本页卡片顺序及当前筛选/排序的可见身份组成，不包含视频播放进度、懒加载封面或推荐区等易变信息。不能把scope期望值直接填为读回值。发生变页、变App或列表变动立即中止。
- cardLocator({page,position,list})：基于刚读回的当前页面解析该卡片locator，返回提供click()的真实浏览器工具对象；不能返回按回车或空操作回调。closeLocator同理，使用实际详情关闭按钮。
- readDetail：一次读回当前详情可见DOM/AX并提取全部所需字段，至少提供loading、app_name、app_id、platform、ad_id、detail_url、raw_text、impressions_estimate、media_type及observed_videos/observed_images/observed_iframes/links。地区、语言、渠道、尺寸等同次读回保存，无则保留真实缺失，不用期望值补齐。图片源必须标记is_original=true（按实际详情原图元素确认），不把App图标/推荐图当原图。
- 直接视频源由observed_videos[].src给出；YouTube原始外链保存links，留给后续补全；有实际“官方资源限制/原始链接/无可用素材”可见提示但源不可得时可记录media_source_unavailable=true及原文，后续仍须有限下载尝试证据，不能直接当媒体成功。试玩读实际iframe，不调用其中脚本。
- persist(path,bytesOrText)：将文件原子保存到当前task根目录，失败抛错；sha256适配器同时接受截图bytes及JSON字符串。本函数没有本地文件读取权限，不负责猜测断点；执行者先读取并核对已有checkpoint/card哈希，再传入下一个位置。最终独立校验仍检查全页连续位置，不能靠startPosition跳卡。
- tab.getScreenshot({emit:false})必须返回原生PNG/JPEG。其他UI能力通过上述适配器使用当前工具文档提供的API。仅在脚本中组装确定性操作，禁止读取页面非可见应用存储或凭据。

## 按页面就绪继续，不固定睡眠

打开卡片后await readDetail，满足以下条件立即保存，不等待视频播放/缓冲、推荐广告主、AI功能等内容：

1. 明确loading=false，且没有验证码/登录提示。
2. 实际App ID和平台匹配，广告ID有效且详情URL包含同一广告ID，不是上一张残留详情。
3. raw_text中出现合法可见详情日期范围。独立首次发现字段不参与截止；重投不在此处跳过。
4. 素材类型明确。目标日候选还需展示估值非空且可解析（保留精度原文）；非目标日边界卡不为补估值而等待。
5. 目标日候选的实际视频/原图/YouTube外链/试玩iframe已可读，或有上述明确无源可见提示。首张旧日期卡在身份、URL、日期和类型核对后即可截止，不等待其无关媒体加载。

未就绪时继续await实际浏览器读回，最多20秒或64次观察，且受批次预算限制。没有固定sleep，也不等待networkidle。身份不符立即停止；缺关键字段到上限返回详情未就绪，不能算零结果。关闭详情后同样读回列表已恢复再继续。

预算检查发生在实际工具调用之间；某个工具调用本身卡住仍由工具超时处理，不承诺中途取消该调用。不要用Promise.race留下仍在点击的后台任务。批次/工具超时都交回模型，记录实际错误后恢复。

## 截止与封账

批量函数保留首张旧日期的卡片及截图，result.terminal含截止ad_id/date/position和实际截图路径/hash/raw_text。终止证据写入原始盘点，seal.screenshot_path可引用这张原生PNG/JPEG，不为固定扩展名转码或重拍。旧seal未提供路径仍兼容evidence/terminal.jpg。本函数只返回单页/单批结果，不自行宣称App或21项全部完成，也不写表或发群消息；依然通过mixed_window及每日状态机封账。
