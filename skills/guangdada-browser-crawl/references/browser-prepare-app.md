# 点卡前一次执行

在授权浏览器REPL导入scripts/browser_prepare_app.mjs的gddPrepareApp；不在page.evaluate执行，不启独立驱动。
CATEGORY→FILTERS→APP→SORT一次连续执行，每阶段仍由runStage通过每日CLI begin/complete核验真实证据，失败立即停止。

正确筛选只读，repair只修差异。APP适配器：clearApp只清旧查询/广告主残留；selectSearchMode选择综合；queryInput是当前实际输入框locator，脚本真实fill(App ID)再press(Enter)；readQuery读已提交可见App ID标签，readSubmitted读实际search_mode=综合、query_terms=[App ID]、selected_app_ids=[]、residual_keywords=[]。输入内容被工具脱敏时读提交后可见标签，不凭期望值返回成功。不调用candidate或readSelected，不选广告主候选。clickSort在回车提交后真实点击最新创意，readSort检查选中。

保持tab/playwright/getScreenshot原生截图、task/scopeSha256、persist/sha256、readCategory/readFilters/repairCategory/repairFilters/expectedFilters与runStage适配。from仅为实际已到达的阶段，不跳过状态。准备只确认查询，不替代逐卡实际App ID/平台核验。模块不会自行打开浏览器或调用私有接口。
