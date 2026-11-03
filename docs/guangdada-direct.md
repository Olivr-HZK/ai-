# 广大大独立采集 v0.2.1

这是一套独立的Playwright＋HTTP下载程序，不安装、不加载、不调用Chrome扩展，不使用chrome.runtime、扩展后台、offscreen或扩展权限。开发时采用与插件一致的卡片读取、身份核验和日期口径，规则存为独立browser-readers.js；运行目录中不需要原插件源码或manifest。

## 固定流程

指定`--date YYYY-MM-DD`，依次准备：游戏→快捷操作→afizzy竞品游戏；工具→afizzy快捷操作→afizzy竞品。每次应用预设后都重新把网站开始/结束日期设为指定日，再选择最新创意，核对实际分类、日期、排序及连续稳定的卡片身份。整个流程只正常操作网页，不调用广大大私有接口。

一页批量读取已有的原媒体链接、App/展示估值/重投信息。缺址先滚动补读，仍缺视频地址或日期才正常打开对应详情，并核对广告ID；每条新地址在关闭详情、处理下一卡或翻页前持久化。遇旧日期截止，记录本页已观察的后续位置为未纳入。日期当天保留，其他缺失/不支持/未核验单独报告。

两个预设的地址先顺序采集并保存，再关闭浏览器，直接通过HTTP三路并发下载CDN原文件。下载不读取广大大网页，不导出/复制Cookie；签名链接仅在本机私有断点中保存，MD不输出。媒体分块落盘、核对长度/实际媒体流、逐项哈希，Python ZIP64流式汇总，每组一ZIP＋唯一下载报告.md。浏览器或账号掉线不影响已经保存的公开CDN地址，但CDN地址本身过期/拒绝仍会失败并记原因。

支持图片、视频、Playable原HTML。Playable不执行脚本、也不自动获取外部依赖。普通视频外链静态尝试明确完整视频直链；公开YouTube外链通过独立yt-dlp解析和FFmpeg合并，优先原始H264/AAC流，不转码、不读取浏览器Cookie或命令行配置。DRM、私有/需登录视频和验证限制不绕过，不保证所有外链可下载。媒体下载失败不以封面替代。

## 安装与运行

独立包解压进入根目录：

```sh
sh install.sh
./scripts/guangdada-direct run --date 2026-11-02 --credentials-env /path/to/existing/project/.env
```

从Git项目安装时执行`sh apps/guangdada-direct/install.sh`；安装器会定位项目根目录并读取自身目录中的requirements-direct.txt。独立交付包则仍执行根目录`sh install.sh`。

现有项目`.env`的字段为GUANGDADA_EMAIL、GUANGDADA_PASSWORD。通过正常登录表单使用，不复制到部署包、不打印凭证、不读取Chrome/Keychain。已登录会话正常复用；需要登录时只提交一次，遇安全验证暂停。默认无头；调试可加`--headed`。无需`setup`扩展授权。

默认私有状态`~/.local/share/gdd-direct`，输出`~/Downloads/广大大独立采集/<日期>_both/`。每组有准备前后截图及原始ZIP和MD；私有状态含带签名URL，应保持本机私密，不上传/分享。默认两个预设全部类型，产出的原包和报告供后续筛选、入表及复刻流程使用，不自动创建定时任务。

兼容入口`scripts/guangdada-headless`现转到此独立程序。旧v0.1.0调用扩展的实现与历史包仅保留作历史，不是新入口。新包本身不包含Chrome扩展。

## 小样、暂停和恢复

```sh
# 两组各保存并下载2个素材，仅测试，不算整日完成
./scripts/guangdada-direct run --date 2026-11-02 --sample-per-preset 2 --credentials-env /path/to/.env
# 核对原预设/日期/页面和广告ID后恢复原断点
./scripts/guangdada-direct resume --date 2026-11-02 --credentials-env /path/to/.env
# 明确结束继续采集，仅下载原断点已保存且日期核验通过的地址，无需打开浏览器
./scripts/guangdada-direct download-collected --date 2026-11-02
# 只重试失败下载；成功文件按哈希复用，不重新采集、不打开浏览器
./scripts/guangdada-direct retry-failed --date 2026-11-02
```

可用`--only game/tool`、`--max-pages N`及`--output`。小样和页数上限在报告里注明，不冒充末页/整日完成。恢复需使用原日期、范围和输出参数。文件锁同时与历史专用入口互斥；拒用非本工具专用的浏览器目录。翻页前保存动作意图；结果不确定时只恢复指定下一页并核验，不盲目再点下一页。已确认地址不被后续空读覆盖，已核验文件可复用。媒体字节不跨重启续传。

`retry-failed`保留原ZIP和旧失败原因，新包追加_retryN，MD对应新包；请交接最新包。HTTP错误保留状态码，重复原地址明确关联到成功原文件。

`complete`指本次指定范围处理结束；`all_media_downloaded`才表示没有失败/缺址/未处理项。`collection_complete`还要求无小样/限页且以真实日期截止或末页结束，二者均满足才标`full_day_complete=true`。存在下载失败退出码为2，不以“已有ZIP”冒充全量成功。全部媒体传输失败不生成空ZIP，也不标成功。

## 当前验证

本机独立实站小样已完成两组预设、日期2026-11-02、保存地址后关闭浏览器再下载：游戏1个Playable原HTML及1个视频、工具2个视频，各一ZIP，CRC/原字节哈希核验；三个视频完整FFmpeg解码通过。HTML只静态核验未执行。浏览器证据extension_loaded=false、extension_runtime_called=false。另有独立程序定向测试及已有页面适配回归，包括无扩展浏览器真实翻页/日期截止、关闭浏览器后HTTP下载、缺址、重复文件复用、HTML不执行、错误文件名、未知翻页不重放、关闭详情前地址落盘、打包失败不冒充完成、公开YouTube下载边界及只重试失败且保留原包。

Mac mini已实际将2026-11-02两个预设采集至真实日期截止：游戏32张、工具576张目标日卡片，另52张本页旧日期卡片排除。初次下载后用v0.2.1只重试失败项，最终602个媒体文件成功、2张重复共用文件、4项未取回（2张CDN原图404、1项无原视频、1项YouTube地址截断）。602文件逐项原字节/ZIP哈希及CRC相符，477视频＋99图片完成解码，26个HTML仅静态核验。18项校验输出的DTS提示经过内存输出时间戳归一化复核，未修改原文件。后续重读失败项时出现其他设备登录及强制退出提示，没有重新登录。用户已要求不继续补无法解析的外链；不能把这轮称所有素材成功，all_media_downloaded及full_day_complete如实为false。

v0.2.1独立20项本机及Mac mini回归通过，运行文件逐字节核对。完整实站证据及MD报告data/guangdada-direct-deploy-20261103/，小样证据data/guangdada-direct-acceptance-20261103/。这些验证不等于所有外链或所有实站异常已通过。
