# guangdada-browser-skill

广大大展示广告浏览器采集 skill 的 npm 安装器，版本 **1.0.0**。

本包将 skill 安装到 Codex 的 skills 目录。它不直接启动爬虫，不附带浏览器控制能力、广大大账号或登录态；实际采集由具备浏览器工具的 Codex 会话执行。

## 安装

发布到 npm 后可使用以下命令（当前仓库中的包是否已发布，以 npm registry 为准）：

```bash
npx guangdada-browser-skill@1.0.0 install
npx guangdada-browser-skill@1.0.0 doctor
```

也可以全局安装：

```bash
npm install -g guangdada-browser-skill@1.0.0
guangdada-browser-skill install
```

默认目标为 `$CODEX_HOME/skills/guangdada-browser-crawl`；未设置 `CODEX_HOME` 时为 `~/.codex/skills/guangdada-browser-crawl`。

```bash
# 只查看安装计划，不写文件
guangdada-browser-skill install --dry-run --json

# 指定其他 skills 目录
guangdada-browser-skill install --skills-dir "/path/to/skills"

# 替换不同版本或本地修改；旧目录保留在 skills 同级的 skill-backups 中
guangdada-browser-skill install --force
```

已有相同文件时安装不会重复写入；已有不同文件默认拒绝覆盖。目标是符号链接或普通文件时拒绝安装。`npm install` 本身没有 postinstall 副作用，需明确运行 `install`。

本地分享 `.tgz` 也可安装，无需先发布到 registry：

```bash
npm install -g ./guangdada-browser-skill-1.0.0.tgz
guangdada-browser-skill install
```

## 使用与依赖

需要 Node.js 20+；skill 状态校验需要 Python 3.10+，视频抽帧需要 FFmpeg 和 ffprobe。`doctor` 只检查命令是否可用，不自动安装依赖，也不声称已验证浏览器或登录状态。

安装后，在能够使用浏览器工具的 Codex 新会话中输入：

```text
使用 $guangdada-browser-crawl，按完整 App 名采集广大大展示广告。
工具类，近30天，Facebook、TikTok、AppLovin，日本和韩国，视频9:16，文案语言不限。
分别采集各 App 和平台，逐卡翻页后抽帧筛选，并报告保留、排除及待复核数量。
```

支持游戏/工具、渠道、国家、广告/素材、7天/30天/90天/1年及自定义日期。App 与筛选通过真实点击读回和前后截图核对，逐页完成清单支持恢复断点；视频六处抽帧后仍需实际查看，抽帧不能代替完整观看或审听。验证码由用户手动完成。

安装器零 npm 依赖、无网络请求；打包内容限 skill 指令、辅助脚本、参考文件及安装器，不包含项目数据、视频、账号、cookies、私有爬虫或下游业务。内置文件哈希用于检测损坏和内容一致性，不是发布者签名。

## 开发与发布

在本目录执行：

```bash
npm test
npm pack --dry-run
npm pack
# 确认最终包名、发布账号及分发许可后，由维护者执行
npm publish --access public
```

`prepack` 从仓库的 `skills/guangdada-browser-crawl/` 按八个文件的白名单构建独立 payload，并核对 skill 与 npm 版本一致。生成的 `skill/` 不纳入 Git；tarball 包含完整 payload，安装无需原仓库。

包名暂定 `guangdada-browser-skill`。`UNLICENSED` 表示此版本未授予开源许可；公开发布前由权利人决定分发许可。
