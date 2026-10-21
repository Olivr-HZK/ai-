# Guru SSO 接入验收 · 2026-10-20

范围是现有本机素材港 `http://127.0.0.1:8877/`，未发布网站、未改 DNS。

## 预检与真实登记

原认证为本地密码、邀请码与 SQLite 会话；业务为共享素材库、任务队列、人工审核、浏览器 Agent 交接。迁移前使用 SQLite backup 留存完整快照，运行数据在私密目录。原库 106 条素材，83 keep / 4 review / 19 exclude。

按用户提供的 npm URL 安装 Guru SSO CLI 0.6.0，完成系统浏览器 Code+PKCE 登录，`auth status` 显示 authenticated，身份由 CLI `/me` 回读。没有读取或导出 CLI 用户令牌。

`oidc-client plan/apply` 创建 `guru-creative-platform-local`，apply 的 `readbackVerified=true`，state ACTIVE。公开 export 再次确认 native-loopback、authorization_code、token_endpoint_auth_method=none，精确回调 `/auth/callback`、退出回调 `/auth/logged-out`。初次独立 validate 曾网络失败；后续 plan、apply 和 export 成功，不把该次 validate 记为通过。

## 自动化测试

`.venv/bin/python -m unittest discover -s apps/creative-platform/tests -v`：21 项通过（临时数据库、本地签名提供方，无广大大采集）。

- 完整 HTTP 登录→state Cookie→PKCE 回调→本地会话→业务 API→登出→退出回调。
- 错误浏览器绑定、过期、重复参数、拒绝授权、回调重放拒绝。
- 签名伪造、HS256 算法替换、issuer/audience/nonce/azp/at_hash 不匹配和令牌过期拒绝；未知 kid 按轮换刷新一次后拒绝。
- 本地旧会话无法访问 SSO 业务；密码/注册/邀请码 HTTP 返回 410。
- 明确 subject 准入、成员不能领取执行、不按 email/用户名自动关联管理员；原管理员 user ID 保留。
- CSRF、资源认证保护、机器 bearer、Cookie、视频 Range、任务状态与完成证据闸口回归通过。

## 真实浏览器

1. 刷新后的原密码会话失效，进入 Guru 授权页；授权基本 profile/email 后，真实回调成功进入原素材库。
2. 原素材 106 条保持不变：83 已确认 / 4 待复核 / 19 排除；历史任务和所属管理员保留。
3. 点击真实素材卡，原视频 readyState=4、360×640、时长 20.340726s、error=null。
4. 点击退出，实际经过 Guru end_session_endpoint 返回 `/auth/logged-out`，最终显示「已退出工作台和 Guru SSO」。再点击登录成功返回素材库；上游身份提供商可自动认证，不把自动认证当作登出失败。
5. SSO 登录后填写完整 App 名，App ID 实际读回为 6740326134；提交「Guru SSO 端到端验收（不执行抓取）」成功入队并取消。旧「平台验收测试（不执行抓取）」也已取消，队列为 0。未启动广大大采集。

浏览器截图在私密运行目录 `data/creative_platform/sso-login.png` 和 `sso-workspace.png`。没有在代码、本文或日志中保存密码、授权码、client secret 或完整 token。

## 明确未验收范围

公网 web-confidential / HTTPS、另一真实同事账号、Guru Back-Channel Logout 和自动浏览器采集不在本次已验证范围；成员权限有隔离测试，不能替代另一真实账号验收。详细配置、准入、会话期限、退出边界和恢复方式见 README。
