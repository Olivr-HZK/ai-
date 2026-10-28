"""Local receipt for user-supplied plugin ZIPs; never start browser collection."""
from datetime import date
import hashlib
import json
from pathlib import Path
import zipfile

MODE = 'plugin-handoff-v1'


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _inspect_package(path):
    path = Path(path).expanduser().resolve()
    if not path.is_file() or path.suffix.lower() != '.zip':
        raise ValueError('Plugin handoff requires an existing ZIP')
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(set(names)) != len(names):
            raise ValueError('Duplicate ZIP member names')
        for name in names:
            normalized = name.replace('\\', '/')
            if Path(normalized).is_absolute() or '..' in Path(normalized).parts:
                raise ValueError('Unsafe ZIP member path')
        if names.count('下载报告.md') != 1:
            raise ValueError('Plugin ZIP must contain its original 下载报告.md')
        if archive.testzip() is not None:
            raise ValueError('Plugin ZIP CRC failed')
        report_bytes = archive.read('下载报告.md')
        report_bytes.decode('utf-8-sig')  # Require a readable original report, without rewriting its bytes.
        media = {'视频': 0, '图片': 0, 'HTML试玩': 0}
        for name in names:
            ext = Path(name).suffix.lower()
            if ext in ('.mp4', '.webm', '.mov', '.m4v'):
                media['视频'] += 1
            elif ext in ('.jpg', '.jpeg', '.png', '.webp', '.gif', '.avif'):
                media['图片'] += 1
            elif ext in ('.html', '.htm'):
                media['HTML试玩'] += 1
    return {'path': str(path), 'sha256': sha(path), 'size': path.stat().st_size,
            'crc_verified': True, 'report_sha256': hashlib.sha256(report_bytes).hexdigest(),
            'archive_media_file_counts': media,
            'date_metadata_and_download_ledger_checked': False}


def inspect_package(path):
    try:
        return _inspect_package(path)
    except (zipfile.BadZipFile, RuntimeError, UnicodeDecodeError) as exc:
        raise ValueError('Invalid plugin ZIP or unreadable original report') from exc


def prepare(target, run_dir, config, source_zips=(), execution_origin='manual'):
    from ua_workflows.social_chat_browser_daily import load, save
    from ua_workflows.social_chat_ranking import rule, label
    from ua_workflows.social_chat_style_tags import validate_style_policy
    from ua_workflows.social_chat_plugin_review_plan import POLICY, validate_policy
    date.fromisoformat(target)
    if execution_origin != 'manual':
        raise ValueError('Plugin handoff is manual; supply the downloaded ZIPs in chat')
    base = Path(run_dir).resolve()
    existing = load(base / 'scope.json') if (base / 'scope.json').exists() else None
    if existing and (existing.get('collection_mode') != MODE or existing.get('target_date') != target):
        raise ValueError('Existing run has a different scope; use a new directory.')
    if not existing and base.exists() and any(p.name != 'runtime' for p in base.iterdir()):
        raise ValueError('Handoff directory must be empty or an existing matching handoff')
    if not existing and config['plugin_handoff'].get('processing_policy_version', POLICY['version']) != POLICY['version']:
        raise ValueError('Unsupported plugin processing policy')
    scope = existing or {'collection_mode': MODE, 'execution_origin': 'manual', 'target_date': target,
        'ranking': rule(config['ranking']), 'style_tagging': config['style_tagging'],
        'processing_policy': json.loads(json.dumps(POLICY)),
        'content_review_policy': config['plugin_handoff']['content_review_policy'],
        'collection_complete_21_apps': False, 'auto_browser_collection': False,
        'auto_retry_downloads': False, 'feishu': config['feishu']}
    if scope['content_review_policy'] != {
        'version': 'male-display-female-oriented-no-talking-head-v1', 'male_display': 'required',
        'female_oriented_appeal': 'required', 'real_person_talking_head': 'exclude', 'uncertain': 'not_admitted'}:
        raise ValueError('Unsupported plugin handoff content policy')
    validate_style_policy(scope['style_tagging'])
    processing = scope.get('processing_policy')
    if processing is not None:
        validate_policy(processing)
    incoming = [inspect_package(p) for p in source_zips]
    prior = load(base / 'handoff.json') if (base / 'handoff.json').exists() else None
    fingerprint = hashlib.sha256(json.dumps(scope, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    if prior and prior['scope_sha256'] != fingerprint:
        raise ValueError('Pinned handoff scope changed')
    for package in (prior or {}).get('packages', []):
        if not Path(package['path']).is_file() or sha(package['path']) != package['sha256']:
            raise ValueError('Previously handed-off ZIP changed or is missing')
    packages = {p['sha256']: p for p in (prior or {}).get('packages', [])}
    for package in incoming:
        packages.setdefault(package['sha256'], package)
    receipt = {'status': 'PACKAGE_RECEIVED' if packages else 'WAITING_PACKAGE',
        'scope_sha256': fingerprint, 'target_date': target, 'collection_mode': MODE,
        'packages': list(packages.values()), 'review_status': 'not_certified_by_intake',
        'collection_complete_21_apps': False, 'browser_started': False,
        'feishu_written_by_intake': False, 'group_message_sent_by_intake': False}
    if not existing:
        save(base / 'scope.json', scope)
    if receipt != prior:
        save(base / 'handoff.json', receipt)
    review_steps = '''第一道筛选直接按插件报告中的明确重投标签剔除，不重新点卡猜测；未见标签仍为未知。随后对全部已下载非重投素材做日内向量去重、目标日前7自然日跨日向量去重，最后按产品一次联合做主体/内容审核、风格打标及模板去重，共四道筛选。
不先按旧内容审核决定过滤。CLIP阈值0.9，按完整App身份及系统分组，每个日内簇保留展示估值最高者。缺历史日期/身份/向量须留账，不当作零结果。
生成标准化清单后，运行pre-review入口（--media-index与--embedding-cache），只产生JOINT_REVIEW_PENDING队列，不认证人工审核完成。向量缓存须绑定同一CLIP模型和代理图SHA256；缺缓存先本地计算，不调用OpenRouter或私有接口。
去重阶段使用原封面或已有3%代理帧；不做内容人工初筛。只对去重剩余项核验完整解码（已有源SHA匹配回执可复用），再按产品分组，一次准备六帧并同时检查内容、模板和风格。源文件/参数/帧哈希一致的证据复用；没有六帧才提取一套，若3%代理已是同参数真实帧，复用并补其余五帧。不得为了三个审核任务各抽一套/看一轮。
在同一条joint_review中填写content、template、style及画面依据；逐产品俯瞰镜头顺序、动作、对白钩子、交互与结束页，判定仅换角色外观或画法的模板变体；共同版式不等于相同模板。内容或模板存疑暂不纳入；通过项才填写正式风格标签。证据不足可放大/补帧/播放，不宣称一次六帧能保证检出所有问题。
内容审核完后，在合格同模板组内仅留最高展示估值一条；代表不过审时不自动复活已被前序去重剔除项补数。交付数量严格遵循本批scope.ranking。''' if processing else '''本历史scope没有固定联合审核版本，不自动迁移或改变历史顺序。
视频实际解码、抽六帧并查看，图片逐原图解码/查看，记录决定、理由、证据哈希；六帧不称完整审听。
通过项同次按scope固定风格版本打标，合并按展示估值降序取scope.ranking规定的前100条（不足取实际通过数）。'''
    review_steps += '\n本批固定交付范围：' + label(scope['ranking']) + '。展示估值只决定排序和同模板代表；不隐式套用前100/前10%限制。'
    prompt = f'''# {target} 插件每日最新素材 · 手动交接

当前：{receipt['status']}；已登记{len(packages)}份ZIP。prepare只登记文件，不表示已审核、入表或完成全量采集。
无ZIP时等待用户交接，不登录/打开广大大，不运行21项App浏览器状态机，不发送采集启动通知。
有ZIP后，先实际读取原下载报告，核对目标日、结束原因、失败/未勾选/重复及媒体文件对应关系；CRC成功不能代替日期或业务元数据核对。
每包保留SHA256与原报告，同日多包合并去重，按原页/位置/广告ID留账；不使用ZIP文件时间猜素材日期。
明确重投先排除；未见标签保持未知。只审核实际取得的视频和全部原图，不自动重采/补下载；试玩不执行。
本批内容规则：男性角色有意义展示、女频恋爱/伴侣陪伴相关，排除真人主持式产品推广口播；女性配角、手部及非口播真人恋爱剧情可保留。存疑不纳入，不凑数。
{review_steps}
生成审核看板及Afizzy_竞品精选_YYYY-MM-DD_N条.zip，包内日期-序号-广告主及CSV/审核说明/SHA256。
按已有授权写指定飞书表并读回实际字段/附件token及大小；主列链接写原广大大详情的真实URL，独立App ID不可见留空；普通文本标签不代表失链，需富文本模式读取真实link；批次/日期/源包摘要/广告ID幂等，保护历史与手动记录。
所有视频、图片或已有Playable附件统一写config.feishu.attachment_field_id对应的附件字段，当前名称“竞品素材（图片/视频/Playable）去除男频”；不再按类型分写视频附件/图片附件，不重建旧列。实际API写入前按字段ID读回当前名称，图片多图全部保留；附件归并不改变审核范围，不执行Playable。
不得伪造21项App全采、首次发现或重投结论，不套用旧全量日报；群播报如执行须使用实际交接批次统计和既有明确授权，不重复发送。
下一步：读取skills/social-chat-browser-daily/references/plugin-handoff.md。
'''
    task = base / 'handoff-task.md'
    if not task.exists() or incoming:
        task.write_text(prompt)
    return {'status': receipt['status'], 'target_date': target, 'run_dir': str(base),
            'packages': len(packages), 'prompt': str(task), 'browser_started': False}
