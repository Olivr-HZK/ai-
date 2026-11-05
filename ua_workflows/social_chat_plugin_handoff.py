"""Local receipt for user-supplied plugin ZIPs; never start browser collection."""
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import zipfile

MODE = 'plugin-handoff-v1'
from ua_workflows.social_chat_template_history import policy_for as template_policy_for, VERSION as TEMPLATE_VERSION
PREVIOUS_DAY_TEMPLATE_POLICY = template_policy_for(TEMPLATE_VERSION)


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
        'auto_retry_downloads': False, 'replication_trigger': 'manual', 'feishu': config['feishu']}
    if not existing:
        from ua_workflows.social_chat_media_orientation import POLICY as ORIENTATION_POLICY
        orientation = config['plugin_handoff'].get('media_orientation_policy')
        if orientation is not None:
            if orientation != ORIENTATION_POLICY:
                raise ValueError('Unsupported media orientation policy')
            scope['media_orientation_policy'] = json.loads(json.dumps(orientation))
        orientation = config.get('orientation_filter')
        if orientation and orientation.get('enabled'):
            scope['orientation_filter'] = json.loads(json.dumps(orientation))
        template_policy = config['plugin_handoff'].get('previous_day_template_dedupe')
        if template_policy is not None:
            if template_policy != template_policy_for(template_policy.get('version')):
                raise ValueError('Unsupported previous-day template dedupe policy')
            scope['previous_day_template_dedupe'] = {
                **template_policy,
                'reference_date': (date.fromisoformat(target) - timedelta(days=1)).isoformat(),
            }
            if template_policy['version'] == TEMPLATE_VERSION:
                mapping_path = Path(__file__).resolve().parents[1] / config['plugin_handoff']['template_identity_mapping']
                scope['template_identity_mapping'] = json.loads(mapping_path.read_text())
        replication = config['plugin_handoff'].get('image_replication_after_ingestion')
        if replication and replication.get('enabled'):
            reference = replication['brand_reference']
            reference_path = Path(__file__).resolve().parents[1] / reference['path']
            if not reference_path.is_file() or sha(reference_path) != reference['sha256']:
                raise ValueError('Approved image replication brand reference changed or is missing')
            scope['image_replication_after_ingestion'] = json.loads(json.dumps(replication))
    from ua_workflows.social_chat_content_policy import policy_for, PLUGIN_COMPANION, PLUGIN_FEEDBACK
    content_policy = scope['content_review_policy']
    if content_policy.get('version') not in (PLUGIN_COMPANION, PLUGIN_FEEDBACK) or content_policy != policy_for(content_policy.get('version')):
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
    if scope.get('media_orientation_policy'):
        review_steps = review_steps.replace('第一道筛选直接按插件报告中的明确重投标签剔除', '第一道先按原文件显示尺寸剔除横屏、暂缓尺寸未知项，并按插件报告中的明确重投标签剔除')
        review_steps = review_steps.replace('对全部已下载非重投素材做日内向量去重', '对第一道通过的已下载素材做日内向量去重')
        review_steps += '\n第一步先剔除明确重投和横屏：读取实际原文件显示尺寸，计入视频旋转/SAR及图片EXIF，宽>高排除，方形保留；尺寸未知暂缓，多图逐张核验。随后才做封面向量去重。入表前和进入视频/图片复刻前各重新核对本阶段源文件尺寸及SHA256，不是横屏才可进入；未知也拦截，不能凭旧审核标记或网页缩略图放行。'
    template_policy = scope.get('previous_day_template_dedupe')
    if template_policy:
        expected = {**template_policy_for(template_policy['version']),
                    'reference_date': (date.fromisoformat(target) - timedelta(days=1)).isoformat()}
        if template_policy != expected:
            raise ValueError('Unsupported pinned previous-day template dedupe policy')
        identity_label='产品' if template_policy['version']==TEMPLATE_VERSION else 'App'
        missing_rule=('前日来源和身份已核验但无可用模板时建立当天新基线，再做产品内去重；缺文件/坏哈希/真实未审核/身份冲突才pending。'
                      if template_policy['version']==TEMPLATE_VERSION else '前一天无批次、覆盖不全或缺模板证据记pending，不能当零历史或宣称跨日模板核验完成。')
        review_steps += f'''
本批额外固定前一天模板比对：参考日{template_policy['reference_date']}，按同{identity_label}/系统完整已审核模板库比较，不只看前一天入表清单。
日内和前7天去重仍都是封面CLIP；前一天模板比对须在联合审核中并列查看源SHA256绑定的六帧/全部原图，必要时补帧/播放，按镜头、脚本钩子、动作、交互及CTA确认，不能用封面向量或模板名称代替。
确认与前一天同模板时剔除当天项，即使当天估值更高也不替换历史；日内合格组仍取最高估值代表。仅换角色、肤色或画法的相同广告结构也按同模板。存疑暂不纳入。
逐项记录same_template/distinct/related_not_duplicate/uncertain，以及reference_date、reference_ids、双方证据哈希和判断理由；{missing_rule}
保存当天全部已确认模板及被跨日剔除项的模板关联，供次日继续比较，避免只存入表项导致同模板隔日重新入选。pre-review只准备封面去重队列，不能认证这一步已完成。'''
        if template_policy['version'] == TEMPLATE_VERSION:
            review_steps += '''
本批固定新版空历史规则：先按scope.template_identity_mapping核实产品身份，系统仍分开。用social_chat_template_history.load_confirmed_history读取前日（有template-history-latest.json时先验证并加载修订历史）。前日模板是否可用取决于它的结构已实际确认，不取决于最终入表、入选或它自己的更早一天去重是否完成，禁止把待核链条逐日传递。
前日来源已核验，且该产品没有可用已确认模板（含已证实首次无前批），当天作为新模板基线，继续产品内模板去重，各组保留最高估值代表；不是全部直接入表。只有缺文件/坏哈希/真实未审核/身份冲突等异常才pending，不能把异常装成零模板。参考social_chat_template_history.history_state和representatives；视觉模板关系仍须原图/六帧实际证据，不能只按字符串模板key猜重复。'''
    review_steps += '''
2026-11-03最新规则：原素材采集、筛选和入表核验后结束，保存待复刻清单并记awaiting_manual_trigger。视频与图片复刻改为用户手动触发；历史scope的自动启用标记不构成新启动授权。需求池手动状态触发规则以当前用户确认的配置为准，未确认前不得提交。启动后内部继续自动排队、核验和结果回填。
用户手动启动后，按skills/social-chat-browser-daily/references/image-replication.md或视频CLI处理明确选中的记录，不重复付费或覆盖原素材。'''
    if content_policy['version'] == PLUGIN_FEEDBACK:
        content_rule = ('男性角色有意义展示、女频恋爱/伴侣陪伴相关；明确真人角色出现即排除，视频/照片/静态图均适用，不以主持或口播为前提。'
                        '女性主体不能靠附带男性镜头通过；男性身份结合角色设定、对白、外观与上下文，不按女仆服等服饰判性别。'
                        '单独操作手机的手部沿原范围，不因本次反馈新增手部排除；写实AI/兽人不自动当真人。'
                        '检查checks.real_person_presence、primary_subject、male_character_display、female_oriented_appeal，并用social_chat_content_policy.validate_review校验；'
                        '明确真人或女性主体exclude，absent+male且两项yes才keep；不确定review，不能仅填无口播当无真人。'
                        '具体人工案例见references/user-feedback-20261104.json；同源明确确认可复用，相似题材仅作判断参考，不自动一概放行。')
    else:
        content_rule = '男性角色有意义展示、女频恋爱/伴侣陪伴相关，排除真人主持式产品推广口播；女性配角、手部及非口播真人恋爱剧情可保留。存疑不纳入，不凑数。'
    prompt = f'''# {target} 插件每日最新素材 · 手动交接

当前：{receipt['status']}；已登记{len(packages)}份ZIP。prepare只登记文件，不表示已审核、入表或完成全量采集。
无ZIP时等待用户交接，不登录/打开广大大，不运行21项App浏览器状态机，不发送采集启动通知。
有ZIP后，先实际读取原下载报告，核对目标日、结束原因、失败/未勾选/重复及媒体文件对应关系；CRC成功不能代替日期或业务元数据核对。
每包保留SHA256与原报告，同日多包合并去重，按原页/位置/广告ID留账；不使用ZIP文件时间猜素材日期。
明确重投先排除；未见标签保持未知。只审核实际取得的视频和全部原图，不自动重采/补下载；试玩不执行。
本批内容规则（{content_policy['version']}）：{content_rule}
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
