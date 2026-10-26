"""Manual browser-run planning, global top-decile selection and Feishu export.

Guangdada collection stays in the authorized browser skill. This module only
handles local evidence plus the explicitly requested Feishu table.
"""
from __future__ import annotations

from ua_workflows.collection_runtime import collection_phase
import argparse
import hashlib
import importlib.util
import json
import math
import re
import sys
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'config/social_chat_browser_daily.json'
FSM_SCRIPT = ROOT / 'skills/guangdada-browser-crawl/scripts/state.py'
TZ = ZoneInfo('Asia/Shanghai')
FIELDS = [
    ('appid', 1), ('目标日期', 1), ('首次发现日期', 1), ('广告主', 3),
    ('平台', 3), ('App ID', 1), ('广告ID', 1), ('展示估值', 2), ('展示估值原文', 1),
    ('当前入选', 7), ('来源', 3),
    ('投放渠道', 4), ('投放地区', 4), ('文案语言', 4), ('素材尺寸', 1),
    ('视频链接', 15), ('广大大详情', 15), ('非真人口播判断', 1),
    ('审核说明', 1), ('审核方式', 1), ('采集批次', 1), ('唯一键', 1), ('同步时间', 1),
]


def load(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    tmp.replace(path)


def fsm():
    spec = importlib.util.spec_from_file_location('gdd_browser_daily_fsm', FSM_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def yesterday(now=None):
    return ((now or datetime.now(TZ)).astimezone(TZ).date() - timedelta(days=1)).isoformat()


def tasks(config):
    apps = load(ROOT / config['competitor_list'])['apps']
    registry = ROOT / config.get('app_id_registry', 'config/social_chat_browser_app_ids.json')
    known = {(x['app_name'], x['platform']): x['app_id'] for x in load(registry)['items']} if registry.exists() else {}
    result = []
    for platform in ['iOS', 'Android']:
        for i, name in enumerate(apps[platform], 1):
            item = {'id': f'{platform.lower()}-{i:02d}', 'app_name': name, 'platform': platform}
            if (name, platform) in known:
                item['app_id'] = known[name, platform]
            override = config.get('category_overrides', {}).get(item.get('app_id'), {}).get(platform)
            if override:
                if override not in ('工具', '游戏'):
                    raise ValueError('Invalid category override')
                item['category'] = override
            result.append(item)
    return result


def prepare(target, run_dir, config, execution_origin="manual"):
    import time
    prepared_monotonic=time.monotonic();prepared_started_at=datetime.now(ZoneInfo('UTC')).isoformat()
    date.fromisoformat(target)
    if execution_origin not in ("manual", "scheduled"):
        raise ValueError("Unknown execution origin")
    scope = {'filters': {**config['filters'], 'date': {'mode': 'custom', 'start': target, 'end': target}},
             'content_checks': config['content_checks'], 'tasks': tasks(config),
             'date_anchor': {'as_of_date': datetime.now(TZ).date().isoformat(), 'timezone': 'Asia/Shanghai'},
             'date_semantics': config['date_semantics'], 'target_date': target,
             'execution_origin': execution_origin}
    from ua_workflows.social_chat_ranking import rule as ranking_rule, label as ranking_label
    scope['ranking'] = ranking_rule(config.get('ranking'))
    if config.get('content_review_policy') is not None:
        scope['content_review_policy'] = config['content_review_policy']
    if config.get('style_tagging') is not None:
        scope['style_tagging'] = config['style_tagging']
    if config.get('collection_mode'):
        if config['collection_mode'] != 'direct-browser-v1':
            raise ValueError('Unsupported collection mode')
        scope['collection_mode'] = config['collection_mode']
    if config.get('media_types'):
        scope['media_types'] = config['media_types']
        scope['media_filter_mode'] = 'default_images_and_videos'
        scope['media_collection_checks'] = [
            {'task_id': task['id'], 'media_types': config['media_types'], 'selected_types': []}
            for task in scope['tasks']]
    module = fsm()
    module.validate_scope(scope)
    run_dir = Path(run_dir)
    if (run_dir / 'scope.json').exists():
        previous = load(run_dir / 'scope.json')
        # Existing evidence retains its original ranking rule and scope hash.
        if 'ranking' in previous:
            scope['ranking'] = previous['ranking']
        else:
            scope.pop('ranking', None)
        # An old scope remains pinned to its original review policy on resume.
        if 'content_review_policy' not in previous:
            scope.pop('content_review_policy', None)
        else:
            scope['content_review_policy'] = previous['content_review_policy']
        if 'style_tagging' not in previous:
            scope.pop('style_tagging', None)
        else:
            scope['style_tagging'] = previous['style_tagging']
        if {k: v for k, v in previous.items() if k not in ('date_anchor','execution_origin')} != {k: v for k, v in scope.items() if k not in ('date_anchor','execution_origin')}:
            raise ValueError('Existing run has a different scope; use a new directory.')
        return {'status': 'resume_existing', 'run_dir': str(run_dir.resolve()), 'tasks': len(scope['tasks']),
                'review_policy': previous.get('content_review_policy', {}).get('version', 'legacy_talking_head')}
    # The wake guard writes runtime evidence before scope preparation begins.
    if run_dir.exists() and any(p.name != 'runtime' for p in run_dir.iterdir()):
        raise ValueError('Run directory must be empty or an existing matching run.')
    from ua_workflows.social_chat_content_policy import policy_for
    policy = scope.get('content_review_policy')
    if policy is not None and policy != policy_for(policy.get('version')):
        raise ValueError('Unsupported content review policy')
    from ua_workflows.social_chat_style_tags import validate_style_policy
    if scope.get('style_tagging') is not None:
        validate_style_policy(scope['style_tagging'])
    module.write(run_dir / 'scope.json', scope)
    module.write(run_dir / 'run.json', {'scope_sha256': module.digest(scope)})
    for task in scope['tasks']:
        (run_dir / 'tasks' / task['id']).mkdir(parents=True, exist_ok=True)
    # Mixed daily runs have one authoritative machine; do not initialize the
    # video-only FSM/state.json beside it. Keep legacy video compatibility.
    if scope.get('media_types') != ['视频', '图片']:
        module.inspect(run_dir)
    from ua_workflows.social_chat_daily_state import init as init_daily_state
    init_daily_state(run_dir,prepared_started_at,prepared_monotonic)
    media = '、'.join(config.get('media_types', [config['filters'].get('media_type', '视频')]))
    from ua_workflows.social_chat_content_policy import MALE_PRIMARY_NO_REAL_PERSON, NO_REAL_PERSON, policy_for
    policy = scope.get('content_review_policy')
    version = policy.get('version') if policy else None
    if policy and policy != policy_for(version):
        raise ValueError('Unsupported content review policy')
    if version == MALE_PRIMARY_NO_REAL_PERSON:
        content_rule = ('视频和图片仅保留男性角色为主体且未见真人的素材。任何真人出镜或女性主体均排除；'
                        '真人包含剧情/生活/影视/照片/手部及混剪中的短暂真人。男女混合、无明确男性主体、真人/AI无法区分均待复核，不得排名。'
                        '明显AI/动漫/CG男性不因写实画风排除；角色主体不等同于目标受众性别。'
                        '审核记录review_policy=male-character-no-real-person-v1；checks.real_person_presence=present/absent/uncertain，'
                        'checks.primary_subject=male/female/mixed/none/uncertain。仅absent+male可keep；present或female必须exclude。')
    elif version == NO_REAL_PERSON:
        content_rule = ('本批固定旧版no-real-person-v1：视频和图片出现任何真人即排除，真人/AI不确定待复核；'
                        '审核记录review_policy=no-real-person-v1及checks.real_person_presence=present/absent/uncertain。')
    else:
        content_rule = '按本批原规则排除真人口播。'
    style = scope.get('style_tagging')
    style_naming = ('仅使用一个风格标签字段，地域审美与画面风格组合命名；古装是题材，写入style_reason，不作风格选项。'
                    '地域审美无法判断但画法明确时可用数字插画或3D角色，不强行猜国家。厚涂旧标签须实际复核后区分，不批量映射。'
                    if style and style['version'] == 'character-style-v2'
                    else '本批沿用固定的旧版风格选项，不自动改名或迁移。')
    style_rule = (f"内容审核通过后，同次实际查看画面并写style_tagging_version={style['version']}、style_tags（多选）、style_reason（画面依据）。"
                  f"选项：{'、'.join(style['tags'])}。"
                  f'{style_naming}'
                  '不按App/投放地区/语言猜风格；风格无法细分时仅填待确认，不影响已经明确通过的内容审核。'
                  '排除/内容待复核不打正式风格标签。标签随排名结果、交付看板和下次飞书同步携带，风格标签为多选字段。'
                  if style else '本历史批次未要求风格标签，不自动补标。')
    ranking_text = ranking_label(scope.get('ranking'))
    instruction = f'''使用 $social-chat-browser-daily 和 $guangdada-browser-crawl 最新规则完成此日采集任务，run目录：{run_dir.resolve()}
本批固定内容规则：{content_rule}
本批风格打标规则：{style_rule}
浏览器采集前执行 .venv/bin/python -m ua_workflows.browser_collection_runtime start --run-dir "{run_dir.resolve()}" 并确认 active=true；每页操作前及每5分钟 renew 同一目录并确认 active=true。保护15分钟未续期自动过期；过期则先start核验再继续。验证码/人工等待、暂停、完成或异常停止时执行stop；恢复先start。不要把prepare命令的短暂保护当作整个浏览器采集保护。异常按skill记录实际工具消息、阶段、App/页/卡和证据路径；工具无堆栈不得编造。
目标日：{target}，Asia/Shanghai。scope.json 已包含完整21项App/平台名单，优先使用已核实App ID搜索；缺ID时先按完整名发现并核对ID，然后再用该ID搜索，不用简称搜索，不调用私有接口或项目爬虫CLI。
以 daily-machine/state.json 为每日唯一执行状态，读取 skill references/daily-state-machine.md。每个浏览器阶段先执行 social_chat_daily_state begin，实际操作并保存证据后 complete --proof；失败执行fail记录真实错误，修复后resume再重试。正确筛选必须readonly，只有不一致才repair；禁止为日志重复点击。新批不得调用旧state.py推进每日状态。
按skill的分类→基础筛选（含日期）→选择综合搜索→清除旧关键词/广告主残留→输入App ID并真实回车提交→核对综合查询唯一关键词为本App ID→重新点击最新创意（不等待或点击广告主候选；逐卡详情核对实际App ID及平台）→逐卡→逐页→seal顺序采集全部21项。
默认采用guangdada-browser-crawl的references/direct-collection.md浏览器直采模式，不依赖插件弹窗：一次组合执行点卡前检查；同页最多10张连续点卡，详情就绪立即保存，正常只返回批次摘要。每页落盘后可将已观察原素材交给同批唯一下载队列，最多3路下载与后续串行点卡并行；不启动重叠队列，成功文件按哈希复用，失败预算跨轮保留。下载预取不提前推进MEDIA/REVIEW关卡。
基础条件：工具、素材，获取{media}。类型控件不选任何选项时即为“图片&视频”：只读检查选项为空及默认显示，有残留类型才清除。每项App/平台只采一次混合列表，共21项；逐卡识别视频/图片，不切换两轮。渠道/地区/文案语言/尺寸均不限。不限条件只读检查，有残留才清除，实际操作保存读回和截图。
日期控件设置目标日范围；每张卡片另从可见详情核对首次发现。按17号规则，只用详情 YYYY-MM-DD~YYYY-MM-DD 的起点判断目标日期，保存 first_discovered_date、first_discovered_source=detail_date_range_start 及 raw_text；独立首次发现字段仅记录，不参与日期判断或提前截止。缺详情日期范围则阻塞。
最新创意从新到旧，遇首张详情首次发现早于目标日即停止本App的混合列表并保存截止证据；候选不存在不等于零素材。重投卡片同样先采集，不因重投标签或其旧独立首次发现日期停止。21项全部封账后，按可见独立“重投”标签统一剔除并记录原因/数量，再对剩余素材审核排名；重投保留原始详情证据。全部App登记并排除重投后，复用预取文件并补齐缺失视频、运行frames.py、实际查看六帧，执行本批固定内容规则；图片下载原图、核验解码/格式/尺寸/SHA256并实际查看，多图逐图审查。图片不伪造视频六帧，同样执行本批固定内容规则。视频抽帧初筛不宣称完整审听。
按skill的独立目标日混合媒体校验完成后，将视频和图片有效候选合并按展示估值降序取{ranking_text}，不降低审核标准补数，再写已授权飞书表，读回实际视频/图片附件；旧视频FSM/select不支持图片，不可伪造通过，须完成并验证混合媒体适配再使用。缺失媒体有限重试后按用户要求单列跳过，未全采/身份不明/缺估值/审核不确定不可静默跳过。
审核及排名后的delivery步骤自动将实际入选媒体单独打包为Afizzy_竞品精选_YYYY-MM-DD_N条.zip，目标日期和数量均按本批实际值；附排名、估值、风格、审核说明CSV和SHA256。核对delivery/package-receipt.json的数量与CRC/媒体哈希结果后提供ZIP链接。不要将打包当成飞书入表或日报成功。
采集开始及核验入表后，分别按config的crawl_start_notice和crawl_completion_report向核实测试群发送一次启动通知及一次完成日报；同批恢复不重发，不发其他群。凭据不输出。定时触发批次遇登录失效或其他设备登录先自行正常重新登录，使用已有保存凭据或项目.env，不输出秘密；手动批次先询问。任何验证码立即hold/stop并等待人工，不自动处理。重登后核对登录、筛选、已选App和断点，不重发通知。
本批实际触发来源execution_origin={execution_origin}（不使用config默认值推断），不得重复创建定时任务。完成报告目标日期、21项App混合列表检查情况、视频/图片分项与合计、按{ranking_text}的实际入表数及链接。
'''
    if scope.get('collection_mode') == 'direct-browser-v1':
        instruction = instruction.replace('每个浏览器阶段先执行 social_chat_daily_state begin，实际操作并保存证据后 complete --proof；失败执行fail记录真实错误，修复后resume再重试。', '每个App只有APP_COLLECTION_PENDING一次begin和一次直采accept验收，由direct_session.mjs自动执行，不再逐CATEGORY/FILTERS/APP/SORT/PROBE调用CLI；失败保存断点并hold，实际恢复后resume。')
        instruction += '\n本批collection_mode=direct-browser-v1：在授权浏览器REPL导入guangdada-browser-crawl/scripts/direct_session.mjs的createDirectSession，显式传当前tab/browser/runDir/taskId/projectRoot。依次start()、batch()、page_end时nextPage()，脚本在真实截止/末页/零结果后生成direct/complete.json并调用social_chat_direct accept；不要使用临时direct-complete.json或手改状态。每App实际分类用task.category覆盖默认分类，Heyyyy iOS为游戏。\n下载入口：.venv/bin/python -m ua_workflows.social_chat_direct_media prefetch --run-dir <本批目录> --watch（单队列锁、3并发、15分钟有界watch）；采集结束后再prefetch一次补齐尾部，随后review-media生成现有六帧/原图manifest和真实失败例外，不自动生成审核决定。后续仍走原RELAUNCH/MEDIA/REVIEW/RANK/DELIVERY/SYNC/REPORT。\n'
    (run_dir / 'browser-task.txt').write_text(instruction)
    return {'status': 'prepared', 'target_date': target, 'run_dir': str(run_dir.resolve()), 'tasks': len(scope['tasks']), 'prompt': str((run_dir / 'browser-task.txt').resolve()),
            'review_policy': scope.get('content_review_policy', {}).get('version', 'legacy_talking_head')}


def estimate(value):
    if isinstance(value, bool):
        raise ValueError('Invalid boolean estimate')
    text = str(value).strip().replace(',', '').replace('，', '').replace(' ', '')
    match = re.fullmatch(r'(?:约|≈|~)?(\d+(?:\.\d+)?)\s*([KkMmBb万亿]?)', text)
    if not match:
        raise ValueError(f'Unusable impression estimate: {value!r}')
    try:
        number = Decimal(match[1]) * {'': 1, 'K': 1000, 'M': 1000000, 'B': 1000000000, '万': 10000, '亿': 100000000}[match[2].upper()]
    except (InvalidOperation, KeyError) as exc:
        raise ValueError('Invalid estimate') from exc
    if not number.is_finite() or number < 0 or not math.isfinite(float(number)):
        raise ValueError('Invalid estimate')
    return number


def discovered(card):
    # 2026-10-20: use the October 17 window rule, even for relaunched ads.
    raw = card.get('raw_text', '')
    match = re.search(r"(?<![\d-])(\d{4}-\d{2}-\d{2})\s*[~～至]\s*(\d{4}-\d{2}-\d{2})(?!\d)", raw)
    if not match:
        raise ValueError(f"Missing visible detail date range: {card.get('ad_id')}")
    if date.fromisoformat(match[1]) > date.fromisoformat(match[2]):
        raise ValueError("Invalid visible detail date range")
    value = match[1]
    date.fromisoformat(value)
    if card.get('first_discovered_date') not in (None, value):
        raise ValueError('First-discovered date contradicts visible detail text')
    return value


def is_relaunch(card):
    """Only the standalone visible UI badge is evidence; never title substring."""
    return bool(re.search(r'(?m)^\s*重投\s*$', card.get('raw_text', '')))


def rank(cards, reviews, target, ranking=None):
    candidates, counts = [], {'raw': len(cards), 'other_date': 0, 'talking_head': 0, 'relaunch': 0}
    identities = set()
    for card in cards:
        key = (card['platform'], card['app_id'], card['ad_id'])
        if key in identities:
            raise ValueError('Duplicate App/platform/ad identity')
        identities.add(key)
        first = discovered(card)
        if first != target:
            counts['other_date'] += 1
            continue
        if is_relaunch(card):
            counts['relaunch'] += 1
            continue
        review = reviews[key]
        oral = review.get('checks', {}).get('real_person_talking_head')
        if oral is True and review.get('decision') == 'exclude':
            counts['talking_head'] += 1
            continue
        if review.get('decision') != 'keep' or oral is not False:
            raise ValueError(f'Unresolved content eligibility for target-day video: {card["ad_id"]}')
        val = estimate(card.get('impressions_estimate'))
        candidates.append({**card, 'first_discovered_date': first, 'estimate_number': float(val),
                           'review_reason': review['reason'], 'review_method': review['method'],
                           '_estimate': val})
    candidates.sort(key=lambda c: (-c['_estimate'], c['app_name'], c['platform'], c['app_id'], c['ad_id']))
    from ua_workflows.social_chat_ranking import count as selection_count, rule as ranking_rule
    count = selection_count(len(candidates), ranking)
    for c in candidates:
        del c['_estimate']
    return {'target_date': target, 'counts': {**counts, 'eligible': len(candidates), 'selected': count},
            'ranking': ranking_rule(ranking), 'tie_policy': 'app_name/platform/app_id/ad_id',
            'items': [{**c, 'global_rank': i} for i, c in enumerate(candidates[:count], 1)]}


def select(run_dir, config):
    run_dir = Path(run_dir).resolve()
    scope = load(run_dir / 'scope.json')
    target = scope['target_date']
    expected = tasks(config)
    if [(t['app_name'], t['platform']) for t in scope['tasks']] != [(t['app_name'], t['platform']) for t in expected]:
        raise ValueError('Run must cover the complete confirmed competitor list in order.')
    required_filters = {**config['filters'], 'date': {'mode': 'custom', 'start': target, 'end': target}}
    if scope['filters'] != required_filters or scope['content_checks'] != config['content_checks'] or scope['date_semantics'] != config['date_semantics']:
        raise ValueError('Run filters/content/date semantics differ from this daily workflow.')
    state = fsm().inspect(run_dir)
    for task_state in state['tasks']:
        if 'REVIEW_COMPLETE' not in task_state['passed']:
            raise ValueError(f"{task_state['task_id']} not ready: {task_state['state']} / {task_state.get('gate_failure')}")
    cards, reviews = [], {}
    for task in scope['tasks']:
        root = run_dir / 'tasks' / task['id']
        task_cards = [load(f) for f in sorted((root / 'cards').glob('*.json'))]
        by_id = {r['ad_id']: r for r in load(root / 'reviews.json')['items']}
        for card in task_cards:
            cards.append(card)
            reviews[(card['platform'], card['app_id'], card['ad_id'])] = by_id[card['ad_id']]
    result = rank(cards, reviews, target, scope.get('ranking'))
    result.update({'scope_sha256': state['scope_sha256'], 'generated_at': datetime.now(TZ).isoformat(), 'run_dir': str(run_dir), 'task_count': len(expected), 'status': 'ready'})
    save(run_dir / 'selection.json', result)
    return result


class Feishu:
    def __init__(self):
        import requests
        from ua_workflows.shared.config import project_env_values
        values = project_env_values()
        self.session = requests.Session()
        response = self.session.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal', json={'app_id': values.get('FEISHU_APP_ID'), 'app_secret': values.get('FEISHU_APP_SECRET')}, timeout=20)
        response.raise_for_status()
        body = response.json()
        if body.get('code') != 0:
            raise ValueError(f'Feishu auth failed: code={body.get("code")}')
        self.session.headers['Authorization'] = 'Bearer ' + body['tenant_access_token']

    def call(self, method, route, **kwargs):
        response = self.session.request(method, 'https://open.feishu.cn/open-apis/' + route, timeout=30, **kwargs)
        data = response.json()
        if not response.ok or data.get('code') != 0:
            # No tokens or credentials in diagnostic output.
            raise ValueError(f'Feishu {method} {route}: HTTP {response.status_code}, code={data.get("code")}, msg={data.get("msg")}, permissions={(data.get("error") or {}).get("permission_violations")}')
        return data.get('data') or {}

    def listing(self, route):
        items, cursor = [], None
        while True:
            params = {'page_size': 100}
            if cursor:
                params['page_token'] = cursor
            page = self.call('GET', route, params=params)
            items.extend(page.get('items', []))
            if not page.get('has_more'):
                return items
            new = page.get('page_token')
            if not new or new == cursor:
                raise ValueError('Feishu pagination did not advance')
            cursor = new


def table_route(config):
    f = config['feishu']
    return f'bitable/v1/apps/{f["app_token"]}/tables/{f["table_id"]}'


def table_url(config):
    return config['feishu']['base_url'] + '?table=' + config['feishu']['table_id']


def provision(config, client):
    route = table_route(config)
    existing = {f['field_name']: f for f in client.listing(route + '/fields')}
    for name, kind in FIELDS:
        if name in existing:
            if existing[name]['type'] != kind:
                raise ValueError('Unexpected remote field type: ' + name)
            continue
        client.call('POST', route + '/fields', json={'field_name': name, 'type': kind})
    actual = {f['field_name']: f for f in client.listing(route + '/fields')}
    if any(name not in actual or actual[name]['type'] != kind for name, kind in FIELDS):
        raise ValueError('Remote field verification failed')
    # Prove record-list permission without inserting synthetic records.
    existing_records = client.listing(route + '/records')
    return {'status': 'ready', 'url': table_url(config), 'fields': len(FIELDS), 'existing_records': len(existing_records)}


def unique_key(target, card):
    return '|'.join([target, card['platform'], card['app_id'], card['ad_id']])


def record_fields(selection, card):
    return {'appid': card['app_id'],
            '目标日期': selection['target_date'], '首次发现日期': card['first_discovered_date'],
            '广告主': card['app_name'], '平台': '广大大', 'App ID': card['app_id'],
            '广告ID': card['ad_id'], '展示估值': card['estimate_number'],
            '展示估值原文': str(card['impressions_estimate']),
            '当前入选': True, '来源': '每日最新', '投放渠道': [card['channel']] if card.get('channel') else [], '投放地区': card['regions'],
            '文案语言': [card['language']] if card.get('language') else [], '素材尺寸': card.get('dimensions') or '',
            '视频链接': {'text': '查看视频', 'link': card['video_url']},
            '广大大详情': {'text': '查看详情', 'link': card['detail_url']},
            '非真人口播判断': '非真人口播（画面初筛）', '审核说明': card['review_reason'],
            '审核方式': card['review_method'] + '；六帧已查看，未完整审听',
            '采集批次': selection['scope_sha256'], '唯一键': unique_key(selection['target_date'], card),
            '同步时间': selection['generated_at']}


def text_field(value):
    if isinstance(value, list):
        return ''.join(x.get('text', '') if isinstance(x, dict) else str(x) for x in value)
    return value or ''


def verify_rows(rows, desired):
    """Verify remote values, not merely that record IDs were returned."""
    by_key = {}
    for row in rows:
        key = text_field(row['fields'].get('唯一键'))
        if key in by_key and key:
            raise ValueError('Duplicate remote unique key on readback: ' + key)
        by_key[key] = row['fields']
    for fields in desired:
        actual = by_key.get(fields['唯一键'], {})
        for name, kind in FIELDS:
            expected, value = fields[name], actual.get(name)
            if kind in (1, 3):
                same = text_field(value) == expected
            elif kind == 4:
                same = isinstance(value, list) and sorted(value) == sorted(expected)
            elif kind == 15:
                same = isinstance(value, dict) and value.get('link') == expected['link']
            elif kind == 2:
                # Bitable also returns number fields as decimal strings.
                try:
                    number = float(value) if type(value) in (float, int, str) else float('nan')
                    same = math.isfinite(number) and math.isclose(number, expected, rel_tol=1e-12)
                except (ValueError, OverflowError):
                    same = False
            else:
                same = value is expected
            if not same:
                raise ValueError('Remote field readback mismatch: ' + name + ' / ' + fields['唯一键'])


def sync(selection, config, client):
    route = table_route(config)
    existing, all_rows = {}, client.listing(route + '/records')
    for row in all_rows:
        key = text_field(row['fields'].get('唯一键'))
        if key:
            if key in existing:
                raise ValueError('Duplicate remote unique key; reconcile before syncing: ' + key)
            existing[key] = row
    desired = [record_fields(selection, c) for c in selection['items']]
    creates = [{'fields': f} for f in desired if f['唯一键'] not in existing]
    updates = [{'record_id': existing[f['唯一键']]['record_id'], 'fields': f} for f in desired if f['唯一键'] in existing]
    for start in range(0, len(creates), 100):
        chunk = creates[start:start + 100]
        digest = hashlib.sha256(json.dumps(chunk, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        # Feishu requires UUID v4 syntax; preserve a stable key for an identical batch.
        client.call('POST', route + '/records/batch_create', params={'client_token': str(uuid.UUID(hex=digest[:32], version=4))}, json={'records': chunk})
    for start in range(0, len(updates), 100):
        client.call('POST', route + '/records/batch_update', json={'records': updates[start:start + 100]})
    # Read back all new selections before retiring the previous selection for this day.
    desired_keys = {f['唯一键'] for f in desired}
    verified = client.listing(route + '/records')
    verify_rows(verified, desired)
    retire = [{'record_id': r['record_id'], 'fields': {'当前入选': False}} for r in verified
              if text_field(r['fields'].get('目标日期')) == selection['target_date']
              and r['fields'].get('当前入选') is True and text_field(r['fields'].get('唯一键')) not in desired_keys]
    for start in range(0, len(retire), 100):
        client.call('POST', route + '/records/batch_update', json={'records': retire[start:start + 100]})
    final = client.listing(route + '/records')
    verify_rows(final, desired)
    active = [r for r in final if text_field(r['fields'].get('目标日期')) == selection['target_date'] and r['fields'].get('当前入选') is True]
    if {text_field(r['fields'].get('唯一键')) for r in active} != desired_keys or len(active) != len(desired_keys):
        raise ValueError('Remote active selection does not match expected top 10%')
    return {'status': 'synced', 'created': len(creates), 'updated': len(updates), 'retired': len(retire), 'verified_active': len(active), 'url': table_url(config)}


def main():
    parser = argparse.ArgumentParser(description='社交竞品浏览器日采集：手动准备、全局Top10%和飞书同步')
    parser.add_argument('command', choices=['prepare', 'select', 'sync', 'provision'])
    parser.add_argument('--date', default='')
    parser.add_argument('--execution-origin', choices=['manual','scheduled'], default='manual', help='本次真实触发来源；仅定时触发传scheduled')
    parser.add_argument('--run-dir', type=Path)
    parser.add_argument('--apply', action='store_true', help='实际写飞书；默认只生成本地待写入包')
    args = parser.parse_args()
    config = load(CONFIG)
    target = args.date or yesterday()
    run_dir = (args.run_dir or ROOT / 'data/social_chat_browser_daily_latest' / target).resolve()
    with collection_phase(run_dir, 'social_chat_browser_daily'):
        if args.command == 'prepare':
            result = prepare(target, run_dir, config, args.execution_origin)
        elif args.command == 'provision':
            result = provision(config, Feishu()) if args.apply else {'status': 'dry_run', 'url': table_url(config), 'fields': FIELDS}
        else:
            selection = select(run_dir, config)  # Always recompute; never trust stale selection.json.
            if args.command == 'select':
                result = {k: v for k, v in selection.items() if k != 'items'}
            elif args.apply:
                result = sync(selection, config, Feishu())
                save(run_dir / 'sync-result.json', result)
            else:
                payload = {'url': table_url(config), 'records': [{'fields': record_fields(selection, c)} for c in selection['items']]}
                save(run_dir / 'feishu-pending.json', payload)
                result = {'status': 'dry_run', 'selected': len(selection['items']), 'payload': str(run_dir / 'feishu-pending.json')}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError) as exc:
        print(json.dumps({'status': 'blocked', 'error': str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)
