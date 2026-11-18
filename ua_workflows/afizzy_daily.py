"""Mac mini handoff entry points. Never certify visual review or auto-write tables."""
from __future__ import annotations
import argparse
from datetime import date, timedelta
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess

from .guangdada_headless_ui import AVAILABLE_PRESETS, preset_scope_suffix, selected_presets

ROOT = Path(__file__).resolve().parents[1]

def target_date(value):
    try:
        if date.fromisoformat(value).isoformat() != value:
            raise ValueError()
    except ValueError:
        raise argparse.ArgumentTypeError('日期须为YYYY-MM-DD') from None
    return value

def load(path):
    return json.loads(Path(path).read_text())

def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def crawler_python():
    configured = os.environ.get('AFIZZY_CRAWLER_PYTHON')
    if configured:
        return Path(configured).expanduser()
    deployed = Path.home() / 'oliver/guangdada-direct-0.2.1/.venv/bin/python'
    return deployed if deployed.is_file() else ROOT / '.venv/bin/python'

def daily_presets(args, config):
    configured = config['daily_workflow']['presets']
    if args.presets or args.command not in ('resume-crawl', 'download-collected', 'prepare') or args.source_zip:
        return args.presets or configured
    runs = args.state_dir.expanduser() / 'runs'
    candidates = []
    for directory in runs.glob(f'{args.date}_*'):
        suffix = directory.name.removeprefix(f'{args.date}_')
        if '_sample' in suffix or not directory.is_dir():
            continue
        keys = ['game', 'tool'] if suffix == 'both' else suffix.split('_')
        try:
            scope = selected_presets(keys)
        except ValueError:
            continue
        if preset_scope_suffix(scope) == suffix and any((directory / f'{key}.json').is_file() for key in keys):
            candidates.append(keys)
    if len(candidates) > 1:
        raise ValueError('目标日有多个采集范围，请用 --presets 指定原范围')
    return candidates[0] if candidates else configured

def collected_archives(private, target, presets=None):
    """Only use each group's latest confirmed delivery, never glob old retries."""
    scope = selected_presets(presets)
    directory = Path(private).expanduser() / 'runs' / f'{target}_{preset_scope_suffix(scope)}'
    result = []
    for preset in scope:
        group = preset['key']
        path = directory / (group + '.json')
        if not path.is_file():
            raise ValueError(f'缺少 {group} 采集断点，请核对 --presets 或显式传 --source-zip')
        state = load(path)
        if (state.get('date') != target or state.get('preset', {}).get('key') != group
                or state.get('sample_limit') or state.get('stop_reason') not in ('date-cutoff', 'last-page')):
            raise ValueError('日期/预设或完整采集范围未确认，未作为整日包交接')
        archive = state.get('archive') or {}
        p = Path(archive.get('path', '/nonexistent'))
        if not p.is_file() or p.stat().st_size != archive.get('bytes') or sha(p) != archive.get('sha256'):
            raise ValueError('采集包缺失或哈希变化，未自动改用旧包')
        result.append(p.resolve())
    return result

def task_text(target, run_dir, scope):
    previous = (date.fromisoformat(target) - timedelta(days=1)).isoformat()
    preset_args = ['--presets', *(p['key'] for p in scope)]
    preset_labels = '、'.join(f"{p['category']}「{p['preset']}」" for p in scope)
    command = shlex.join([str(ROOT / 'scripts/afizzy-daily'), 'prepare', '--date', target,
                          '--run-dir', str(run_dir), *preset_args])
    return f'''# Afizzy {target} · Mac mini执行任务

在这台Mac mini执行，读取docs/mac-mini-afizzy-runbook.md、docs/afizzy-material-workflow.md及skills/social-chat-browser-daily/SKILL.md。
按用户确认顺序：独立无头爬取→筛选→合格原素材入表并独立读回→待用户手动触发复刻。

1. 先检查原断点和原包；已有指定日成果先复用，不重跑本地已完成的采集。需要采集才执行scripts/afizzy-daily crawl --date {target} {shlex.join(preset_args)}。
   依次选择{preset_labels}；每组应用预设后均重设网站开始/结束日期为{target}，选择最新创意。禁用扩展，先保存地址，再统一下载。
2. {command}
   prepare只登记原包及scope，不表示媒体或内容审核通过。逐卡核对原报告、元数据与媒体对应；失败单列，重投只认明确标签。外链无法解析无需追补；CDN404与外链失败分开记录。
3. 全部已下载非重投视频/图片先检查原文件显示尺寸及SHA256，计入图片EXIF和视频旋转/SAR；横屏排除、尺寸未知暂缓。只对通过项做同App/系统CLIP0.9日内去重、前7自然日去重；缺历史和身份留账。生成标准化media-index.json后使用pre-review入口，沿用scope固定的尺寸规则；完整视频解码、六帧及内容审核只处理后续合格队列。
4. 对固定剩余队列按产品实际查看一套六帧/全部原图，同时审核男性有意义展示、女频恋爱/陪伴关联、无真人主持式推广口播、风格与广告模板。存疑不纳入。
   对照{previous}同产品/系统完整已确认模板库。新scope固定飞书备用来源时，按content-review.md补取缺失原件或查找前日表内候选。确认同模板剔除当天项，覆盖不全且无同模板匹配仍待核。不能用CLIP或旧试算冒充模板/内容审核。
5. 合格同模板组仅留最高展示估值者，全部合格项排序，不截前100/前10%。依据实际主要画面文案及完整音轨识别给每项一个主语言，保留证据。
6. 审核覆盖及语言证据完成后，按唯一键上传原附件到既有竞品素材字段，核对字段/token/名称/精确大小，保护历史/手动记录。没有审核结果不写表；脚本不会替代这一步认证。
7. 原素材独立核验入表后结束本次流程，保存本批record_ids、源SHA256和视频/图片待复刻清单，状态记awaiting_manual_trigger；不自动上传Replica或调用图片生成。
   用户明确指定批次、日期或记录并说开始复刻后，再单独启动：视频按replica-studio-cli.md使用现有CLI智能Logo流程，图片按image-replication.md使用内置image_gen；Playable暂不处理，翻译需明确语言需求。
   手动启动时视频prepare须限定已核验回执中的record_ids和expected_count；随后自动排队、查询、下载核验及回填同一记录，不为每条重复询问。未知付费动作只读核对、不重发，保留原附件。
8. 报告实际采集/失败、重投、去重、审核、入选、入表数量、待手动触发复刻数量及结果包/飞书链接。分别记录阶段完成，不发群、不建定时，不把待触发、排队或已有ZIP当复刻成功。
'''

def parser():
    p = argparse.ArgumentParser(description='Afizzy Mac mini流程入口；task/doctor/status只读，筛选需实际证据')
    p.add_argument('command', choices=['task', 'doctor', 'status', 'crawl', 'resume-crawl',
                                      'download-collected', 'prepare', 'pre-review', 'embed',
                                      'stage', 'heartbeat', 'check', 'watchdog', 'daily'])
    p.add_argument('--date', type=target_date)
    p.add_argument('--presets', nargs='+', choices=[p['key'] for p in AVAILABLE_PRESETS],
                   help='默认使用每日预设；恢复旧批次时传入原范围')
    p.add_argument('--run-dir', type=Path)
    p.add_argument('--state-dir', type=Path, default=Path.home() / '.local/share/gdd-direct')
    p.add_argument('--credentials-env', type=Path, default=ROOT / '.env')
    p.add_argument('--crawler-python', type=Path, default=crawler_python())
    p.add_argument('--source-zip', type=Path, action='append', default=[])
    p.add_argument('--media-index', type=Path)
    p.add_argument('--embedding-cache', type=Path)
    p.add_argument('--history-vector-pack', type=Path)
    p.add_argument('--model-provenance', type=Path, help='embed：钉住模型来源的provenance JSON')
    p.add_argument('--limit', type=int, help='embed：本次最多补算多少条')
    p.add_argument('--batch-size', type=int, default=16, help='embed：每批编码张数')
    p.add_argument('--device', help='embed：sentence-transformers设备，默认mps')
    p.add_argument('--dry-run', action='store_true', help='embed：只统计缺口，不加载模型')
    p.add_argument('--stage', choices=['crawl', 'prepare', 'embed', 'pre-review', 'joint-review',
                                       'select', 'sync', 'report'], help='stage：阶段名')
    p.add_argument('--status', choices=['running', 'done', 'failed', 'started'], help='stage/heartbeat：状态')
    p.add_argument('--note', help='stage/heartbeat：原因或备注')
    p.add_argument('--max-age', type=int, default=120, help='check/watchdog：允许的最长无更新分钟数')
    p.add_argument('--notify', action='store_true', help='check/watchdog：配置了告警webhook时发送')
    p.add_argument('--backfill', action='store_true', help='watchdog：缺失时补跑采集与登记')
    return p

def run_crawl(a, mode='run'):
    """Run the collection CLI; separated so tests never spawn a browser entry."""
    presets = daily_presets(a, load(ROOT / 'config/social_chat_browser_daily.json'))
    scope = selected_presets(presets)
    return subprocess.run([str(a.crawler_python.expanduser()), '-m', 'ua_workflows.guangdada_direct', mode,
                           '--date', a.date, '--state-dir', str(a.state_dir.expanduser()),
                           '--credentials-env', str(a.credentials_env.expanduser()),
                           '--presets', *(p['key'] for p in scope)], cwd=ROOT).returncode


def mark_stage(run_dir, stage, status, *, note=None, target_date=None):
    if run_dir is None:
        return
    try:
        from .social_chat_run_state import mark
        mark(run_dir, stage, status, note=note, target_date=target_date)
    except Exception:
        pass


def default_embedding_cache():
    """Stable cache location; the historical per-run path is kept as the default."""
    from .shared.config import DATA_DIR
    return DATA_DIR / 'social_chat_plugin_reprocess_v2' / 'embedding-cache'


def _embed(a, parser_, run_dir=None):
    from .social_chat_clip_embed import (collect_inputs, embed_missing, encoder_for, missing,
                                         provenance_from_bundle, provenance_from_file, verify_model)
    if not a.media_index:
        parser_.error('embed需要--media-index')
    cache = (a.embedding_cache or default_embedding_cache()).expanduser()
    inputs = collect_inputs(a.media_index)
    pending = missing(inputs, cache)
    summary = {'status': 'dry_run' if a.dry_run else None, 'unique': len(inputs),
               'pending': len(pending), 'cache': str(cache)}
    if a.dry_run or not pending:
        if not a.dry_run:
            summary['status'] = 'complete'
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    pinned = (provenance_from_bundle(a.history_vector_pack) if a.history_vector_pack
              else provenance_from_file(a.model_provenance) if a.model_provenance else None)
    if pinned is None:
        parser_.error('embed需要--history-vector-pack或--model-provenance钉住同一CLIP模型')
    mark_stage(run_dir, 'embed', 'running', target_date=a.date)
    snapshot = verify_model(pinned)
    encode = encoder_for(snapshot, a.device)
    try:
        result = embed_missing(a.media_index, cache, pinned['fingerprint'], encode,
                               batch_size=a.batch_size, limit=a.limit,
                               progress=lambda done, total: print(f'encoded {done} / {total}', flush=True))
    except Exception as error:
        mark_stage(run_dir, 'embed', 'failed', note=type(error).__name__, target_date=a.date)
        raise
    mark_stage(run_dir, 'embed', 'done', target_date=a.date)
    summary.update(result, status='complete')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def main(argv=None):
    p = parser(); a = p.parse_args(argv)
    if a.command == 'doctor':
        print(json.dumps({'project': str(ROOT), 'crawler_python_exists': a.crawler_python.is_file(),
                          'credentials_file_present': a.credentials_env.is_file(),
                          'modules': {n: importlib.util.find_spec(n) is not None for n in
                                      ('requests', 'dotenv', 'numpy', 'PIL', 'torch', 'sentence_transformers')},
                          'browser_started': False, 'business_requests_sent': False}, ensure_ascii=False, indent=2))
        return 0
    if a.command == 'embed':
        embed_dir = (a.run_dir.expanduser().resolve() if a.run_dir else
                     (ROOT / 'data/afizzy_daily' / a.date).resolve() if a.date else None)
        return _embed(a, p, embed_dir)
    if not a.date:
        p.error('此命令需要显式--date，不按运行日猜目标日')
    run_dir = (a.run_dir or ROOT / 'data/social_chat_plugin_handoff' / a.date).expanduser().resolve()
    if a.command == 'task':
        presets = daily_presets(a, load(ROOT / 'config/social_chat_browser_daily.json'))
        print(task_text(a.date, run_dir, selected_presets(presets))); return 0
    if a.command == 'status':
        result = {'date': a.date, 'run_dir': str(run_dir), 'artifacts': {}}
        for name in ('scope.json', 'handoff.json', 'pre-review/plan.json', 'selection.json',
                     'feishu-sync.json', 'replication/verification.json', 'execution-control.json'):
            path = run_dir / name
            if path.is_file():
                v = load(path)
                observed = v.get('target_date', v.get('date'))
                if observed and observed != a.date:
                    raise ValueError('运行目录日期与请求不符')
                result['artifacts'][name] = {k: v[k] for k in
                    ('status', 'target_date', 'review_status', 'review_certified', 'selected_count', 'verified_active') if k in v}
        print(json.dumps(result, ensure_ascii=False, indent=2)); return 0
    daily_dir = (a.run_dir or (ROOT / 'data/afizzy_daily' / a.date)).expanduser().resolve()
    if a.command == 'stage':
        from .social_chat_run_state import mark
        if not a.stage or not a.status:
            p.error('stage需要--stage与--status')
        control = mark(daily_dir, a.stage, a.status, note=a.note, target_date=a.date)
        print(json.dumps({'run_dir': str(daily_dir), 'stage': a.stage,
                          'entry': control['stages'][a.stage]}, ensure_ascii=False, indent=2))
        return 0
    if a.command == 'heartbeat':
        from .social_chat_run_state import heartbeat
        if a.status not in ('started', 'done', 'failed'):
            p.error('heartbeat的--status须为started/done/failed')
        print(json.dumps(heartbeat(daily_dir, status=a.status, stage=a.stage, note=a.note, target_date=a.date),
                         ensure_ascii=False, indent=2))
        return 0
    if a.command in ('check', 'watchdog'):
        from .social_chat_run_state import check_daily, notify, project_values, render_alert
        decision = check_daily(daily_dir, max_age_minutes=a.max_age, target_date=a.date)
        summary = {'run_dir': str(daily_dir), 'decision': decision}
        if a.command == 'watchdog' and decision['status'] in ('missing', 'failed', 'stalled'):
            summary['alert'] = render_alert(decision)
            if a.backfill and decision['status'] == 'missing':
                from .social_chat_run_state import heartbeat as write_heartbeat
                write_heartbeat(daily_dir, status='started', stage='crawl', note='watchdog backfill', target_date=a.date)
                code = main(['crawl', '--date', a.date, '--run-dir', str(daily_dir),
                             '--crawler-python', str(a.crawler_python), '--state-dir', str(a.state_dir),
                             '--credentials-env', str(a.credentials_env)])
                summary['backfill'] = {'crawl_returncode': code}
        if a.notify:
            summary['notify'] = notify(summary.get('alert') or render_alert(decision),
                                       values=project_values())
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if decision['status'] in ('running', 'done') else 2
    if a.command == 'daily':
        from .social_chat_run_state import heartbeat as write_heartbeat, mark
        write_heartbeat(daily_dir, status='started', stage='crawl', target_date=a.date)
        mark(daily_dir, 'crawl', 'running', target_date=a.date)
        try:
            code = run_crawl(a)
        except Exception as error:
            mark(daily_dir, 'crawl', 'failed', note=type(error).__name__, target_date=a.date)
            write_heartbeat(daily_dir, status='failed', stage='crawl',
                            note=type(error).__name__, target_date=a.date)
            raise
        if code:
            mark(daily_dir, 'crawl', 'failed', note=f'crawl exit {code}', target_date=a.date)
            write_heartbeat(daily_dir, status='failed', stage='crawl',
                            note=f'crawl exit {code}', target_date=a.date)
            return code
        mark(daily_dir, 'crawl', 'done', target_date=a.date)
        write_heartbeat(daily_dir, status='done', stage='crawl', target_date=a.date)
        return 0
    if a.command in ('crawl', 'resume-crawl', 'download-collected'):
        return run_crawl(a, {'crawl': 'run', 'resume-crawl': 'resume',
                             'download-collected': 'download-collected'}[a.command])
    if a.command == 'prepare':
        from .social_chat_plugin_handoff import prepare
        config = load(ROOT / 'config/social_chat_browser_daily.json')
        presets = daily_presets(a, config)
        packages = a.source_zip or collected_archives(a.state_dir, a.date, presets)
        mark_stage(daily_dir, 'prepare', 'running', target_date=a.date)
        try:
            result = prepare(a.date, run_dir, config, packages)
        except Exception as error:
            mark_stage(daily_dir, 'prepare', 'failed', note=type(error).__name__, target_date=a.date)
            raise
        mark_stage(daily_dir, 'prepare', 'done', target_date=a.date)
        print(json.dumps(result, ensure_ascii=False, indent=2)); return 0
    if not a.media_index or not a.embedding_cache:
        p.error('pre-review需要--media-index和--embedding-cache，不推断审核结论')
    scope = load(run_dir / 'scope.json'); handoff = load(run_dir / 'handoff.json')
    if scope.get('target_date') != a.date or handoff.get('status') != 'PACKAGE_RECEIVED':
        raise ValueError('尚未登记匹配目标日原包')
    from .social_chat_plugin_review_plan import prepare_queue
    mark_stage(daily_dir, 'pre-review', 'running', target_date=a.date)
    try:
        result = prepare_queue(a.date, a.media_index, a.embedding_cache, run_dir / 'pre-review/plan.json',
                               scope['processing_policy'], a.history_vector_pack,
                               orientation_policy=scope.get('media_orientation_policy'))
    except Exception as error:
        mark_stage(daily_dir, 'pre-review', 'failed', note=type(error).__name__, target_date=a.date)
        raise
    mark_stage(daily_dir, 'pre-review', 'done',
               note=f'queue={result["counts"].get("review_pending", 0)}'
                    f' undeduped={len(result.get("dedupe_unavailable") or [])}', target_date=a.date)
    print(json.dumps({'status': result['status'], 'review_queue': result['counts'].get('review_pending', 0),
                      'dedupe_unavailable': len(result.get('dedupe_unavailable') or []),
                      'review_certified': result['review_certified']}, ensure_ascii=False)); return 0

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError) as error:
        print('流程入口未继续：' + str(error)); raise SystemExit(2)
