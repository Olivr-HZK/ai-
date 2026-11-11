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
                                      'download-collected', 'prepare', 'pre-review'])
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
    return p

def main(argv=None):
    p = parser(); a = p.parse_args(argv)
    if a.command == 'doctor':
        print(json.dumps({'project': str(ROOT), 'crawler_python_exists': a.crawler_python.is_file(),
                          'credentials_file_present': a.credentials_env.is_file(),
                          'modules': {n: importlib.util.find_spec(n) is not None for n in
                                      ('requests', 'dotenv', 'numpy', 'PIL', 'torch', 'sentence_transformers')},
                          'browser_started': False, 'business_requests_sent': False}, ensure_ascii=False, indent=2))
        return 0
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
    if a.command in ('crawl', 'resume-crawl', 'download-collected'):
        presets = daily_presets(a, load(ROOT / 'config/social_chat_browser_daily.json'))
        scope = selected_presets(presets)
        mode = {'crawl': 'run', 'resume-crawl': 'resume', 'download-collected': 'download-collected'}[a.command]
        return subprocess.run([str(a.crawler_python.expanduser()), '-m', 'ua_workflows.guangdada_direct', mode,
                               '--date', a.date, '--state-dir', str(a.state_dir.expanduser()),
                               '--credentials-env', str(a.credentials_env.expanduser()),
                               '--presets', *(p['key'] for p in scope)], cwd=ROOT).returncode
    if a.command == 'prepare':
        from .social_chat_plugin_handoff import prepare
        config = load(ROOT / 'config/social_chat_browser_daily.json')
        presets = daily_presets(a, config)
        packages = a.source_zip or collected_archives(a.state_dir, a.date, presets)
        print(json.dumps(prepare(a.date, run_dir, config, packages), ensure_ascii=False, indent=2)); return 0
    if not a.media_index or not a.embedding_cache:
        p.error('pre-review需要--media-index和--embedding-cache，不推断审核结论')
    scope = load(run_dir / 'scope.json'); handoff = load(run_dir / 'handoff.json')
    if scope.get('target_date') != a.date or handoff.get('status') != 'PACKAGE_RECEIVED':
        raise ValueError('尚未登记匹配目标日原包')
    from .social_chat_plugin_review_plan import prepare_queue
    result = prepare_queue(a.date, a.media_index, a.embedding_cache, run_dir / 'pre-review/plan.json',
                           scope['processing_policy'], a.history_vector_pack,
                           orientation_policy=scope.get('media_orientation_policy'))
    print(json.dumps({'status': result['status'], 'review_queue': result['counts'].get('review_pending', 0),
                      'review_certified': result['review_certified']}, ensure_ascii=False)); return 0

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError) as error:
        print('流程入口未继续：' + str(error)); raise SystemExit(2)
