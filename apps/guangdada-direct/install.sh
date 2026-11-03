#!/bin/sh
set -eu
gdd_install_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
gdd_requirements="$gdd_install_dir/requirements-direct.txt"
if [ -d "$gdd_install_dir/ua_workflows" ]; then
  cd "$gdd_install_dir"
elif [ -d "$gdd_install_dir/../../ua_workflows" ]; then
  cd "$gdd_install_dir/../.."
else
  printf '%s\n' '找不到独立采集源码目录，请完整保留项目或交付包结构。' >&2
  exit 2
fi
gdd_python=''
for candidate in "${GDD_PYTHON:-}" /opt/homebrew/bin/python3 /usr/local/bin/python3 python3; do
  if [ -n "$candidate" ] && "$candidate" -c 'import sys; assert sys.version_info >= (3,10)' >/dev/null 2>&1; then
    gdd_python="$candidate"
    break
  fi
done
if [ -z "$gdd_python" ]; then
  printf '%s\n' '需要Python 3.10或更新版本，可用GDD_PYTHON指定解释器。' >&2
  exit 2
fi
if [ -x .venv/bin/python ] && ! .venv/bin/python -c 'import sys; assert sys.version_info >= (3,10)' >/dev/null 2>&1; then
  mv .venv ".venv-unsupported-$(date +%Y%m%d%H%M%S)"
fi
"$gdd_python" -m venv .venv
.venv/bin/python -m pip install -r "$gdd_requirements"
.venv/bin/python -m playwright install chromium
printf '%s\n' '安装完成。请确认系统有ffprobe/ffmpeg，再传指定日期及原项目.env运行。无需扩展授权。'
