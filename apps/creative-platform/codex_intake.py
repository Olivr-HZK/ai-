"""Bounded, tool-disabled Codex CLI text extraction using the owner's saved login."""

from __future__ import annotations
import json, os, shutil, signal, subprocess, tempfile
from pathlib import Path
from core import Problem

MODEL = "gpt-6-luna"
EFFORT = "medium"
TIMEOUT = 120
DISABLED = [
    "shell_tool",
    "shell_snapshot",
    "multi_agent",
    "multi_agent_v2",
    "apps",
    "plugins",
    "hooks",
    "computer_use",
    "in_app_browser",
    "view_image",
    "image_generation",
    "skill_search",
    "workspace_dependencies",
    "goals",
    "sleep_tool",
]


def extract(schema, instructions, payload):
    executable = shutil.which("codex")
    if not executable:
        raise Problem("本机未找到Codex CLI，请联系管理员检查安装和登录", 503)
    # No shell interpolation. User input is stdin data, never a command/flag/path.
    # Retain only the login locator and basic runtime/proxy variables, no project API keys.
    names = [
        "HOME",
        "PATH",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "CODEX_HOME",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "https_proxy",
        "http_proxy",
        "all_proxy",
        "no_proxy",
        "SSL_CERT_FILE",
    ]
    env = {k: os.environ[k] for k in names if k in os.environ}
    with tempfile.TemporaryDirectory(prefix="guru-intake-") as temp:
        root = Path(temp)
        schema_path = root / "schema.json"
        system_path = root / "instructions.txt"
        out = root / "result.json"
        schema_path.write_text(json.dumps(schema, ensure_ascii=False))
        system_path.write_text(instructions)
        args = [
            executable,
            "--no-daemon",
            "exec",
            "--ignore-user-config",
            "--ephemeral",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--model",
            MODEL,
            "--json",
            "--color",
            "never",
            "--cd",
            temp,
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(out),
        ]
        for value in [
            f'model_reasoning_effort="{EFFORT}"',
            'approval_policy="never"',
            'web_search="disabled"',
            "project_doc_max_bytes=0",
            'shell_environment_policy.inherit="none"',
            'history.persistence="none"',
            "model_instructions_file=" + json.dumps(str(system_path)),
            "log_dir=" + json.dumps(str(root / "logs")),
        ]:
            args.extend(["-c", value])
        for feature in DISABLED:
            args.extend(["--disable", feature])
        args.append("-")
        with (
            (root / "events.jsonl").open("wb") as stdout,
            (root / "stderr.log").open("wb") as stderr,
        ):
            try:
                proc = subprocess.Popen(
                    args,
                    stdin=subprocess.PIPE,
                    stdout=stdout,
                    stderr=stderr,
                    cwd=temp,
                    env=env,
                    start_new_session=True,
                )
                try:
                    proc.communicate(
                        json.dumps(payload, ensure_ascii=False).encode(),
                        timeout=TIMEOUT,
                    )
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    proc.communicate()
                    raise Problem(
                        "Codex解析超时，请重试或手动填写；尚未创建任务", 504
                    ) from None
            except OSError:
                raise Problem("Codex CLI无法启动，请联系管理员检查安装", 503) from None
        if proc.returncode != 0:
            # Do not return raw CLI diagnostics: they may contain local paths or account details.
            raise Problem(
                "Codex解析失败，请检查本机CLI登录、模型权限或使用额度；尚未创建任务",
                503,
            )
        events = root / "events.jsonl"
        if (
            not out.is_file()
            or out.stat().st_size > 262144
            or events.stat().st_size > 2097152
        ):
            raise Problem("Codex返回不完整或过大，请重试", 502)
        try:
            completed = False
            for line in events.read_text().splitlines():
                event = json.loads(line)
                kind = event.get("type", "")
                if kind in ["turn.failed", "error"]:
                    raise ValueError("failed")
                item = event.get("item", {})
                if item and item.get("type") not in ["reasoning", "agent_message"]:
                    raise ValueError("unexpected tool")
                completed = completed or kind == "turn.completed"
            if not completed:
                raise ValueError("incomplete turn")
            result = json.loads(out.read_text())
            if not isinstance(result, dict):
                raise ValueError("not object")
            return result
        except (ValueError, UnicodeError, OSError):
            raise Problem(
                "Codex输出未通过完整性校验，请重试；尚未创建任务", 502
            ) from None
