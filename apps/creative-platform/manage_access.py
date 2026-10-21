#!/usr/bin/env python3
"""Local operator-only allowlist maintenance; never trusts an email/display name."""

import argparse, json, os, tempfile, uuid
from pathlib import Path

DEFAULT = Path(__file__).resolve().parents[2] / "data/creative_platform/sso.json"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["allow", "revoke", "admin", "member", "list"])
    p.add_argument("subject", nargs="?")
    p.add_argument("--config", type=Path, default=DEFAULT)
    a = p.parse_args()
    c = json.loads(a.config.read_text())
    if a.action == "list":
        print(
            json.dumps(
                {k: c[k] for k in ["allowed_subjects", "admin_subjects"]}, indent=2
            )
        )
        return
    try:
        sub = str(uuid.UUID(a.subject))
    except (ValueError, TypeError, AttributeError):
        p.error("请输入已核实的Guru subject UUID，不接受姓名或邮箱")
    allowed = set(c["allowed_subjects"])
    admins = set(c["admin_subjects"])
    if a.action == "allow":
        allowed.add(sub)
    if a.action == "admin":
        allowed.add(sub)
        admins.add(sub)
    if a.action == "member":
        admins.discard(sub)
    if a.action == "revoke":
        allowed.discard(sub)
        admins.discard(sub)
    if not admins:
        p.error("不能移除最后一名管理员")
    c["allowed_subjects"] = sorted(allowed)
    c["admin_subjects"] = sorted(admins)
    fd, name = tempfile.mkstemp(dir=a.config.parent, prefix=".sso-config-")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(c, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, a.config)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    print("访问配置已更新；重启本机服务生效。")


if __name__ == "__main__":
    main()
