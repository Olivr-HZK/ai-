"""
简单测试脚本：调用 workflow_competitor_hot_rank，只使用前 9 个关键词。

用法（项目根目录，且已配置虚拟环境与 .env）：

  python scripts/test_workflow_competitor_hot_rank.py
"""

from workflow_competitor_hot_rank import main as run_hot_rank


def main() -> None:
    # 仅限制前 9 个关键词，方便调试人气值Top1% 选择逻辑
    import sys

    # 直接通过命令行参数转发到 workflow_competitor_hot_rank
    # 等价于：python scripts/workflow_competitor_hot_rank.py --limit-keywords 9
    sys.argv = ["workflow_competitor_hot_rank.py", "--limit-keywords", "9"]
    run_hot_rank()


if __name__ == "__main__":
    main()

