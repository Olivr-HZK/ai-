from __future__ import annotations

import unittest


class VeAdBreakdownAnalysisTest(unittest.TestCase):
    def test_ve_prompt_requests_ad_breakdown_fields(self) -> None:
        from ua_workflows.video_enhancer.analyze import _ve_fixed_footer

        footer = _ve_fixed_footer()

        for label in (
            "【社会证明】",
            "【BGM/配乐】",
            "【音效】",
            "【旁白】",
            "【字幕动画/文字入场】",
            "【结尾CTA】",
        ):
            self.assertIn(label, footer)

    def test_ve_ad_breakdown_is_kept_in_local_analysis_summary(self) -> None:
        from ua_workflows.video_enhancer.analyze import (
            _build_ve_structured_analysis_summary,
            _strip_arrow2_footer_lines,
        )

        (
            cleaned,
            _tags,
            _category,
            _ad_one_liner,
            effect_one_liner,
            _play_one_liner,
            hook_one_liner,
            voiceover_script,
            risk_level,
            play_fingerprint,
            differentiator,
            template_fingerprint,
            _play_asset_id,
            play_asset_name,
            _play_asset_subtag_ids,
            play_asset_subtag_names,
            play_asset_novelty_label,
            play_asset_reason,
            ad_breakdown,
        ) = _strip_arrow2_footer_lines(
            "\n".join(
                [
                    "【Hook解析】开头用旧照破损对比制造修复期待",
                    "【脚本口播】上传老照片，等待修复生成高清结果",
                    "【核心卖点】老照片修复转高清动态视频",
                    "【社会证明】展示用户反馈截图",
                    "【BGM/配乐】温情钢琴铺底",
                    "【音效】修复完成提示音",
                    "【旁白】配音员介绍老照片修复能力",
                    "【字幕动画/文字入场】关键词逐字弹出",
                    "【结尾CTA】点击下方链接体验",
                    "【玩法指纹】老照片修复生成动态视频",
                    "【差异点】亲情回忆",
                    "【模板指纹】旧照破损开场到高清动态结果对比",
                    "【玩法资产ID】old_photo",
                    "【玩法资产名称】老照片修复",
                    "【玩法变种ID】base",
                    "【玩法变种名称】基础变体",
                    "【玩法归类】已沉淀玩法",
                    "【玩法判断理由】输入对象和输出形态都匹配老照片修复玩法",
                    "【风险标签】无明显风险",
                    "【风险等级】低风险",
                ]
            )
        )
        self.assertEqual(cleaned, "")
        self.assertEqual(ad_breakdown["社会证明"], "展示用户反馈截图")
        self.assertEqual(ad_breakdown["结尾CTA"], "点击下方链接体验")

        summary = _build_ve_structured_analysis_summary(
            hook_one_liner=hook_one_liner,
            voiceover_script=voiceover_script,
            effect_one_liner=effect_one_liner,
            play_fingerprint=play_fingerprint,
            differentiator=differentiator,
            template_fingerprint=template_fingerprint,
            play_asset_name=play_asset_name,
            play_asset_subtag_names=play_asset_subtag_names,
            play_asset_novelty_label=play_asset_novelty_label,
            play_asset_reason=play_asset_reason,
            material_tags=[],
            risk_level=risk_level,
            ad_breakdown=ad_breakdown,
        )

        self.assertIn("广告拆解：", summary)
        self.assertIn("BGM/配乐=温情钢琴铺底", summary)
        self.assertIn("字幕动画/文字入场=关键词逐字弹出", summary)


if __name__ == "__main__":
    unittest.main()
