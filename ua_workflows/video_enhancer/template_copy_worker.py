"""Local worker for VE template-copy jobs."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT, load_project_env
from ua_workflows.video_enhancer.template_trigger import DEFAULT_JOB_DIR


DEFAULT_MODEL_REF_DIR = DATA_DIR / "template_model_refs"
DEFAULT_WORK_DIR = DATA_DIR / "ve_template_copy_runs"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".heic", ".tif", ".tiff"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".m4v", ".avi", ".mkv"}
SKILL_RESULT_MARKER = "VE_TEMPLATE_COPY_RESULT_JSON"
RECOGNITION_RESULT_MARKER = "VE_TEMPLATE_RECOGNITION_RESULT_JSON"
PASS_VERDICTS = {"pass", "passed", "success", "succeeded", "completed", "usable"}
FAIL_VERDICTS = {"fail", "failed", "quality_failed", "blocked", "unusable", "rejected"}
MAX_RECOGNITION_GROUPS = int(os.getenv("VE_TEMPLATE_RECOGNITION_MAX_GROUPS", "12"))


class TemplateCopyWorkerError(RuntimeError):
    """Expected worker failure with a stable code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _slug(value: Any, *, fallback: str = "job") -> str:
    text = str(value or "").strip()
    import re

    text = re.sub(r"[^0-9A-Za-z._-]+", "_", text).strip("._-")
    return text[:100] or fallback


def read_job(job_path: Path) -> dict[str, Any]:
    payload = json.loads(job_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TemplateCopyWorkerError("invalid_job", "job 文件不是 JSON 对象")
    return payload


def write_job(job_path: Path, job: dict[str, Any]) -> None:
    job_path.parent.mkdir(parents=True, exist_ok=True)
    job_path.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")


def _job_run_dir(job: dict[str, Any], *, work_dir: Path = DEFAULT_WORK_DIR) -> Path:
    return work_dir / _slug(job.get("job_id") or job.get("ad_key") or job.get("record_id"))


def _candidate_model_refs(model_ref_dir: Path) -> list[Path]:
    if not model_ref_dir.exists():
        return []
    return sorted(
        path
        for path in model_ref_dir.iterdir()
        if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
    )


def select_model_ref(model_ref_dir: Path = DEFAULT_MODEL_REF_DIR, *, preferred: str = "") -> Path:
    if preferred:
        path = Path(preferred).expanduser()
        if path.exists() and path.is_file():
            return path
        raise TemplateCopyWorkerError("missing_model_ref", f"指定模特图不存在: {path}")
    candidates = _candidate_model_refs(model_ref_dir)
    if not candidates:
        raise TemplateCopyWorkerError("missing_model_ref", f"没有可用模特图: {model_ref_dir}")
    return candidates[0]


def _guess_extension_from_url(url: str, *, default: str = ".bin") -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in IMAGE_EXTENSIONS or suffix in VIDEO_EXTENSIONS:
        return suffix
    content_type = mimetypes.guess_type(url)[0] or ""
    guessed = mimetypes.guess_extension(content_type) if content_type else ""
    return guessed or default


def _source_url(job: dict[str, Any]) -> str:
    task = job.get("task") if isinstance(job.get("task"), dict) else {}
    handoff = task.get("aigc_template_copy_input") if isinstance(task.get("aigc_template_copy_input"), dict) else {}
    return str(handoff.get("source_url") or task.get("video_url") or task.get("cover_url") or "").strip()


def materialize_source(
    job: dict[str, Any],
    *,
    run_dir: Path,
    download: bool = True,
    timeout: float = 60.0,
) -> Path:
    source = _source_url(job)
    if not source:
        raise TemplateCopyWorkerError("missing_source", "job 缺少 source_url/video_url/cover_url")
    parsed = urlparse(source)
    if parsed.scheme in {"", "file"}:
        path = Path(parsed.path if parsed.scheme == "file" else source).expanduser()
        if not path.exists() or not path.is_file():
            raise TemplateCopyWorkerError("missing_source", f"本地源素材不存在: {path}")
        return path
    if parsed.scheme not in {"http", "https"}:
        raise TemplateCopyWorkerError("unsupported_source_url", f"不支持的源素材地址: {source}")
    if not download:
        raise TemplateCopyWorkerError("source_not_local", "源素材是远程 URL，但当前关闭下载")
    run_dir.mkdir(parents=True, exist_ok=True)
    ext = _guess_extension_from_url(source, default=".mp4")
    out_path = run_dir / f"source{ext}"
    resp = requests.get(source, timeout=timeout, stream=True)
    resp.raise_for_status()
    with out_path.open("wb") as handle:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
    return out_path


def build_codex_prompt(job: dict[str, Any], *, source_path: Path, model_path: Path, output_dir: Path) -> str:
    task = job.get("task") if isinstance(job.get("task"), dict) else {}
    kind = str(task.get("suggested_template_kind") or "needs_manual_review")
    lines = [
        "Use $aigc-template-copy to process this local VE template-copy job.",
        "",
        "Important constraints:",
        "- Use the explicit local source and model image paths below; do not use Eagle/date-folder intake.",
        "- Use Video Lab CLI for image editing/video generation if generation is enabled and available.",
        "- The Video Lab CLI is already available as `video-lab` on PATH; run it directly and do not search the filesystem for the executable.",
        "- If generation is blocked, still write the prompts and segment judgment.",
        "- Treat the model image as an identity source only; if it is visibly unsuitable for the template body framing, write a blocker instead of forcing a poor generation.",
        "- For full-body dance/walking templates, require a full-body or at least half-body model/reference suitable for body pose transfer before spending credits.",
        "- Do not run image-to-video unless the generated first-frame/effect image genuinely scores 90+ against the source key frame.",
        "- If the first-frame image fails identity, pose, body framing, or source-template fidelity, stop after writing the prompt, mark the job as quality_failed, and do not create a video task.",
        "- Do not mark a job completed just because Video Lab tasks finished; completed means the artifact is usable as APP template inspiration.",
        "- Write all outputs under the requested output directory.",
        "- End your final answer with one line that starts with VE_TEMPLATE_COPY_RESULT_JSON: followed by one compact JSON object.",
        "- Required result JSON keys: status, quality_verdict, score, mode, task_ids, output_paths, reason.",
        "- Use status=completed and quality_verdict=passed only when the output is actually usable as APP template inspiration.",
        "- Use status=quality_failed when first-frame/effect quality is below 90 or source fidelity is not good enough; do not create a video task in that case.",
        "- Use status=blocked when required source/model/tool inputs are missing.",
        "",
        f"Job ID: {job.get('job_id') or ''}",
        f"Record ID: {job.get('record_id') or task.get('record_id') or ''}",
        f"Ad key: {job.get('ad_key') or task.get('ad_key') or ''}",
        f"Product: {task.get('product') or ''}",
        f"Rating: {task.get('rating_label') or task.get('rating') or ''}",
        f"Suggested template kind: {kind}",
        f"Source path: {source_path}",
        f"Model selfie path: {model_path}",
        f"Output directory: {output_dir}",
        "",
        "Useful record context:",
        f"- Core: {task.get('core') or ''}",
        f"- Hook: {task.get('hook') or ''}",
        f"- Script/voiceover: {task.get('script_or_voiceover') or ''}",
        f"- Template fingerprint: {task.get('template_fingerprint') or ''}",
        f"- Material tags: {task.get('material_tags') or ''}",
    ]
    return "\n".join(lines).strip() + "\n"


def build_recognition_codex_prompt(job: dict[str, Any], *, source_path: Path, evidence_dir: Path) -> str:
    """Build a recognition-only prompt that reuses aigc-template-copy's inspection rules."""
    task = job.get("task") if isinstance(job.get("task"), dict) else {}
    handoff = task.get("aigc_template_copy_input") if isinstance(task.get("aigc_template_copy_input"), dict) else {}
    context = {
        "job_id": job.get("job_id") or "",
        "record_id": job.get("record_id") or task.get("record_id") or "",
        "ad_key": job.get("ad_key") or task.get("ad_key") or "",
        "product": task.get("product") or "",
        "suggested_template_kind": task.get("suggested_template_kind") or handoff.get("suggested_template_kind") or "",
        "template_reason": task.get("template_reason") or "",
        "core": task.get("core") or "",
        "hook": task.get("hook") or "",
        "script_or_voiceover": task.get("script_or_voiceover") or "",
        "template_fingerprint": task.get("template_fingerprint") or "",
    }
    schema = {
        "status": "completed",
        "mode": "image_template or video_template",
        "template_type": "图片模板 or 视频模板",
        "reason": "Chinese segment judgment, including why screenshot/segment count was chosen",
        "groups": [
            {
                "kind": "image or video",
                "label": "图片复刻 1 or 视频模板 1",
                "timestamp": 1.2,
                "start_time": 1.2,
                "end_time": 5.2,
                "reason": "Chinese reason this finished result is reusable",
            }
        ],
        "excluded": [
            {
                "time_range": "0.0-0.8s",
                "reason": "UI/progress/ad/end-card/repeat/transition",
            }
        ],
    }
    lines = [
        "Use $aigc-template-copy's inspection and template-mode rules for this VE source, but run recognition only.",
        "",
        "Hard boundaries:",
        "- Do not run Video Lab, GPT Image 2, Vidu, image generation, image-to-video, Eagle import, or product generation.",
        "- Do not create prompts for generation except short judgment reasons.",
        "- Inspect the source timeline according to the skill: do not assume frame 0; identify finished result clips vs UI/progress/ad/end cards; for static montages keep every distinct reusable still variant; for dynamic result clips keep every distinct reusable video segment.",
        "- Return the exact number of reusable groups needed by the source, not a fixed count.",
        "- Prefer timestamps in seconds. For static image groups, set timestamp to the clean stable finished frame. For video groups, set timestamp to the first frame of the result segment and set start_time/end_time for the segment.",
        f"- Cap only for operational safety at {MAX_RECOGNITION_GROUPS} groups; if the source has more, keep the most reusable distinct groups and mention the cap in reason.",
        "- End your final answer with one line that starts with "
        f"{RECOGNITION_RESULT_MARKER}: followed by one compact JSON object.",
        "- Required JSON shape:",
        json.dumps(schema, ensure_ascii=False),
        "",
        f"Source path: {source_path}",
        f"Evidence/output directory for local extraction: {evidence_dir}",
        "",
        "Record context:",
        json.dumps(context, ensure_ascii=False, indent=2),
    ]
    return "\n".join(lines).strip() + "\n"


def _decode_json_at(text: str, start: int) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    snippet = text[start:].lstrip()
    if snippet.startswith(":"):
        snippet = snippet[1:].lstrip()
    payload, _ = decoder.raw_decode(snippet)
    if not isinstance(payload, dict):
        raise TemplateCopyWorkerError("invalid_skill_result", "skill 结果不是 JSON 对象")
    return payload


def parse_skill_result(text: str) -> dict[str, Any]:
    """Parse the structured result emitted by the Codex skill run."""
    text = str(text or "")
    marker_index = text.rfind(SKILL_RESULT_MARKER)
    if marker_index >= 0:
        return _decode_json_at(text, marker_index + len(SKILL_RESULT_MARKER))

    for match in reversed(list(re.finditer(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.I))):
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and ("status" in payload or "quality_verdict" in payload):
            return payload

    first_brace = text.find("{")
    if first_brace >= 0:
        try:
            return _decode_json_at(text, first_brace)
        except (json.JSONDecodeError, TemplateCopyWorkerError):
            pass
    raise TemplateCopyWorkerError("missing_skill_result", f"Codex 输出缺少 {SKILL_RESULT_MARKER} 结构化结果")


def parse_recognition_skill_result(text: str) -> dict[str, Any]:
    """Parse the recognition-only structured result emitted by a Codex skill run."""
    text = str(text or "")
    marker_index = text.rfind(RECOGNITION_RESULT_MARKER)
    if marker_index >= 0:
        return _decode_json_at(text, marker_index + len(RECOGNITION_RESULT_MARKER))
    raise TemplateCopyWorkerError("missing_recognition_result", f"Codex 输出缺少 {RECOGNITION_RESULT_MARKER} 结构化结果")


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number < 0:
        return 0.0
    return number


def _clamp_group_count(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    limit = max(1, MAX_RECOGNITION_GROUPS)
    return groups[:limit]


def normalize_recognition_skill_result(
    result: dict[str, Any],
    *,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    """Normalize Codex recognition output to a stable local extraction plan."""
    mode = str(result.get("mode") or fallback.get("mode") or "image_template").strip()
    if mode not in {"image_template", "video_template"}:
        mode = "video_template" if str(result.get("template_type") or "") == "视频模板" else str(fallback.get("mode") or "image_template")
    template_type = str(result.get("template_type") or "").strip()
    if template_type not in {"图片模板", "视频模板"}:
        template_type = "视频模板" if mode == "video_template" else "图片模板"
    reason = str(result.get("reason") or fallback.get("reason") or "").strip()

    raw_groups = result.get("groups")
    if not isinstance(raw_groups, list):
        raw_groups = []
    groups: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_groups, start=1):
        if not isinstance(raw, dict):
            continue
        kind = str(raw.get("kind") or "").strip().lower()
        if kind not in {"image", "video"}:
            kind = "video" if mode == "video_template" else "image"
        timestamp = _float_or_none(raw.get("timestamp"))
        start_time = _float_or_none(raw.get("start_time"))
        end_time = _float_or_none(raw.get("end_time"))
        if timestamp is None:
            timestamp = start_time if start_time is not None else 0.0
        if start_time is None:
            start_time = max(0.0, timestamp - (0.6 if kind == "image" else 0.0))
        if end_time is None or end_time <= start_time:
            end_time = start_time + (4.0 if kind == "video" else 2.0)
        groups.append(
            {
                "index": index,
                "kind": kind,
                "label": str(raw.get("label") or f"{'视频模板' if kind == 'video' else '图片复刻'} {index}"),
                "timestamp": float(timestamp),
                "start_time": float(start_time),
                "end_time": float(end_time),
                "reason": str(raw.get("reason") or "").strip(),
            }
        )
    if not groups:
        groups = [
            {
                "index": 1,
                "kind": "video" if mode == "video_template" else "image",
                "label": "视频模板 1" if mode == "video_template" else "图片复刻 1",
                "timestamp": 0.0,
                "start_time": 0.0,
                "end_time": 4.0,
                "reason": "未返回分组，使用源素材开头作为保底参考。",
            }
        ]
    groups = _clamp_group_count(groups)
    return {
        **fallback,
        "mode": mode,
        "template_type": template_type,
        "reason": reason,
        "skill_result": result,
        "template_groups": groups,
    }


def _score_value(result: dict[str, Any]) -> float | None:
    value = result.get("score")
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def classify_skill_result(result: dict[str, Any]) -> dict[str, str]:
    status = str(result.get("status") or "").strip().lower()
    verdict = str(result.get("quality_verdict") or result.get("verdict") or "").strip().lower()
    reason = str(result.get("reason") or result.get("message") or "").strip()
    score = _score_value(result)

    passed = verdict in PASS_VERDICTS or (status in PASS_VERDICTS and score is not None and score >= 90)
    if status in {"completed", "success", "succeeded"} and passed:
        return {"status": "completed", "state": "completed", "bitable_status": "已完成", "error": ""}

    if status == "blocked":
        return {
            "status": "failed",
            "state": "blocked",
            "bitable_status": "失败",
            "error": reason or "skill blocked",
        }

    if status == "quality_failed" or verdict in FAIL_VERDICTS or (score is not None and score < 90):
        return {
            "status": "failed",
            "state": "quality_failed",
            "bitable_status": "失败",
            "error": reason or "quality score below threshold",
        }

    if status in {"failed", "error"}:
        return {
            "status": "failed",
            "state": "failed",
            "bitable_status": "失败",
            "error": reason or "skill failed",
        }

    return {
        "status": "failed",
        "state": "invalid_skill_result",
        "bitable_status": "失败",
        "error": reason or "skill result did not declare a passing quality verdict",
    }


def build_image_editing_params(
    *,
    model_path: str,
    prompt: str,
    size: str = "720*1280",
    quality: str = "low",
) -> dict[str, Any]:
    return {
        "input_data": {
            "images": [{"path": model_path}],
            "prompt": {"mode": "direct", "text": prompt},
        },
        "generation_config": {
            "image": {
                "provider": "cms",
                "model": "gpt-image-2",
                "model_type": "wwxq",
                "size": size,
                "quality": quality,
            }
        },
        "task_config": {"execution_count": 1},
    }


def build_image_to_video_params(
    *,
    first_frame_path: str,
    prompt: str,
    duration: int = 5,
    resolution: str = "720p",
    aspect_ratio: str = "9:16",
) -> dict[str, Any]:
    return {
        "input_data": {
            "images": [{"path": first_frame_path}],
            "prompt": {"mode": "direct", "text": prompt},
        },
        "generation_config": {
            "video": {
                "provider": "cms",
                "model": "viduq3-turbo",
                "duration": int(duration),
                "resolution": resolution,
                "aspect_ratio": aspect_ratio,
                "audio": False,
            }
        },
        "task_config": {"execution_count": 1, "auto_optimize": False},
    }


def write_worker_artifacts(
    *,
    run_dir: Path,
    codex_prompt: str,
    image_params: dict[str, Any],
    video_params: dict[str, Any],
) -> dict[str, str]:
    run_dir.mkdir(parents=True, exist_ok=True)
    prompt_path = run_dir / "codex_prompt.txt"
    image_params_path = run_dir / "params.image-editing.json"
    video_params_path = run_dir / "params.image-to-video.json"
    prompt_path.write_text(codex_prompt, encoding="utf-8")
    image_params_path.write_text(json.dumps(image_params, ensure_ascii=False, indent=2), encoding="utf-8")
    video_params_path.write_text(json.dumps(video_params, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "codex_prompt_path": str(prompt_path),
        "image_params_path": str(image_params_path),
        "video_params_path": str(video_params_path),
    }


def _mark_failed(job_path: Path, job: dict[str, Any], exc: TemplateCopyWorkerError) -> None:
    runner = job.setdefault("runner", {})
    runner["state"] = exc.code
    runner["error"] = exc.message
    job["status"] = "failed"
    write_job(job_path, job)


def prepare_template_copy_job(
    job_path: Path,
    *,
    model_ref_dir: Path = DEFAULT_MODEL_REF_DIR,
    model_path: str = "",
    work_dir: Path = DEFAULT_WORK_DIR,
    download: bool = True,
    dry_run: bool = True,
) -> dict[str, Any]:
    job = read_job(job_path)
    run_dir = _job_run_dir(job, work_dir=work_dir)
    try:
        source_path = materialize_source(job, run_dir=run_dir, download=download)
        selected_model_path = select_model_ref(model_ref_dir, preferred=model_path)
    except TemplateCopyWorkerError as exc:
        _mark_failed(job_path, job, exc)
        raise

    output_dir = run_dir / "outputs"
    prompt = build_codex_prompt(job, source_path=source_path, model_path=selected_model_path, output_dir=output_dir)
    image_params = build_image_editing_params(
        model_path=str(selected_model_path),
        prompt="仅以用户上传的自拍作为身份参考。根据源素材关键帧迁移构图、姿态、灯光、环境和视觉风格，生成可用于 APP 模板的首帧/效果图。",
    )
    video_params = build_image_to_video_params(
        first_frame_path=str(output_dir / "first_frame_effect.png"),
        prompt="从首帧开始。保持人物身份、构图和视觉风格，按源素材片段复刻自然动作、表情节奏和镜头运动。",
    )
    artifacts = write_worker_artifacts(
        run_dir=run_dir,
        codex_prompt=prompt,
        image_params=image_params,
        video_params=video_params,
    )

    runner = job.setdefault("runner", {})
    runner.update(
        {
            "state": "prepared",
            "dry_run": bool(dry_run),
            "run_dir": str(run_dir),
            "source_path": str(source_path),
            "model_path": str(selected_model_path),
            "artifacts": artifacts,
        }
    )
    job["status"] = "prepared"
    write_job(job_path, job)
    return {
        "status": "prepared",
        "job_path": str(job_path),
        "run_dir": str(run_dir),
        "source_path": str(source_path),
        "model_path": str(selected_model_path),
        "codex_prompt": prompt,
        **artifacts,
    }


def _source_is_video(source_path: Path, job: dict[str, Any]) -> bool:
    if source_path.suffix.lower() in IMAGE_EXTENSIONS:
        return False
    if source_path.suffix.lower() in VIDEO_EXTENSIONS:
        return True
    task = job.get("task") if isinstance(job.get("task"), dict) else {}
    handoff = task.get("aigc_template_copy_input") if isinstance(task.get("aigc_template_copy_input"), dict) else {}
    source_kind = str(handoff.get("source_kind") or "").strip().lower()
    if source_kind == "video":
        return True
    if source_kind == "image":
        return False
    return source_path.suffix.lower() in VIDEO_EXTENSIONS


def _recognition_from_job(job: dict[str, Any], *, source_path: Path) -> dict[str, Any]:
    task = job.get("task") if isinstance(job.get("task"), dict) else {}
    suggested = str(task.get("suggested_template_kind") or "").strip()
    is_video = _source_is_video(source_path, job)
    if is_video and suggested != "image_template_candidate":
        mode = "video_template"
        template_type = "视频模板"
    else:
        mode = "image_template"
        template_type = "图片模板"
    reason = str(task.get("template_reason") or "").strip()
    if not reason:
        reason = "素材包含视频源，输出关键帧和短视频片段供模板参考。" if is_video else "素材为静态图片源，输出关键参考截图供模板参考。"
    return {
        "mode": mode,
        "template_type": template_type,
        "suggested_template_kind": suggested,
        "reason": reason,
        "source_path": str(source_path),
    }


def _run_subprocess(cmd: list[str], *, timeout_sec: int = 60) -> bool:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except Exception:
        return False
    return proc.returncode == 0


def _write_placeholder(path: Path, source_path: Path, *, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"{label}\nsource={source_path}\n".encode("utf-8"))


def _extract_reference_frame_at(
    source_path: Path,
    target_path: Path,
    *,
    is_video: bool,
    timestamp: float = 0.0,
) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if is_video:
        ffmpeg = shutil.which("ffmpeg")
        seek = max(0.0, float(timestamp or 0.0))
        if ffmpeg and _run_subprocess(
            [
                ffmpeg,
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                f"{seek:.3f}",
                "-i",
                str(source_path),
                "-frames:v",
                "1",
                str(target_path),
            ]
        ):
            return target_path
        _write_placeholder(target_path, source_path, label="reference screenshot placeholder")
        return target_path
    if source_path.suffix.lower() in IMAGE_EXTENSIONS:
        shutil.copyfile(source_path, target_path)
        return target_path
    _write_placeholder(target_path, source_path, label="reference screenshot placeholder")
    return target_path


def _extract_reference_screenshot(source_path: Path, target_path: Path, *, is_video: bool) -> Path:
    return _extract_reference_frame_at(source_path, target_path, is_video=is_video, timestamp=0.0)


def _extract_reference_video_segment_range(
    source_path: Path,
    target_path: Path,
    *,
    is_video: bool,
    start_time: float = 0.0,
    end_time: float | None = None,
) -> Path | None:
    if not is_video:
        return None
    target_path.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg = shutil.which("ffmpeg")
    start = max(0.0, float(start_time or 0.0))
    end = float(end_time) if end_time is not None else start + 4.0
    duration = max(0.5, min(8.0, end - start))
    if ffmpeg and _run_subprocess(
        [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{start:.3f}",
            "-i",
            str(source_path),
            "-t",
            f"{duration:.3f}",
            "-c",
            "copy",
            str(target_path),
        ],
        timeout_sec=120,
    ):
        return target_path
    _write_placeholder(target_path, source_path, label="reference video segment placeholder")
    return target_path


def _extract_reference_video_segment(source_path: Path, target_path: Path, *, is_video: bool) -> Path | None:
    return _extract_reference_video_segment_range(source_path, target_path, is_video=is_video, start_time=0.0, end_time=4.0)


def _run_recognition_codex_skill(
    *,
    job: dict[str, Any],
    source_path: Path,
    refs_dir: Path,
    codex_bin: str = "",
    codex_model: str = "",
    timeout_sec: int = 60 * 30,
) -> dict[str, Any]:
    prompt_path = refs_dir / "recognition_codex_prompt.txt"
    output_path = refs_dir / "recognition_codex_last_message.md"
    prompt = build_recognition_codex_prompt(job, source_path=source_path, evidence_dir=refs_dir)
    refs_dir.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt, encoding="utf-8")
    codex_result = run_codex_prompt(
        prompt_path,
        output_path=output_path,
        execute=True,
        codex_bin=codex_bin,
        codex_model=codex_model,
        timeout_sec=timeout_sec,
    )
    if codex_result.get("returncode") != 0:
        raise TemplateCopyWorkerError(
            "recognition_codex_failed",
            str(codex_result.get("stderr") or codex_result.get("stdout") or "Codex recognition failed").strip(),
        )
    text = output_path.read_text(encoding="utf-8") if output_path.exists() else ""
    parsed = parse_recognition_skill_result(text)
    return {
        "codex_exec": codex_result,
        "recognition_prompt_path": str(prompt_path),
        "recognition_output_path": str(output_path),
        "skill_result": parsed,
    }


def _extract_recognition_artifacts(
    *,
    source_path: Path,
    refs_dir: Path,
    source_is_video: bool,
    recognition: dict[str, Any],
) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    groups = recognition.get("template_groups")
    if not isinstance(groups, list) or not groups:
        groups = [
            {
                "index": 1,
                "kind": "video" if recognition.get("mode") == "video_template" else "image",
                "label": "默认参考",
                "timestamp": 0.0,
                "start_time": 0.0,
                "end_time": 4.0,
                "reason": "",
            }
        ]
    screenshots: list[str] = []
    segments: list[str] = []
    materialized_groups: list[dict[str, Any]] = []
    for fallback_index, group in enumerate(groups, start=1):
        if not isinstance(group, dict):
            continue
        index = int(group.get("index") or fallback_index)
        frame_path = refs_dir / f"reference_frame_{index:02d}.jpg"
        segment_path = refs_dir / f"reference_segment_{index:02d}.mp4"
        timestamp = float(group.get("timestamp") or 0.0)
        start_time = float(group.get("start_time") or max(0.0, timestamp - 0.6))
        end_time = float(group.get("end_time") or start_time + 4.0)
        frame = _extract_reference_frame_at(
            source_path,
            frame_path,
            is_video=source_is_video,
            timestamp=timestamp,
        )
        screenshots.append(str(frame))
        segment = _extract_reference_video_segment_range(
            source_path,
            segment_path,
            is_video=source_is_video,
            start_time=start_time,
            end_time=end_time,
        )
        if segment:
            segments.append(str(segment))
        materialized_groups.append(
            {
                **group,
                "reference_screenshot": str(frame),
                "reference_video_segment": str(segment) if segment else "",
            }
        )
    return screenshots, segments, materialized_groups


def run_template_recognition_only(
    job_path: Path,
    *,
    work_dir: Path = DEFAULT_WORK_DIR,
    download: bool = True,
    use_codex_skill: bool = False,
    codex_bin: str = "",
    codex_model: str = "",
) -> dict[str, Any]:
    """Create reference artifacts for template judgment without Video Lab generation."""
    job = read_job(job_path)
    run_dir = _job_run_dir(job, work_dir=work_dir)
    try:
        source_path = materialize_source(job, run_dir=run_dir, download=download)
    except TemplateCopyWorkerError as exc:
        _mark_failed(job_path, job, exc)
        raise

    recognition = _recognition_from_job(job, source_path=source_path)
    source_is_video = _source_is_video(source_path, job)
    refs_dir = run_dir / "recognition_refs"
    codex_artifacts: dict[str, Any] = {"skipped": True}
    if use_codex_skill:
        try:
            codex_artifacts = _run_recognition_codex_skill(
                job=job,
                source_path=source_path,
                refs_dir=refs_dir,
                codex_bin=codex_bin,
                codex_model=codex_model,
            )
            recognition = normalize_recognition_skill_result(
                codex_artifacts.get("skill_result") if isinstance(codex_artifacts.get("skill_result"), dict) else {},
                fallback=recognition,
            )
        except TemplateCopyWorkerError as exc:
            _mark_failed(job_path, job, exc)
            raise

    reference_screenshots, reference_video_segments, template_groups = _extract_recognition_artifacts(
        source_path=source_path,
        refs_dir=refs_dir,
        source_is_video=source_is_video,
        recognition=recognition,
    )
    recognition["template_groups"] = template_groups
    result_path = refs_dir / "recognition_result.json"
    result_payload = {
        "status": "completed",
        "state": "recognition_completed",
        "bitable_status": "已完成",
        "recognition": recognition,
        "reference_screenshots": reference_screenshots,
        "reference_video_segments": reference_video_segments,
        "recognition_codex": codex_artifacts,
    }
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    runner = job.setdefault("runner", {})
    artifacts = runner.setdefault("artifacts", {})
    artifacts["recognition_result_path"] = str(result_path)
    if codex_artifacts.get("recognition_prompt_path"):
        artifacts["recognition_prompt_path"] = str(codex_artifacts["recognition_prompt_path"])
    if codex_artifacts.get("recognition_output_path"):
        artifacts["recognition_output_path"] = str(codex_artifacts["recognition_output_path"])
    runner.update(
        {
            "state": "recognition_completed",
            "run_dir": str(run_dir),
            "source_path": str(source_path),
            "recognition": recognition,
            "reference_screenshots": reference_screenshots,
            "reference_video_segments": reference_video_segments,
            "recognition_codex": codex_artifacts,
        }
    )
    runner.pop("codex_exec", None)
    runner.pop("skill_result", None)
    runner.pop("error", None)
    job["status"] = "completed"
    write_job(job_path, job)
    return {
        **result_payload,
        "job_path": str(job_path),
        "run_dir": str(run_dir),
        "recognition_result_path": str(result_path),
    }


def run_codex_prompt(
    prompt_path: Path,
    *,
    output_path: Path,
    execute: bool,
    codex_bin: str = "",
    codex_model: str = "",
    timeout_sec: int = 60 * 60,
) -> dict[str, Any]:
    if not execute:
        return {"skipped": True, "reason": "dry_run"}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    codex_executable = str(codex_bin or "").strip() or shutil.which("codex") or "codex"
    cmd = build_codex_exec_command(
        codex_executable=codex_executable,
        output_path=output_path,
        codex_model=codex_model,
    )
    prompt = prompt_path.read_text(encoding="utf-8")
    proc = subprocess.run(cmd, input=prompt, text=True, capture_output=True, cwd=str(PROJECT_ROOT), timeout=timeout_sec)
    return {
        "skipped": False,
        "returncode": proc.returncode,
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
        "output_path": str(output_path),
    }


def normalize_codex_model_name(codex_model: str) -> str:
    """Normalize common human shorthand to Codex CLI model identifiers."""
    model = str(codex_model or "").strip()
    aliases = {
        "gpt5.5": "gpt-5.5",
    }
    return aliases.get(model.lower(), model)


def build_codex_exec_command(*, codex_executable: str, output_path: Path, codex_model: str = "") -> list[str]:
    """Build a Codex CLI command compatible with current `codex exec` parsing."""
    cmd = [
        codex_executable,
        "--ask-for-approval",
        "never",
        "exec",
        "--cd",
        str(PROJECT_ROOT),
        "--sandbox",
        "danger-full-access",
        "--output-last-message",
        str(output_path),
        "-",
    ]
    model = normalize_codex_model_name(codex_model)
    if model:
        cmd[3:3] = ["--model", model]
    return cmd


def _codex_last_message(output_path: Path, codex_result: dict[str, Any]) -> str:
    if output_path.exists():
        return output_path.read_text(encoding="utf-8")
    return "\n".join(
        part
        for part in [
            str(codex_result.get("stdout") or ""),
            str(codex_result.get("stderr") or ""),
        ]
        if part
    )


def _finalize_execution_failure(
    job_path: Path,
    job: dict[str, Any],
    *,
    state: str,
    message: str,
    bitable_status: str = "失败",
) -> dict[str, Any]:
    runner = job.setdefault("runner", {})
    runner["state"] = state
    runner["error"] = message
    job["status"] = "failed"
    write_job(job_path, job)
    return {
        "status": "failed",
        "state": state,
        "bitable_status": bitable_status,
        "error": message,
        "job_path": str(job_path),
    }


def execute_prepared_template_copy_job(
    job_path: Path,
    *,
    codex_bin: str = "",
    codex_model: str = "",
    execute: bool = True,
    timeout_sec: int = 60 * 60,
) -> dict[str, Any]:
    """Run Codex for a prepared template-copy job and persist the skill result."""
    job = read_job(job_path)
    runner = job.setdefault("runner", {})
    artifacts = runner.setdefault("artifacts", {})
    prompt_path_value = artifacts.get("codex_prompt_path")
    if not prompt_path_value:
        return _finalize_execution_failure(
            job_path,
            job,
            state="missing_codex_prompt",
            message="job 尚未准备 codex_prompt.txt",
        )
    prompt_path = Path(str(prompt_path_value))
    if not prompt_path.exists():
        return _finalize_execution_failure(
            job_path,
            job,
            state="missing_codex_prompt",
            message=f"codex prompt 不存在: {prompt_path}",
        )
    run_dir = Path(str(runner.get("run_dir") or prompt_path.parent))
    output_path = run_dir / "codex_last_message.md"
    artifacts["codex_last_message_path"] = str(output_path)

    codex_result = run_codex_prompt(
        prompt_path,
        output_path=output_path,
        execute=execute,
        codex_bin=codex_bin,
        codex_model=codex_model,
        timeout_sec=timeout_sec,
    )
    runner["codex_exec"] = codex_result
    if codex_result.get("skipped"):
        runner["state"] = "prepared"
        job["status"] = "prepared"
        write_job(job_path, job)
        return {
            "status": "prepared",
            "state": "prepared",
            "bitable_status": "已提交",
            "job_path": str(job_path),
            "run_dir": str(run_dir),
            "codex_result": codex_result,
        }
    if int(codex_result.get("returncode") or 0) != 0:
        return _finalize_execution_failure(
            job_path,
            job,
            state="codex_failed",
            message=str(codex_result.get("stderr") or codex_result.get("stdout") or "codex exec failed")[-1000:],
        )

    final_message = _codex_last_message(output_path, codex_result)
    try:
        skill_result = parse_skill_result(final_message)
    except TemplateCopyWorkerError as exc:
        return _finalize_execution_failure(job_path, job, state=exc.code, message=exc.message)

    skill_result_path = run_dir / "skill_result.json"
    skill_result_path.write_text(json.dumps(skill_result, ensure_ascii=False, indent=2), encoding="utf-8")
    artifacts["skill_result_path"] = str(skill_result_path)

    classified = classify_skill_result(skill_result)
    runner["state"] = classified["state"]
    runner["skill_result"] = skill_result
    if classified["error"]:
        runner["error"] = classified["error"]
    else:
        runner.pop("error", None)
    job["status"] = classified["status"]
    write_job(job_path, job)
    return {
        "status": classified["status"],
        "state": classified["state"],
        "bitable_status": classified["bitable_status"],
        "error": classified["error"],
        "job_path": str(job_path),
        "run_dir": str(run_dir),
        "skill_result_path": str(skill_result_path),
        "codex_result": codex_result,
    }


def iter_queued_jobs(job_dir: Path = DEFAULT_JOB_DIR) -> list[Path]:
    if not job_dir.exists():
        return []
    paths: list[Path] = []
    for path in sorted(job_dir.glob("*.json")):
        try:
            job = read_job(path)
        except Exception:
            continue
        if str(job.get("status") or "") in {"queued", "prepared"}:
            paths.append(path)
    return paths


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="准备或消费 VE 模板复刻 job")
    parser.add_argument("--job", default="", help="单个 job JSON；不传则扫描 job-dir")
    parser.add_argument("--job-dir", default=str(DEFAULT_JOB_DIR))
    parser.add_argument("--model-ref-dir", default=str(DEFAULT_MODEL_REF_DIR))
    parser.add_argument("--model-path", default=os.getenv("VE_TEMPLATE_COPY_MODEL_PATH", ""))
    parser.add_argument("--work-dir", default=str(DEFAULT_WORK_DIR))
    parser.add_argument("--no-download", action="store_true", help="不下载远程源素材，只接受本地路径")
    parser.add_argument("--execute-codex", action="store_true", help="实际调用 codex exec；默认只准备 prompt/参数")
    parser.add_argument("--codex-bin", default=os.getenv("VE_TEMPLATE_COPY_CODEX_BIN", ""), help="覆盖 codex 可执行文件路径")
    parser.add_argument("--codex-model", default=os.getenv("VE_TEMPLATE_COPY_CODEX_MODEL", ""), help="覆盖 codex exec 使用的模型")
    parser.add_argument("--limit", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    args = parse_args(argv)
    job_paths = [Path(args.job)] if args.job else iter_queued_jobs(Path(args.job_dir))[: max(1, args.limit)]
    results: list[dict[str, Any]] = []
    for job_path in job_paths:
        prepared = prepare_template_copy_job(
            job_path,
            model_ref_dir=Path(args.model_ref_dir),
            model_path=args.model_path,
            work_dir=Path(args.work_dir),
            download=not args.no_download,
            dry_run=not args.execute_codex,
        )
        if args.execute_codex:
            execution = execute_prepared_template_copy_job(
                job_path,
                codex_bin=args.codex_bin,
                codex_model=args.codex_model,
                execute=True,
            )
            prepared["execution"] = execution
        results.append(prepared)
    print(json.dumps({"processed": len(results), "results": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
