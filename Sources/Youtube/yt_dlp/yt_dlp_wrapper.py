"""
yt_dlp_wrapper.py

- yt-dlp를 subprocess로 호출해서 메타데이터 JSON을 받아오는 최소 래퍼.
- 단일 videoId 또는 URL 기준으로 사용.
"""

import json
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional


HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]          # .../01_Sources/YouTube
RAW_DIR = PROJECT_ROOT / "raw" / "yt_dlp_meta"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_metadata_json(
    video_id: str,
    save: bool = True,
    extra_args: Optional[list] = None,
) -> Dict[str, Any]:
    """
    특정 YouTube videoId에 대해 yt-dlp --dump-json 실행.
    :param video_id: YouTube video ID (e.g. "dQw4w9WgXcQ")
    :param save: raw/yt_dlp_meta/ 아래에 저장할지 여부
    :param extra_args: yt-dlp에 추가로 넘길 인자 리스트
    """

    url = f"https://www.youtube.com/watch?v={video_id}"

    cmd = ["yt-dlp", "-J", "--no-warnings", "--skip-download", url]
    if extra_args:
        cmd.extend(extra_args)

    # yt-dlp 실행
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    if completed.returncode != 0:
        raise RuntimeError(
            f"yt-dlp 실행 실패 (code={completed.returncode}): {completed.stderr[:500]}"
        )

    data = json.loads(completed.stdout)

    if save:
        # 파일명: videoId.json
        out_path = RAW_DIR / f"{video_id}.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return data
