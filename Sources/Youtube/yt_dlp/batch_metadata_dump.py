"""
batch_metadata_dump.py

- raw/search/*.json에서 videoId 목록을 읽고,
- 각 videoId에 대해 yt_dlp_wrapper.fetch_metadata_json 실행.
"""

import json
from pathlib import Path
from typing import List

from yt_dlp_wrapper import fetch_metadata_json


HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]
SEARCH_RAW_DIR = PROJECT_ROOT / "raw" / "search"


def load_video_ids_from_search() -> List[str]:
    video_ids: List[str] = []

    for json_path in SEARCH_RAW_DIR.glob("*.json"):
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("items", [])
        for item in items:
            vid = item.get("id")
            if isinstance(vid, str):
                video_ids.append(vid)

    return list(set(video_ids))


def run_batch():
    vids = load_video_ids_from_search()
    print(f"총 대상 영상 수: {len(vids)}")

    for vid in vids:
        try:
            fetch_metadata_json(vid, save=True)
        except Exception as e:
            print(f"[실패] {vid}: {e}")


if __name__ == "__main__":
    run_batch()
