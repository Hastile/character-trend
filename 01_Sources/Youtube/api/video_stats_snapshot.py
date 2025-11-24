"""
video_stats_snapshot.py

YouTube Data API 기반 조회수/좋아요/댓글 스냅샷 수집기.
- raw/search/*.json 에서 videoId 목록 로드
- videos.list 로 현재 통계 조회
- raw/stats_snapshots/ 에 timestamp 기반으로 저장

이 스냅샷들이 Δviews/Δt, 스파이크 탐지, 알고리즘 감지의 핵심 데이터가 된다.
"""

import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from youtube_client import YouTubeStatsClient  # 🔹 공통 클라이언트 사용


# ------------------------------
# 디렉토리 설정
# ------------------------------

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]   # .../01_Sources/Youtube
SEARCH_RAW_DIR = PROJECT_ROOT / "raw" / "search"
SNAPSHOT_DIR = PROJECT_ROOT / "raw" / "stats_snapshots"


# ------------------------------
# 로깅 설정
# ------------------------------

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "youtube_stats_snapshot.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ------------------------------
# raw/search → videoId 로드
# ------------------------------

def load_video_ids_from_details() -> List[str]:
    """
    search_api.py는 detail_items만 저장하므로
    실제 videoId는 item["id"]에 string으로 저장됨.

    raw/search/*.json → payload["items"] → item["id"]
    """
    if not SEARCH_RAW_DIR.exists():
        raise FileNotFoundError(f"검색 결과 폴더가 없습니다: {SEARCH_RAW_DIR}")

    video_ids: List[str] = []

    for json_path in SEARCH_RAW_DIR.glob("*.json"):
        try:
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            items = data.get("items", [])
            for item in items:
                vid = item.get("id")
                if isinstance(vid, str):
                    video_ids.append(vid)

        except Exception as e:
            logger.warning("로드 실패 %s: %s", json_path, e)

    # 중복 제거
    return list(set(video_ids))


# ------------------------------
# 스냅샷 저장
# ------------------------------

def save_snapshot(stats_items: List[Dict[str, Any]]) -> Path:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{timestamp}__snapshot.json"

    out_path = SNAPSHOT_DIR / filename

    payload = {
        "snapshot_time_utc": timestamp,
        "items": stats_items
    }

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("스냅샷 저장 완료: %s", out_path)
    return out_path


# ------------------------------
# 실행 플로우
# ------------------------------

def run_snapshot():
    logger.info("=== 스냅샷 수집 시작 ===")

    video_ids = load_video_ids_from_details()

    if not video_ids:
        logger.warning("videoId가 없음. raw/search 폴더 확인 필요.")
        return

    logger.info("대상 영상 수: %d", len(video_ids))

    client = YouTubeStatsClient()

    # YouTube API는 id 최대 50개 제한
    batch_size = 50
    all_items: List[Dict[str, Any]] = []

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i:i+batch_size]
        data = client.get_video_details(batch)
        items = data.get("items", [])
        all_items.extend(items)
        time.sleep(1.0)  # API 부담 완화용 딜레이

    save_snapshot(all_items)

    logger.info("=== 스냅샷 수집 종료 ===")


# ------------------------------
# 직접 실행 시
# ------------------------------

if __name__ == "__main__":
    run_snapshot()
