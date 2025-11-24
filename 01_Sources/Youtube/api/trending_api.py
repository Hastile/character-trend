"""
trending_api.py

YouTube Data API v3의 videos.list(chart=mostPopular)를 이용해
지역/카테고리별 트렌딩 영상을 수집한다.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from youtube_client import YouTubeTrendingClient

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]
TRENDING_DIR = PROJECT_ROOT / "raw" / "trending"

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "youtube_trending.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 기본 카테고리 목록 (필요에 따라 수정 가능)
DEFAULT_CATEGORY_IDS: List[str] = [
    "1",   # Film & Animation
    "10",  # Music
    "17",  # Sports
    "20",  # Gaming
    "22",  # People & Blogs
    "23",  # Comedy
    "24",  # Entertainment
    "25",  # News & Politics
    "26",  # Howto & Style
    "27",  # Education
    "28",  # Science & Technology
]

def collect_trending(
    region_code: str = "KR",
    category_ids: Optional[List[str]] = None,
    max_results_per_cat: int = 20,
) -> Path:
    """
    지역/카테고리별 mostPopular 영상 수집 후 하나의 JSON으로 저장.
    """
    client = YouTubeTrendingClient()
    TRENDING_DIR.mkdir(parents=True, exist_ok=True)

    if category_ids is None:
        category_ids = DEFAULT_CATEGORY_IDS

    all_items: List[Dict[str, Any]] = []
    for cid in category_ids:
        logger.info("트렌딩 수집: region=%s, category=%s", region_code, cid)
        data = client.list_most_popular(
            region_code=region_code,
            category_id=cid,
            max_results=max_results_per_cat
        )
        items = data.get("items", [])
        for item in items:
            item["__category_id_from_request"] = cid  # 후속 분석용
        all_items.extend(items)

    now_utc = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{now_utc}__trending_{region_code}.json"
    out_path = TRENDING_DIR / filename
    payload = {
        "region_code": region_code,
        "fetched_at_utc": now_utc,
        "items": all_items,
    }
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("트렌딩 저장 완료: %s (items=%d)", out_path, len(all_items))
    return out_path

if __name__ == "__main__":
    collect_trending(region_code="KR", max_results_per_cat=20)
