"""
search_api.py

YouTube Data API v3 기반 기본 수집기.
- 키워드로 영상 검색 (search.list)
- videoId 리스트에 대해 상세 정보(statistics 포함) 조회 (videos.list)
- raw/search/ 아래에 날짜+키워드 기준으로 JSON 저장

필터(카테고리/IP 등)는 교차검증 전에 사용하지 않기 위해
함수 틀만 남겨두고 실제 호출은 하지 않는다.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from youtube_client import YouTubeSearchClient, YouTubeStatsClient  # 🔹 공통 클라이언트 사용

# ------------------------------
# 설정
# ------------------------------

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]  # .../01_Sources/YouTube
RAW_DIR = PROJECT_ROOT / "raw" / "search"

# ------------------------------
# 로깅 설정
# ------------------------------

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "youtube_search_api.log"

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
# (미사용) 필터 함수 틀
# → 현재 단계에서는 실제로 사용하지 않음.
# ------------------------------

def _filter_by_category_example(items: List[Dict[str, Any]], allowed_categories: List[str]) -> List[Dict[str, Any]]:
    """
    이 함수는 아직 교차검증 전이므로 사용하지 않는다.
    categoryId 기반 필터 예시 구조만 남겨둔 것.
    """
    filtered: List[Dict[str, Any]] = []
    for item in items:
        snippet = item.get("snippet", {})
        category_id = snippet.get("categoryId") or item.get("categoryId")
        if category_id in allowed_categories:
            filtered.append(item)
    return filtered


# ------------------------------
# 검색 → 상세조회 → 저장 플로우
# ------------------------------

def search_and_collect(
    query: str,
    max_results: int = 50,
    order: str = "date",
    published_after: Optional[str] = None,
    region_code: Optional[str] = None,
    output_dir: Optional[Path] = None
) -> Path:
    """
    1. search.list로 영상 리스트 조회
    2. videos.list로 상세 정보 조회
    3. raw/search/에 JSON 저장
    4. 저장된 파일 경로 반환

    필터(category/IP)는 현재 단계에서는 적용하지 않는다.
    """
    search_client = YouTubeSearchClient()
    stats_client = YouTubeStatsClient()

    logger.info("YouTube 검색 시작: query=%s, max_results=%d", query, max_results)

    # 1) 검색
    search_data = search_client.search(
        query=query,
        max_results=max_results,
        order=order,
        published_after=published_after,
        region_code=region_code,
    )

    search_items = search_data.get("items", [])
    video_ids: List[str] = [
        item["id"]["videoId"]
        for item in search_items
        if item.get("id", {}).get("kind") == "youtube#video"
    ]

    logger.info("검색 결과 영상 수: %d", len(video_ids))

    # 2) 상세 조회
    details_data = stats_client.get_video_details(video_ids)
    detail_items = details_data.get("items", [])

    logger.info("상세 정보 수신 영상 수: %d", len(detail_items))

    # ※ 여기에서 카테고리/키워드 필터를 넣을 수 있지만,
    #    현재 단계에서는 사용하지 않으므로 주석으로만 남긴다.
    #
    # allowed_categories = ["1", "24", "31"]  # 예: Film & Animation, Entertainment, Anime/Animation 등
    # detail_items = _filter_by_category_example(detail_items, allowed_categories)

    # 3) 저장 준비
    if output_dir is None:
        output_dir = RAW_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    today_str = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    safe_query = "".join(c if c.isalnum() else "_" for c in query)[:50]
    filename = f"{today_str}__{safe_query}.json"
    output_path = output_dir / filename

    payload: Dict[str, Any] = {
        "query": query,
        "max_results": max_results,
        "order": order,
        "published_after": published_after,
        "region_code": region_code,
        "fetched_at_utc": today_str,
        "items": detail_items,
        "raw_search_response": search_data,   # 필요하면 추후 제거 가능
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("검색 결과 저장 완료: %s", output_path)
    return output_path


# ------------------------------
# 간단 실행 예시
# ------------------------------

if __name__ == "__main__":
    test_queries = [
        "Attack on Titan"
    ]

    for q in test_queries:
        try:
            path = search_and_collect(
                query=q,
                max_results=20,
                order="date",
                region_code="KR",  # 필요에 따라 변경
            )
            print(f"saved: {path}")
        except Exception as e:
            logger.exception("쿼리 실행 중 예외 발생: %s", e)
