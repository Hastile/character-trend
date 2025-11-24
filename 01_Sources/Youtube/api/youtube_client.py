"""
youtube_client.py

YouTube Data API v3 공통 클라이언트.
- API 키 로딩
- request_with_retry 공통 처리
- search.list 전용 YouTubeSearchClient
- videos.list 전용 YouTubeStatsClient

search_api.py, video_stats_snapshot.py, scraper 계열 모두 이 파일을 import하여 사용한다.
"""

import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests


# --------------------------------
# 디렉토리 설정
# --------------------------------

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]  # .../01_Sources/Youtube
CONFIG_DIR = PROJECT_ROOT / "config"
YOUTUBE_KEYS_PATH = CONFIG_DIR / "youtube_keys.json"

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


# --------------------------------
# 로깅
# --------------------------------

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "youtube_client.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# --------------------------------
# 공통 유틸
# --------------------------------

def load_api_key() -> str:
    """
    config/youtube_keys.json에서 YouTube Data API 키 로드
    """
    if not YOUTUBE_KEYS_PATH.exists():
        raise FileNotFoundError(f"API 키 파일이 없습니다: {YOUTUBE_KEYS_PATH}")

    with YOUTUBE_KEYS_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    api_key = data.get("api_key")
    if not api_key:
        raise ValueError("youtube_keys.json에 'api_key' 항목이 없음.")

    return api_key


def request_with_retry(
    url: str,
    params: Dict[str, Any],
    max_retries: int = 3,
    wait: float = 1.5
) -> Dict[str, Any]:
    """
    공통 retry 로직.
    search_api.py / snapshot 모듈에서 중복되던 코드 제거.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=10)

            if resp.status_code == 200:
                return resp.json()

            # quotaExceeded, rate limit
            if resp.status_code in (403, 429):
                logger.warning(
                    "YouTube API 쿼타/리밋(status=%s), 재시도 %d/%d",
                    resp.status_code, attempt, max_retries
                )
                time.sleep(wait * attempt)
                continue

            # 그 외 에러
            logger.warning(
                "YouTube 비정상 응답(status=%s): %s",
                resp.status_code, resp.text[:500]
            )
            time.sleep(wait * attempt)

        except requests.RequestException as e:
            logger.warning(
                "요청 예외 발생 %s — 재시도 %d/%d",
                e, attempt, max_retries
            )
            time.sleep(wait * attempt)

    raise RuntimeError(f"API 요청 실패: url={url}, params={params}")


# --------------------------------
# 베이스 클라이언트
# --------------------------------

class YouTubeBaseClient:
    """
    검색/상세 조회 공통 요소 담당
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or load_api_key()

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        모든 API는 이 경로로 통일해서 들어간다.
        """
        url = f"{YOUTUBE_API_BASE}/{endpoint}"
        params["key"] = self.api_key
        return request_with_retry(url, params)


# --------------------------------
# search.list 전용
# --------------------------------

class YouTubeSearchClient(YouTubeBaseClient):
    """
    검색(search.list)을 담당하는 클라이언트
    """

    def search(
        self,
        query: str,
        max_results: int = 50,
        order: str = "date",
        published_after: Optional[str] = None,
        region_code: Optional[str] = None,
        page_token: Optional[str] = None,
        safe_search: str = "none"
    ) -> Dict[str, Any]:

        params: Dict[str, Any] = {
            "part": "snippet",
            "type": "video",
            "q": query,
            "maxResults": max(1, min(max_results, 50)),
            "order": order,
            "safeSearch": safe_search
        }

        if published_after:
            params["publishedAfter"] = published_after
        if region_code:
            params["regionCode"] = region_code
        if page_token:
            params["pageToken"] = page_token

        return self._make_request("search", params)


# --------------------------------
# videos.list 전용 (상세 정보, 스냅샷)
# --------------------------------

class YouTubeStatsClient(YouTubeBaseClient):
    """
    videos.list 조회 (statistics/snippet/contentDetails)
    """

    def get_video_details(self, video_ids: List[str]) -> Dict[str, Any]:
        """
        videoIds는 1~50개 단위로 처리
        """
        if not video_ids:
            return {"items": []}

        params: Dict[str, Any] = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids),
            "maxResults": len(video_ids)
        }

        return self._make_request("videos", params)
