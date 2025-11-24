# 01_Sources/Youtube/scraper/related_videos_scraper.py

"""
related_videos_scraper.py

특정 videoId에 대해 Innertube API의 /next 엔드포인트를 호출해
관련 영상 목록을 수집한다.
"""

import json
from pathlib import Path
from datetime import datetime
from Sources.Youtube.api.youtube_client import InnertubeClient
import logging

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[2]
RAW_DIR = PROJECT_ROOT / "raw" / "related"
LOG_DIR = PROJECT_ROOT / "logs"

LOG_FILE = LOG_DIR / "related_videos_scraper.log"
logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# 미리 config에서 키/컨텍스트를 읽어온다고 가정
from Sources.Youtube.config.config_loader import load_innertube_config  # (사용자 구현 필요)

client = InnertubeClient(
    api_key=load_innertube_config()["api_key"],
    context=load_innertube_config()["context"]
)

def scrape_related(video_id: str) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    data = client.get_related_videos(video_id)

    now_utc = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{now_utc}__related_{video_id}.json"
    out_path = RAW_DIR / filename
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info("관련 영상 저장: %s", out_path)
    return out_path

if __name__ == "__main__":
    # 테스트 ID
    scrape_related("dQw4w9WgXcQ")
