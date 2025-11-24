# 01_Sources/Youtube/scraper/home_feed_scraper.py

"""
home_feed_scraper.py

Innertube API의 browse 엔드포인트를 사용해
홈피드 추천 영상을 수집한다.
"""

import json
from pathlib import Path
from datetime import datetime
from Sources.Youtube.api.youtube_client import InnertubeClient
import logging

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[2]
RAW_DIR = PROJECT_ROOT / "raw" / "home_feed"
LOG_DIR = PROJECT_ROOT / "logs"

LOG_FILE = LOG_DIR / "home_feed_scraper.log"
logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

from Sources.Youtube.config.config_loader import load_innertube_config

client = InnertubeClient(
    api_key=load_innertube_config()["api_key"],
    context=load_innertube_config()["context"]
)

def scrape_home_feed() -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    data = client.get_home_feed()

    now_utc = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{now_utc}__home_feed.json"
    out_path = RAW_DIR / filename
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info("홈피드 저장: %s", out_path)
    return out_path

if __name__ == "__main__":
    scrape_home_feed()
