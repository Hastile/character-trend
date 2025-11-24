"""
trending_topics.py

raw/trending/*.json 의 최신 데이터를 읽어 카테고리별 토픽을 생성한다.
생성된 토픽은 02_Normalized/trending_topics/ 에 JSON으로 저장된다.
"""

import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# 디렉토리 설정
HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]  # .../02_Normalized
YOUTUBE_ROOT = PROJECT_ROOT.parents[1] / "01_Sources" / "YouTube"
TRENDING_RAW_DIR = YOUTUBE_ROOT / "raw" / "trending"
TOPICS_DIR = PROJECT_ROOT / "trending_topics"
TOPICS_DIR.mkdir(parents=True, exist_ok=True)

# 카테고리 라벨 (trend_insights.py와 동일하게 맞춤)
CATEGORY_LABELS: Dict[str, str] = {
    "1": "Film & Animation",
    "10": "Music",
    "17": "Sports",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "31": "Anime/Animation",
}

STOPWORDS = {
    "the","a","an","and","or","of","to","in","on",
    "이","그","저","것","오늘","영상","쇼츠","shorts",
    "video","official","mv","edit","full","episode",
}

def _tokenize(text: str) -> List[str]:
    import re
    text = text.lower()
    text = re.sub(r"[^0-9a-z가-힣]+", " ", text)
    tokens = [t for t in text.split() if len(t) > 1 and t not in STOPWORDS]
    return tokens

def _load_latest_trending() -> Dict[str, Any]:
    files = sorted(TRENDING_RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"트렌딩 파일이 없습니다: {TRENDING_RAW_DIR}")
    latest = files[-1]
    with latest.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def build_topics() -> List[Dict[str, Any]]:
    data = _load_latest_trending()
    fetched_at = data.get("fetched_at_utc")
    items = data.get("items", [])

    # 카테고리별 데이터 집계
    topics: Dict[str, Dict[str, Any]] = {}
    keyword_accumulator: Dict[str, Counter] = defaultdict(Counter)

    for item in items:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        cat_id = snippet.get("categoryId") or item.get("__category_id_from_request") or "unknown"
        views = int(stats.get("viewCount", 0))
        video_id = item.get("id")

        # 토픽 초기화
        if cat_id not in topics:
            topics[cat_id] = {
                "topic_id": f"{cat_id}_{fetched_at}",
                "category_id": cat_id,
                "label": CATEGORY_LABELS.get(cat_id, cat_id),
                "video_ids": [],
                "total_views": 0,
                "video_count": 0,
                "top_keywords": [],
            }

        # 정보 누적
        topics[cat_id]["video_ids"].append(video_id)
        topics[cat_id]["total_views"] += views
        topics[cat_id]["video_count"] += 1

        # 키워드 누적
        title = snippet.get("title", "") or ""
        desc = snippet.get("description", "") or ""
        tokens = _tokenize(title + " " + desc)
        keyword_accumulator[cat_id].update(tokens)

    # 각 카테고리 토픽에 top_keywords 채우기
    for cid, topic in topics.items():
        top_words = keyword_accumulator[cid].most_common(5)
        topic["top_keywords"] = [w for w, _ in top_words]

    return list(topics.values())

def save_topics(topics: List[Dict[str, Any]], region_code: str, fetched_at: str) -> Path:
    filename = f"{fetched_at}__topics_{region_code}.json"
    out_path = TOPICS_DIR / filename
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"region_code": region_code,
                   "fetched_at_utc": fetched_at,
                   "topics": topics}, f, ensure_ascii=False, indent=2)
    return out_path

if __name__ == "__main__":
    data = _load_latest_trending()
    region = data.get("region_code", "unknown")
    fetched_at = data.get("fetched_at_utc", datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    topics = build_topics()
    path = save_topics(topics, region, fetched_at)
    print(f"Saved topics file: {path}")
