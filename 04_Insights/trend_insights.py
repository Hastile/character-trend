"""
trend_insights.py

raw/trending/*.json 을 읽어
- 카테고리별 비중 및 조회수 합계
- 주요 키워드 빈도
를 계산하고 간단한 리포트를 작성한다.
"""

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Any, List

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]  # .../04_Insights
YOUTUBE_ROOT = PROJECT_ROOT.parents[1] / "01_Sources" / "YouTube"
TRENDING_DIR = YOUTUBE_ROOT / "raw" / "trending"
REPORT_DIR = PROJECT_ROOT
REPORT_DIR.mkdir(parents=True, exist_ok=True)

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

def _load_latest_trending() -> Dict[str, Any]:
    files = sorted(TRENDING_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"트렌딩 파일이 없습니다: {TRENDING_DIR}")
    latest = files[-1]
    with latest.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def _tokenize(text: str) -> List[str]:
    import re
    text = text.lower()
    text = re.sub(r"[^0-9a-z가-힣]+", " ", text)
    tokens = [t for t in text.split() if len(t) > 1 and t not in STOPWORDS]
    return tokens

def build_insights() -> Dict[str, Any]:
    data = _load_latest_trending()
    items = data.get("items", [])

    cat_counts = Counter()
    cat_views = defaultdict(int)
    keyword_counts = Counter()

    for item in items:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        cat_id = snippet.get("categoryId") or item.get("__category_id_from_request") or "unknown"
        views = int(stats.get("viewCount", 0))
        cat_counts[cat_id] += 1
        cat_views[cat_id] += views

        title = snippet.get("title", "") or ""
        desc = snippet.get("description", "") or ""
        tokens = _tokenize(title + " " + desc)
        keyword_counts.update(tokens)

    top_cats = [
        {
            "category_id": cid,
            "label": CATEGORY_LABELS.get(cid, cid),
            "count": count,
            "total_views": cat_views[cid],
        }
        for cid, count in cat_counts.most_common()
    ]
    top_keywords = keyword_counts.most_common(30)

    return {
        "region_code": data.get("region_code"),
        "fetched_at_utc": data.get("fetched_at_utc"),
        "top_categories": top_cats,
        "top_keywords": top_keywords,
        "total_items": len(items),
    }

def save_markdown_report(insights: Dict[str, Any]) -> Path:
    ts = insights.get("fetched_at_utc", "unknown")
    region = insights.get("region_code", "unknown")
    filename = f"trend_report_{region}_{ts}.md"
    out_path = REPORT_DIR / filename
    lines: List[str] = []
    lines.append(f"# YouTube Trending Report — {region}")
    lines.append("")
    lines.append(f"- Fetched at (UTC): **{ts}**")
    lines.append(f"- Total items: **{insights.get('total_items', 0)}**")
    lines.append("")
    lines.append("## Top Categories")
    for cat in insights["top_categories"][:10]:
        lines.append(
            f"- **{cat['label']}** (ID {cat['category_id']}): "
            f"{cat['count']} videos, {cat['total_views']} views"
        )
    lines.append("")
    lines.append("## Top Keywords")
    for word, cnt in insights["top_keywords"][:20]:
        lines.append(f"- `{word}` — {cnt}")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path

if __name__ == "__main__":
    ins = build_insights()
    path = save_markdown_report(ins)
    print(f"saved report: {path}")
