"""
topic_scoring.py

트렌딩 토픽과 스냅샷 기반 스파이크 점수를 결합하여
카테고리별 토픽의 우선순위를 계산하고 저장한다.
"""

import json
from pathlib import Path
from typing import Dict, Any, List

# 기존 scoring 모듈에서 스파이크 점수를 로딩
from scoring import run_scoring

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]   # .../03_Scoring
NORMALIZED_DIR = PROJECT_ROOT.parents[1] / "02_Normalized" / "trending_topics"
TOPIC_SCORE_DIR = PROJECT_ROOT / "topic_scores"
TOPIC_SCORE_DIR.mkdir(parents=True, exist_ok=True)

def _load_latest_topics() -> Dict[str, Any]:
    files = sorted(NORMALIZED_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"토픽 파일이 없습니다: {NORMALIZED_DIR}")
    latest = files[-1]
    with latest.open("r", encoding="utf-8") as f:
        return json.load(f)

def score_topics() -> List[Dict[str, Any]]:
    # 최근 토픽 로딩
    data = _load_latest_topics()
    region = data.get("region_code", "unknown")
    fetched_at = data.get("fetched_at_utc", "")
    topics = data.get("topics", [])
    
    # 스파이크 점수 로딩: videoId -> {score, delta_views, delta_likes, ...}
    scoring_results: Dict[str, Dict[str, Any]] = run_scoring(limit_snapshots=5)
    
    scored_topics: List[Dict[str, Any]] = []
    for t in topics:
        vids = t.get("video_ids", [])
        # Δviews 합계와 스파이크 평균 계산
        total_delta = 0
        spike_scores = []
        for vid in vids:
            stats = scoring_results.get(vid)
            if stats:
                total_delta += stats.get("delta_views", 0)
                spike_scores.append(stats.get("score", 0.0))
        avg_spike = sum(spike_scores) / len(spike_scores) if spike_scores else 0.0
        
        # topic_score = avg_spike (필요에 따라 가중치 조정 가능)
        t_score = avg_spike
        
        scored_topics.append({
            "topic_id": t["topic_id"],
            "category_id": t["category_id"],
            "label": t["label"],
            "video_count": t["video_count"],
            "total_views": t["total_views"],
            "total_delta_views": total_delta,
            "avg_spike_score": round(avg_spike, 2),
            "topic_score": round(t_score, 2),
            "top_keywords": t["top_keywords"],
        })
    
    # 스코어에 따라 내림차순 정렬
    scored_topics.sort(key=lambda x: x["topic_score"], reverse=True)
    
    # 결과 저장
    filename = f"{fetched_at}__topic_scores_{region}.json"
    out_path = TOPIC_SCORE_DIR / filename
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({
            "region_code": region,
            "fetched_at_utc": fetched_at,
            "topics": scored_topics,
        }, f, ensure_ascii=False, indent=2)
    
    return scored_topics

if __name__ == "__main__":
    results = score_topics()
    print(f"Scored {len(results)} topics.")
