"""
scoring.py (in 03_Scoring)

01_Sources/Youtube/raw/stats_snapshots 를 참조하여
Δviews 기반 스파이크 점수를 계산하는 모듈.
"""

import json
from pathlib import Path
from typing import Dict, List, Any

import numpy as np

HERE = Path(__file__).resolve()
# 03_Scoring/scoring.py → parents[2]가 레포 root
YOUTUBE_ROOT = HERE.parents[2] / "01_Sources" / "Youtube"
SNAPSHOT_DIR = YOUTUBE_ROOT / "raw" / "stats_snapshots"

def load_recent_snapshots(limit: int = 5) -> List[Dict[str, Any]]:
    files = sorted(SNAPSHOT_DIR.glob("*.json"))
    snapshots = []
    for path in files[-limit:]:
        try:
            with path.open("r", encoding="utf-8") as f:
                snapshots.append(json.load(f))
        except Exception:
            continue
    return snapshots

def build_time_series(snapshots: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    series: Dict[str, List[Dict[str, Any]]] = {}
    for snap in snapshots:
        timestamp = snap.get("snapshot_time_utc")
        for item in snap.get("items", []):
            vid = item.get("id")
            if isinstance(vid, str):
                stats = item.get("statistics", {})
                row = {
                    "time": timestamp,
                    "views": int(stats.get("viewCount", 0)),
                    "likes": int(stats.get("likeCount", 0)),
                    "comments": int(stats.get("commentCount", 0)),
                }
                series.setdefault(vid, []).append(row)
    # 시간순 정렬
    for vid in series:
        series[vid] = sorted(series[vid], key=lambda x: x["time"])
    return series

def compute_deltas(ts: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for vid, rows in ts.items():
        if len(rows) < 2:
            continue
        prev, curr = rows[-2], rows[-1]
        dv = curr["views"] - prev["views"]
        dl = curr["likes"] - prev["likes"]
        dc = curr["comments"] - prev["comments"]
        out[vid] = {
            "delta_views": dv,
            "delta_likes": dl,
            "delta_comments": dc,
            "current_views": curr["views"],
        }
    return out

def compute_spike_scores(delta_map: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    if not delta_map:
        return {}
    values = np.array([v["delta_views"] for v in delta_map.values()])
    mean = float(np.mean(values))
    std = float(np.std(values)) or 1.0
    scores: Dict[str, float] = {}
    for vid, data in delta_map.items():
        z = (data["delta_views"] - mean) / std
        score = max(0.0, min(100.0, (z + 3) / 6 * 100))
        scores[vid] = round(score, 2)
    return scores

def run_scoring(limit_snapshots: int = 5) -> Dict[str, Any]:
    snaps = load_recent_snapshots(limit_snapshots)
    ts = build_time_series(snaps)
    delta_map = compute_deltas(ts)
    spike_scores = compute_spike_scores(delta_map)
    results: Dict[str, Any] = {}
    for vid, score in spike_scores.items():
        d = delta_map.get(vid, {})
        results[vid] = {
            "score": score,
            "delta_views": d.get("delta_views"),
            "delta_likes": d.get("delta_likes"),
            "delta_comments": d.get("delta_comments"),
            "current_views": d.get("current_views"),
        }
    return results

if __name__ == "__main__":
    res = run_scoring(limit_snapshots=5)
    for vid, info in res.items():
        print(vid, info)
