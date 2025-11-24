"""
scoring.py

유튜브 영상 스냅샷(raw/stats_snapshots/*.json)을 기반으로
Δviews / Δlikes / Δcomments를 계산하고 스파이크 점수를 산출하는 모듈.

기본 로직:
1) 스냅샷 폴더에서 최근 N개의 스냅샷 로드
2) videoId 기준으로 시간순 데이터 정렬
3) Δviews, Δlikes, Δcomments 계산
4) z-score 기반 스파이크 감지
5) 최종적으로 영상별 스코어(0~100)를 생성

이 모듈은 이후 "오늘 찍을 캐릭터" 추천 파이프라인의 핵심 기반이 된다.
"""

import json
from pathlib import Path
from typing import Dict, List, Any
import numpy as np

# --------------------------------------
# 경로 설정
# --------------------------------------
HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]   # .../01_Sources/Youtube
SNAPSHOT_DIR = PROJECT_ROOT / "raw" / "stats_snapshots"

# --------------------------------------
# 1) 스냅샷 로더
# --------------------------------------

def load_recent_snapshots(limit: int = 5) -> List[Dict[str, Any]]:
    """
    최근 N개의 스냅샷 파일을 시간순으로 로드
    """
    files = sorted(SNAPSHOT_DIR.glob("*.json"))
    recent = files[-limit:]

    snapshots = []
    for path in recent:
        try:
            with path.open("r", encoding="utf-8") as f:
                snapshots.append(json.load(f))
        except Exception:
            continue

    return snapshots

# --------------------------------------
# 2) 스냅샷 → time-series 구조로 재정렬
# --------------------------------------

def build_time_series(snapshots: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    videoId별로 시간순 기록을 정렬한다.
    Return: { videoId: [ {views, likes, comments, time}, ... ] }
    """
    series = {}

    for snap in snapshots:
        timestamp = snap.get("snapshot_time_utc")
        items = snap.get("items", [])

        for item in items:
            vid = item.get("id")
            if not isinstance(vid, str):
                continue

            stats = item.get("statistics", {})
            row = {
                "time": timestamp,
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0)),
            }

            if vid not in series:
                series[vid] = []
            series[vid].append(row)

    # 시간순 정렬
    for vid in series:
        series[vid] = sorted(series[vid], key=lambda x: x["time"])

    return series

# --------------------------------------
# 3) Δ 계산
# --------------------------------------

def compute_deltas(ts: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """
    각 videoId에 대해 Δviews, Δlikes, Δcomments를 계산한다.
    Return: { videoId: { deltas, last_values } }
    """
    out = {}

    for vid, rows in ts.items():
        if len(rows) < 2:
            continue  # delta 불가

        # 가장 최근 두 개로 delta 계산
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

# --------------------------------------
# 4) z-score 기반 스파이크 스코어
# --------------------------------------

def compute_spike_scores(delta_map: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """
    Δviews 중심으로 z-score를 계산해 0~100 스케일로 변환.
    """
    if not delta_map:
        return {}

    values = np.array([v["delta_views"] for v in delta_map.values()])

    mean = np.mean(values)
    std = np.std(values) if np.std(values) != 0 else 1

    scores = {}
    for vid, data in delta_map.items():
        z = (data["delta_views"] - mean) / std
        # 0~100 스케일 변환(간단 모델)
        score = max(0.0, min(100.0, (z + 3) / 6 * 100))
        scores[vid] = round(score, 2)

    return scores

# --------------------------------------
# 5) 최종 좌표
# --------------------------------------

def run_scoring(limit_snapshots: int = 5) -> Dict[str, Any]:
    """
    전체 스코어링 파이프라인 실행 후 결과 반환.
    { videoId: {score, delta_views, ...} }
    """
    snaps = load_recent_snapshots(limit_snapshots)
    ts = build_time_series(snaps)
    delta_map = compute_deltas(ts)
    spike_scores = compute_spike_scores(delta_map)

    results = {}
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


# --------------------------------------
# 실행 예시
# --------------------------------------
if __name__ == "__main__":
    out = run_scoring(limit_snapshots=5)
    for vid, info in out.items():
        print(vid, info)
