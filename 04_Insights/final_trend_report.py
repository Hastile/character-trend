# final_trend_report.py
import json
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1]
TRENDING_TOPICS_DIR = ROOT / "02_Normalized" / "trending_topics"
TOPIC_SCORES_DIR    = ROOT / "03_Scoring" / "topic_scores"
REPORT_DIR          = HERE.parent / "final_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

def _load_latest_json(dir_path: Path) -> dict:
    files = sorted(dir_path.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No files in {dir_path}")
    with files[-1].open("r", encoding="utf-8") as f:
        return json.load(f)

def build_final_report():
    topics_data = _load_latest_json(TRENDING_TOPICS_DIR)
    scores_data = _load_latest_json(TOPIC_SCORES_DIR)
    # topics_data["topics"], scores_data["topics"]를 매칭하여
    # topic_id 혹은 category_id 기준으로 합치기
    # 필요한 종합 랭킹을 계산 후 리포트를 구성

    # 최종 리포트 저장
    report_path = REPORT_DIR / f"final_report_{topics_data['fetched_at_utc']}.md"
    with report_path.open("w", encoding="utf-8") as f:
        # 마크다운 형식으로 작성
        f.write("# Final Trend Report\n\n")
        # ... 내용 채우기 ...
    return report_path

if __name__ == "__main__":
    print("saved:", build_final_report())
