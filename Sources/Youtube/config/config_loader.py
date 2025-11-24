# 01_Sources/Youtube/config/config_loader.py

import json
from pathlib import Path

HERE = Path(__file__).resolve()
CONFIG_PATH = HERE / "youtube_keys.json"

def load_innertube_config() -> dict:
    """
    youtube_keys.json에 저장된 Innertube API 키와 context 로드.

    예시 youtube_keys.json:
    {
      "api_key": "AIzaSyXXXXXX",
      "context": {
         "client": {
           "clientName": "WEB",
           "clientVersion": "2.20241121",
           "hl": "en",
           "gl": "US"
         }
      }
    }
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)
