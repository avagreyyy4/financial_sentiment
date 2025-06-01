import os
import json
import requests
from pathlib import Path
from datetime import date

# Replace this with a known dataset ID from your Apify run that worked
EXISTING_DATASET_ID = "gfFTyxNfpiL2Pwq1T"
APIFY_TOKEN = "apify_api_7ua5RrN817059hOmIroWLsfQk1mxi13J01K1"
TICKER = "NVDA"  # Just label the output file for now

OUTPUT_DIR = Path(f"data/test_pull_{date.today().isoformat()}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def pull_and_save_dataset(dataset_id):
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json&clean=true&token={APIFY_TOKEN}"
    res = requests.get(url)
    
    if res.status_code != 200:
        print(f"❌ Failed to fetch dataset: {res.status_code}")
        print(res.text)
        return

    try:
        tweets = res.json()
    except Exception as e:
        print("❌ JSON decode error:", e)
        return

    out_path = OUTPUT_DIR / f"{TICKER}_test.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(tweets, f, indent=2)

    print(f"✅ Pulled {len(tweets)} tweets. Saved to {out_path}")

pull_and_save_dataset(EXISTING_DATASET_ID)

