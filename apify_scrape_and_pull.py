import requests
import time
import json
from pathlib import Path
from datetime import date

APIFY_TOKEN = "apify_api_7ua5RrN817059hOmIroWLsfQk1mxi13J01K1"
TASK_ID = "Ffs3D8o0X6OGZPg4n"
TICKERS = ["NVDA", "AMZN", "MSFT", "AAPL", "TSLA", "META"]
BASE_DIR = Path(f"data/pull_{date.today().isoformat()}")
BASE_DIR.mkdir(parents=True, exist_ok=True)

def trigger_task(ticker):
    print(f"🚀 Triggering task for ${ticker}...")
    url = f"https://api.apify.com/v2/actor-tasks/{TASK_ID}/runs?token={APIFY_TOKEN}"
    payload = {
        "searchTerms": [f"${ticker}"],
        "tweetLanguage": "en",
        "sort": "Latest",
        "maxItems": 10,
    }
    res = requests.post(url, json=payload)
    if res.status_code != 201:
        print(f"❌ Failed to trigger task for ${ticker}: {res.status_code}")
        return None
    return res.json()["data"]["id"]

def wait_for_completion(run_id):
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    for _ in range(30):  # Up to ~2.5 mins
        res = requests.get(status_url)
        status = res.json()["data"]["status"]
        if status in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
            return res.json()["data"]
        time.sleep(5)
    return None

def download_data(dataset_id, ticker):
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json&clean=true&token={APIFY_TOKEN}"
    res = requests.get(url)
    if res.status_code == 200:
        data = res.json()
        out_path = BASE_DIR / f"{ticker}_tweets.json"
        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"✅ Saved {len(data)} tweets to {out_path}")
    else:
        print(f"❌ Failed to fetch dataset for ${ticker}: {res.status_code}")

# Main
for ticker in TICKERS:
    run_id = trigger_task(ticker)
    if not run_id:
        continue
    run_data = wait_for_completion(run_id)
    if not run_data:
        print(f"❌ Timed out waiting for ${ticker}")
        continue
    dataset_id = run_data.get("defaultDatasetId")
    if not dataset_id:
        print(f"⚠️ No dataset found for ${ticker}")
        continue
    download_data(dataset_id, ticker)

print(f"\n🎉 All done! Check folder: {BASE_DIR}")

