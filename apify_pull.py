import os
import time
import json
import requests
from pathlib import Path
from datetime import date

# === HARDCODE YOUR CREDENTIALS HERE ===
APIFY_TOKEN = "apify_api_7ua5RrN817059hOmIroWLsfQk1mxi13J01K1"
APIFY_TASK_ID = "Ffs3D8o0X6OGZPg4n"

TICKERS = ["NVDA", "AMZN", "MSFT", "AAPL", "TSLA", "META"]
OUTPUT_DIR = Path(f"data/pull_{date.today().isoformat()}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def is_likely_spam(text):
    spam_keywords = ["free", "win", "link", "giveaway", "http", "https", "retweet", "follow", "airdrop"]
    return any(kw in text.lower() for kw in spam_keywords)

def trigger_task(ticker):
    print(f"\n🚀 Triggering task for ${ticker}...")
    url = f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/runs?token={APIFY_TOKEN}"
    payload = {
        "searchTerms": [f"${ticker}"],
        "tweetLanguage": "en",
        "sort": "Latest",
        "maxItems": 10  # Reduce for debugging
    }
    res = requests.post(url, json=payload)
    if res.status_code != 201:
        print(f"❌ Failed to trigger task for ${ticker} — {res.status_code}")
        return None
    return res.json().get("id")

def wait_for_run(run_id):
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    for _ in range(30):  # Wait up to 150 seconds
        res = requests.get(status_url)
        data = res.json()
        if data["status"] == "SUCCEEDED":
            return data.get("defaultDatasetId")
        elif data["status"] in ["FAILED", "TIMED-OUT"]:
            return None
        time.sleep(5)
    return None

def download_and_save_tweets(dataset_id, ticker):
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json&clean=true&token={APIFY_TOKEN}"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"❌ Failed to fetch dataset for ${ticker}")
        return
    tweets = res.json()
    filtered = [t for t in tweets if not is_likely_spam(t.get("text", ""))]
    out_path = OUTPUT_DIR / f"{ticker}_tweets.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(filtered, f, indent=2)
    print(f"✅ Saved {len(filtered)} tweets for ${ticker} to {out_path}")

# === MAIN LOOP ===
for ticker in TICKERS:
    run_id = trigger_task(ticker)
    if not run_id:
        continue
    dataset_id = wait_for_run(run_id)
    if not dataset_id:
        print(f"⚠️ No dataset ID found for ${ticker}")
        continue
    download_and_save_tweets(dataset_id, ticker)

print("\n🎉 All done! Files saved in:", OUTPUT_DIR)

