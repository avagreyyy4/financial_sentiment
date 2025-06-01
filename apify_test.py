import os
import json
import time
import requests
from datetime import date
from pathlib import Path

# HARD-CODED API TOKEN & TASK ID
APIFY_TOKEN = "apify_api_7ua5RrN817059hOmIroWLsfQk1mxi13J01K1"
TASK_ID = "Ffs3D8o0X6OGZPg4n"

TICKERS = ["NVDA", "AMZN", "MSFT", "AAPL", "TSLA", "META"]
OUTPUT_DIR = Path(f"data/pull_{date.today().isoformat()}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def trigger_task(ticker):
    print(f"🚀 Triggering task for ${ticker}...")
    url = f"https://api.apify.com/v2/actor-tasks/{TASK_ID}/runs?token={APIFY_TOKEN}"
    payload = {
        "searchTerms": [f"${ticker}"],
        "tweetLanguage": "en",
        "maxItems": 10,
        "sort": "Latest"
    }
    res = requests.post(url, json=payload)
    if res.status_code != 201:
        print(f"❌ Failed to trigger task for {ticker} — {res.status_code}")
        return None
    return res.json().get("id")

def wait_for_run(run_id):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    for _ in range(40):  # wait up to 200 seconds
        res = requests.get(url)
        data = res.json()
        status = data.get("status")
        if status in ("SUCCEEDED", "FAILED", "TIMED-OUT"):
            return data
        time.sleep(5)
    return None

def download_dataset(dataset_id):
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json&clean=true&token={APIFY_TOKEN}"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"❌ Failed to download dataset {dataset_id} — {res.status_code}")
        return []
    try:
        return res.json()
    except json.JSONDecodeError:
        print("❌ Error decoding JSON response.")
        return []

def is_likely_spam(text):
    spam_keywords = ["free", "win", "link", "giveaway", "http", "https", "retweet", "follow", "airdrop"]
    return any(kw in text.lower() for kw in spam_keywords)

def scrape_ticker(ticker):
    run_id = trigger_task(ticker)
    if not run_id:
        return

    run_data = wait_for_run(run_id)
    if not run_data:
        print(f"⚠️ Run timed out or invalid for {ticker}")
        return

    dataset_id = run_data.get("defaultDatasetId")
    if not dataset_id:
        print(f"⚠️ No dataset ID for {ticker}")
        return

    tweets = download_dataset(dataset_id)
    filtered = [t for t in tweets if not is_likely_spam(t.get("text", "")) and f"${ticker}" in t.get("text", "")]
    out_path = OUTPUT_DIR / f"{ticker}_tweets.json"
    with open(out_path, "w") as f:
        json.dump(filtered, f, indent=2)
    print(f"✅ Saved {len(filtered)} tweets for ${ticker} → {out_path}")

# Run all
for ticker in TICKERS:
    scrape_ticker(ticker)

print(f"\n🎉 All done! Data saved to: {OUTPUT_DIR}")

