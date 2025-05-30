import requests
import json
import time
from datetime import datetime

tickers = ["NVDA", "AMZN", "MSFT", "PLTR", "AKRO"]
headers = {"User-Agent": "Mozilla/5.0"}

def fetch_posts(ticker, limit=30):
    url = f"https://www.reddit.com/search.json?q={ticker}&sort=new&limit={limit}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        posts = response.json()["data"]["children"]
        return [{
            "ticker": ticker,
            "subreddit": post["data"].get("subreddit", ""),
            "title": post["data"].get("title", ""),
            "score": post["data"].get("score", 0),
            "created_utc": datetime.utcfromtimestamp(post["data"].get("created_utc")).isoformat(),
            "link": "https://reddit.com" + post["data"].get("permalink", "")
        } for post in posts]
    except Exception as e:
        print(f"❌ Error for {ticker}: {e}")
        return []

all_data = []
for ticker in tickers:
    print(f"🔎 Fetching posts for: {ticker}")
    all_data.extend(fetch_posts(ticker))
    time.sleep(2)

with open("reddit_raw_posts.json", "w") as f:
    json.dump(all_data, f, indent=2)

print(f"✅ Saved {len(all_data)} posts to reddit_raw_posts.json")

