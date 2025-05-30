import json
import pandas as pd
from datetime import datetime
import re

# Load raw scraped JSON file
with open("reddit_raw_posts.json", "r") as f:
    all_posts = json.load(f)

# ✅ Define your stock tickers
tickers = ["NVDA", "AMZN", "MSFT", "PLTR", "AKRO"]

# ✅ Subreddits that are financial-focused
allowed_subreddits = {
    "stocks", "investing", "wallstreetbets", "StockMarket", "financialindependence",
    "OptionsTrading", "OptionsMillionaire", "StockOptionsAlerts", "HenryZhang",
    "TickerTalkByLiam", "GammaEdge", "ChartNavigators", "Palantir_Investors",
    "InnerCircleInvesting", "InvestmentGemsGroup", "spy", "Optionmillionaires",
    "getagraph", "NvidiaStock", "Stock_Picks"
}

# ✅ Keywords indicating a finance-related post
finance_keywords = [
    "stock", "trade", "options", "calls", "puts", "position", "price", "target",
    "buy", "sell", "signal", "earnings", "portfolio", "squeeze", "short", "premarket"
]

def is_likely_financial_post(post):
    # Must be in an allowed financial subreddit
    if post["subreddit"] not in allowed_subreddits:
        return False

    # Must have one of the finance-related keywords
    title = post["title"].lower()
    if not any(keyword in title for keyword in finance_keywords):
        return False

    return True

# ✅ Now filter the posts
filtered_rows = []

for post in all_posts:
    title = post.get("title", "")
    subreddit = post.get("subreddit", "")
    created_utc = post.get("created_utc")
    score = post.get("score", 0)
    link = post.get("link", "")
    
    # Ignore posts with no title or broken link
    if not title or not link:
        continue

    for ticker in tickers:
        # Use strict ticker pattern like $NVDA or NVDA surrounded by word boundaries
        if re.search(rf"\b\$?{ticker}\b", title, re.IGNORECASE):
            post_data = {
                "ticker": ticker,
                "subreddit": subreddit,
                "title": title,
                "score": score,
                "created_utc": created_utc,
                "link": link
            }

            if is_likely_financial_post(post_data):
                filtered_rows.append(post_data)
            break  # prevent double-counting same post for multiple tickers

# ✅ Output to a clean CSV
df = pd.DataFrame(filtered_rows)
df.to_csv("reddit_filtered_financial_posts.csv", index=False)
print(f"✅ Saved {len(df)} finance-related posts to reddit_filtered_financial_posts.csv")

