import tweepy
import os
import json
from time import sleep
from datetime import datetime
from dotenv import load_dotenv

# Load token
load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

# Define tickers and tweet counts
tickers = ["PLTR", "AMZN", "NVDA", "MSFT", "PFE"]
tweets_per_ticker = 600
tweets_per_call = 100

# Basic spam filter
def is_likely_spam(text):
    spam_keywords = ["pumpfight", "join the", "signal", "airdrop", "LFG", "moon"]
    if any(word.lower() in text.lower() for word in spam_keywords):
        return True
    if text.count("$") > 4 or text.count("#") > 4:
        return True
    return False

# Folder for today's pull
folder = f"pull_{datetime.today().strftime('%Y_%m_%d')}"
os.makedirs(folder, exist_ok=True)

def scrape_tweets(ticker, max_total):
    query = f"({ticker}) lang:en -is:retweet"
    all_tweets = []
    next_token = None

    while len(all_tweets) < max_total:
        try:
            response = client.search_recent_tweets(
                query=query,
                max_results=tweets_per_call,
                tweet_fields=["created_at", "text", "author_id"],
                next_token=next_token
            )
        except tweepy.errors.TooManyRequests:
            print("Rate limited. Sleeping 15 min...")
            sleep(15 * 60)
            continue
        except Exception as e:
            print(f"Error for {ticker}: {e}")
            break

        if response.data:
            for tweet in response.data:
                if not is_likely_spam(tweet.text):
                    all_tweets.append(tweet.data)
            print(f"{ticker}: Collected {len(all_tweets)} tweets (non-spam)")
        else:
            print(f"{ticker}: No more tweets found.")
            break

        next_token = response.meta.get("next_token")
        if not next_token:
            break

    with open(f"{folder}/{ticker}_tweets.json", "w") as f:
        for tweet in all_tweets:
            json.dump(tweet, f)
            f.write("\n")

    print(f"Saved {len(all_tweets)} tweets for {ticker}.")

# Run for all tickers
for ticker in tickers:
    scrape_tweets(ticker, tweets_per_ticker)

