import os
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="twitter_data",
    user="postgres",
    password="pass",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# Folders containing your tweet JSON files
folders = ["pull_2025_05_01", "pull_2025_05_30"]

# Helper function to insert or get ticker_id
def get_or_create_ticker_id(ticker):
    cur.execute("SELECT id FROM tickers WHERE ticker_name = %s", (ticker,))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO tickers (ticker_name) VALUES (%s) RETURNING id", (ticker,))
        return cur.fetchone()[0]

# Loop through files
for folder in folders:
    for filename in os.listdir(folder):
        if filename.endswith("_tweets.json"):
            ticker = filename.split("_")[0].upper()
            ticker_id = get_or_create_ticker_id(ticker)
            filepath = os.path.join(folder, filename)
            print(f"Processing {filepath} for ticker: {ticker}")

            with open(filepath, "r") as f:
                for line in f:
                    try:
                        tweet = json.loads(line)
                        cur.execute("""
                            INSERT INTO tweets (id, created_at, text, author_id, ticker_id)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING;
                        """, (
                            int(tweet["id"]),
                            tweet["created_at"],
                            tweet["text"],
                            int(tweet["author_id"]),
                            ticker_id
                        ))
                    except Exception as e:
                        print(f"Error on tweet {tweet.get('id', 'unknown')}: {e}")

conn.commit()
cur.close()
conn.close()
print("✅ Done loading all tweets with ticker IDs.")

