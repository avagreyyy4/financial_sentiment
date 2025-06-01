from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Load DB credentials from .env
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", 5432)

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT ticker_name FROM tickers ORDER BY ticker_name;")
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return render_template('index.html', tickers=tickers)

@app.route('/ticker/<symbol>')
def ticker_page(symbol):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT text, created_at
        FROM tweets t
        JOIN tickers tk ON t.ticker_id = tk.id
        WHERE tk.ticker_name = %s
        ORDER BY created_at DESC
        LIMIT 10;
    """, (symbol,))
    tweets = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('ticker.html', symbol=symbol, tweets=tweets)

if __name__ == '__main__':
    app.run(debug=True)

