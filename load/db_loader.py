import pandas as pd
import sqlite3
import glob
import os
from datetime import datetime

DB_PATH = "data/crypto.db"

def get_latest_processed():
    files = glob.glob("data/processed/crypto_latest.csv")
    if not files:
        raise FileNotFoundError("No processed data found. Run cleaner.py first.")
    latest = max(files, key=os.path.getctime)
    print(f"Loading: {latest}")
    return pd.read_csv(latest)

def create_tables(conn):
    """Create tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS coins (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id         TEXT,
            symbol          TEXT,
            name            TEXT,
            current_price   REAL,
            market_cap      REAL,
            market_cap_rank INTEGER,
            total_volume    REAL,
            high_24h        REAL,
            low_24h         REAL,
            price_change_24h REAL,
            pct_change_1h   REAL,
            pct_change_24h  REAL,
            pct_change_7d   REAL,
            circulating_supply REAL,
            ath             REAL,
            ath_change_pct  REAL,
            market_dominance_pct  REAL,
            volatility_score      REAL,
            volume_to_mcap_ratio  REAL,
            momentum_signal       TEXT,
            ath_distance_pct      REAL,
            coin_category         TEXT,
            price_position        TEXT,
            extracted_at    TEXT,
            loaded_at       TEXT
        )
    """)
    conn.commit()
    print("Tables ready.")

def load_to_db(df):
    """Incrementally load — skip coins already loaded for this timestamp."""
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    df["loaded_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Avoid duplicate loads — check if this extract batch already exists
    existing = pd.read_sql(
        "SELECT DISTINCT extracted_at FROM coins", conn
    )["extracted_at"].tolist()

    new_rows = df[~df["extracted_at"].isin(existing)]

    if new_rows.empty:
        print("No new data to load — already up to date.")
    else:
        new_rows.to_sql("coins", conn, if_exists="append", index=False)
        print(f"Loaded {len(new_rows)} new rows into database.")

    conn.close()
    return len(new_rows)

def run_sql_analysis():
    """Run analyst SQL queries and print results."""
    conn = sqlite3.connect(DB_PATH)

    print("\n" + "="*50)
    print("SQL ANALYSIS REPORT")
    print("="*50)

    # Query 1 — Top 10 by market cap
    print("\n1. Top 10 coins by market cap:")
    q1 = pd.read_sql("""
        SELECT symbol, name, current_price,
               ROUND(market_dominance_pct, 2) AS dominance_pct,
               coin_category
        FROM coins
        ORDER BY market_cap DESC
        LIMIT 10
    """, conn)
    print(q1.to_string(index=False))

    # Query 2 — Window function: rank coins by 24h % change within each category
    print("\n2. Best performer per category (window function):")
    q2 = pd.read_sql("""
        SELECT *
FROM (
    SELECT symbol, coin_category, pct_change_24h,
           RANK() OVER (
               PARTITION BY coin_category
               ORDER BY pct_change_24h DESC
           ) AS rank_in_category
    FROM coins
)
WHERE rank_in_category = 1;
    """, conn)
    print(q2.to_string(index=False))

    # Query 3 — Momentum signal breakdown
    print("\n3. Momentum signal summary:")
    q3 = pd.read_sql("""
        SELECT momentum_signal,
               COUNT(*) AS coin_count,
               ROUND(AVG(pct_change_24h), 2) AS avg_24h_change,
               ROUND(AVG(volatility_score), 2) AS avg_volatility
        FROM coins
        GROUP BY momentum_signal
        ORDER BY coin_count DESC
    """, conn)
    print(q3.to_string(index=False))

    # Query 4 — CTE: coins near 24h high with strong momentum
    print("\n4. Coins near 24h high with buy signal (CTE):")
    q4 = pd.read_sql("""
        WITH strong_coins AS (
            SELECT symbol, name, current_price,
                   pct_change_24h, volatility_score,
                   momentum_signal, price_position
            FROM coins
            WHERE momentum_signal IN ('buy', 'strong_buy')
        )
        SELECT * FROM strong_coins
        WHERE price_position = 'near_high'
        ORDER BY pct_change_24h DESC
        LIMIT 5
    """, conn)
    print(q4.to_string(index=False))

    # Query 5 — Most volatile small caps
    print("\n5. Most volatile small cap coins:")
    q5 = pd.read_sql("""
        SELECT symbol, name,
               ROUND(volatility_score, 2) AS volatility,
               ROUND(pct_change_24h, 2)   AS change_24h,
               ROUND(volume_to_mcap_ratio, 4) AS vol_ratio
        FROM coins
        WHERE coin_category = 'small_cap'
        ORDER BY volatility_score DESC
        LIMIT 5
    """, conn)
    print(q5.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    df = get_latest_processed()
    load_to_db(df)
    run_sql_analysis()