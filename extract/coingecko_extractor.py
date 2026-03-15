import requests
import pandas as pd
from datetime import datetime
import os

BASE_URL = "https://api.coingecko.com/api/v3"

def fetch_top_coins(limit=50):
    print(f"Fetching top {limit} coins from CoinGecko...")

    url = f"{BASE_URL}/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": limit,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return None

    data = response.json()
    # print(data)
    print(f"Successfully fetched {len(data)} coins.")
    return data
# data = fetch_top_coins()
# print(data)
def parse_coins(raw_data):
    print("Parsing data...")
    records = []

    for coin in raw_data:
        records.append({
            "coin_id":          coin["id"],
            "symbol":           coin["symbol"].upper(),
            "name":             coin["name"],
            "current_price":    coin["current_price"],
            "market_cap":       coin["market_cap"],
            "market_cap_rank":  coin["market_cap_rank"],
            "total_volume":     coin["total_volume"],
            "high_24h":         coin["high_24h"],
            "low_24h":          coin["low_24h"],
            "price_change_24h": coin["price_change_24h"],
            "pct_change_1h":    coin.get("price_change_percentage_1h_in_currency"),
            "pct_change_24h":   coin.get("price_change_percentage_24h_in_currency"),
            "pct_change_7d":    coin.get("price_change_percentage_7d_in_currency"),
            "circulating_supply": coin["circulating_supply"],
            "ath":              coin["ath"],
            "ath_change_pct":   coin["ath_change_percentage"],
            "extracted_at":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        })
    df = pd.DataFrame(records)
    print(f"Parsed {len(df)} coins successfully.")
    return df
def save_raw(df):
    os.makedirs("data/raw", exist_ok=True)
    filename = f"data/raw/crypto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved to: {filename}")
    return filename

if __name__ == "__main__":
    raw = fetch_top_coins(limit=50)

    if raw:
        df = parse_coins(raw)
        save_raw(df)

        print("\n--- Preview (top 10 coins) ---")
        print(df[["symbol", "name", "current_price",
                   "pct_change_24h", "market_cap"]].head(10).to_string(index=False))
