import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

def load_latest_raw():
    """Load the most recently extracted CSV file."""
    files = glob.glob("data/raw/crypto_*.csv")
    if not files:
        raise FileNotFoundError("No raw data found. Run extractor first.")
    latest = max(files, key=os.path.getctime)
    print(f"Loading: {latest}")
    df = pd.read_csv(latest)
    return df

def clean_data(df):
    """Fix data types, handle nulls, remove bad rows."""
    print("Cleaning data...")

    # Drop rows where essential fields are missing
    df = df.dropna(subset=["current_price", "market_cap", "symbol"])

    # Fill missing % change values with 0
    for col in ["pct_change_1h", "pct_change_24h", "pct_change_7d"]:
        df[col] = df[col].fillna(0).round(2)

    # Ensure correct data types
    df["current_price"]   = pd.to_numeric(df["current_price"],   errors="coerce")
    df["market_cap"]      = pd.to_numeric(df["market_cap"],      errors="coerce")
    df["total_volume"]    = pd.to_numeric(df["total_volume"],     errors="coerce")
    df["extracted_at"]    = pd.to_datetime(df["extracted_at"])

    # Remove any duplicate coins
    df = df.drop_duplicates(subset=["coin_id"])

    print(f"Clean data shape: {df.shape}")
    return df

def add_analyst_metrics(df):
    """Derive business metrics — this is the analyst value-add."""
    print("Adding analyst metrics...")

    total_market_cap = df["market_cap"].sum()

    # 1. Market dominance % — what share of total market each coin holds
    df["market_dominance_pct"] = (
        (df["market_cap"] / total_market_cap) * 100
    ).round(4)

    # 2. Volatility score — 24h price range as % of current price
    df["volatility_score"] = (
        ((df["high_24h"] - df["low_24h"]) / df["current_price"]) * 100
    ).round(2)

    # 3. Volume to market cap ratio — how actively traded vs its size
    df["volume_to_mcap_ratio"] = (
        df["total_volume"] / df["market_cap"]
    ).round(4)

    # 4. Momentum signal — based on 1h, 24h, 7d trend direction
    def momentum(row):
        score = 0
        if row["pct_change_1h"]  > 0: score += 1
        if row["pct_change_24h"] > 0: score += 1
        if row["pct_change_7d"]  > 0: score += 1
        if score == 3:   return "strong_buy"
        elif score == 2: return "buy"
        elif score == 1: return "neutral"
        else:            return "sell"

    df["momentum_signal"] = df.apply(momentum, axis=1)

    # 5. Distance from all-time high %
    df["ath_distance_pct"] = df["ath_change_pct"].round(2)

    # 6. Coin category — based on market cap size
    def categorize(mcap):
        if mcap >= 10_000_000_000:   return "large_cap"
        elif mcap >= 1_000_000_000:  return "mid_cap"
        else:                        return "small_cap"

    df["coin_category"] = df["market_cap"].apply(categorize)

    # 7. Price range flag — is coin near 24h high or low?
    def price_position(row):
        if row["high_24h"] == row["low_24h"]:
            return "stable"
        position = (row["current_price"] - row["low_24h"]) / (row["high_24h"] - row["low_24h"])
        if position >= 0.75:   return "near_high"
        elif position <= 0.25: return "near_low"
        else:                  return "mid_range"

    df["price_position"] = df.apply(price_position, axis=1)

    print("Metrics added successfully.")
    return df

def save_processed(df):
    """Save transformed data to processed folder."""
    os.makedirs("data/processed", exist_ok=True)
    filename = "data/processed/crypto_latest.csv"    
    df.to_csv(filename, index=False)
    print(f"Processed data saved to: {filename}")
    return filename

def run_transform():
    df = load_latest_raw()
    df = clean_data(df)
    df = add_analyst_metrics(df)
    save_processed(df)

    # Quick analyst summary
    print("\n--- Analyst Summary ---")
    print(f"Total coins:        {len(df)}")
    print(f"Total market cap:   ${df['market_cap'].sum():,.0f}")
    print(f"BTC dominance:      {df[df['symbol']=='BTC']['market_dominance_pct'].values[0]:.2f}%")
    print(f"Strong buy signals: {len(df[df['momentum_signal']=='strong_buy'])}")
    print(f"Sell signals:       {len(df[df['momentum_signal']=='sell'])}")
    print(f"\nTop 5 by volatility:")
    print(df.nlargest(5, "volatility_score")[["symbol", "volatility_score", "pct_change_24h"]].to_string(index=False))
    print(f"\nMomentum breakdown:")
    print(df["momentum_signal"].value_counts().to_string())

    return df

if __name__ == "__main__":
    run_transform()