import os
import time
import json
import pandas as pd
import yfinance as yf
from datetime import datetime
from supabase import create_client, Client
import numpy as np

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ⚡ INTERVAL SETTING ⚡
# "5min" = 5 minute candles
# This controls the resolution of your history files
RESAMPLE_INTERVAL = "5min" 

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ CRITICAL ERROR: GitHub Secrets are missing.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def chunks(lst, n):
    """Helper to break list into smaller batches"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_and_store():
    print("--- 🚀 Starting Smart Scrape + Sharding + Crypto Support ---")
    
    # 1. Fetch Symbols & Metadata
    all_stocks = [] 
    start = 0
    fetch_size = 1000
    
    print("Fetching stock profiles from Supabase...")
    while True:
        try:
            response = supabase.table("stock_profiles").select("symbol, company_name, market").range(start, start + fetch_size - 1).execute()
            rows = response.data
            if not rows: break
            for r in rows:
                all_stocks.append({
                    "symbol": r['symbol'],
                    "company_name": r.get('company_name', r.get('symbol')),
                    "market": r.get('market', 'US')
                })
            start += fetch_size
        except Exception as e:
            print(f"⚠️ Error fetching profiles: {e}")
            break
    
    print(f"✅ Found {len(all_stocks)} stocks/cryptos.")
    
    # 2. Process Batch
    BATCH_SIZE = 100 
    total_upserted = 0
    
    latest_prices_cache = [] 
    history_cache = {} 
    metadata_map = {s['symbol']: s for s in all_stocks}
    symbol_list = [s['symbol'] for s in all_stocks]

    try: 
        for batch in chunks(symbol_list, BATCH_SIZE):
            try:
                tickers_for_yahoo = " ".join(batch)
                
                # Download 5 days buffer to ensure we catch the current day
                # (Crypto is 24/7, stocks are 9-4, this covers all cases)
                data = yf.download(
                    tickers_for_yahoo, 
                    period="5d",
                    interval="5m", 
                    progress=False, 
                    group_by='ticker',
                    threads=True,
                    auto_adjust=False 
                )
                
                payload = []
                
                for symbol in batch:
                    try:
                        yahoo_symbol = symbol 
                        # Handle Multi-Index columns from yfinance
                        if len(batch) == 1: stock_df = data
                        else:
                            if yahoo_symbol not in data.columns.levels[0]: continue
                            stock_df = data[yahoo_symbol]
                        
                        if stock_df.empty: continue
                        stock_df = stock_df.dropna(subset=['Close'])
                        if stock_df.empty: continue

                        # --- 1. METADATA & MARKET TYPE LOGIC ---
                        meta = metadata_map.get(symbol, {})
                        market_type = meta.get('market', 'US')
                        
                        # Auto-detect Crypto if not explicitly set in DB
                        if (market_type == 'US') and ('-USD' in symbol or 'BTC' in symbol or 'ETH' in symbol):
                            market_type = 'CRYPTO'

                        # --- 2. LATEST PRICE LOGIC ---
                        last_candle = stock_df.iloc[-1]
                        current_price = float(last_candle['Close'])
                        current_time = last_candle.name 
                        
                        # Calculate daily change
                        # For Crypto, "prev_close" is technically 00:00 UTC start price
                        prev_data = stock_df[stock_df.index.normalize() < current_time.normalize()]
                        prev_close = float(prev_data.iloc[-1]['Close']) if not prev_data.empty else float(stock_df.iloc[0]['Open'])
                        
                        change_value = current_price - prev_close
                        change_pct = ((change_value) / prev_close * 100) if prev_close != 0 else 0.0

                        # Data point for Supabase & latest_prices.json
                        data_point = {
                            "symbol": symbol,
                            "company_name": meta.get('company_name', symbol),
                            "market": market_type,
                            "price": round(current_price, 2),
                            "change_percent": round(change_pct, 2),
                            "change_value": round(change_value, 2),
                            "recorded_at": current_time.to_pydatetime().isoformat(),
                            "previous_close": round(prev_close, 2) # Helpful for frontend
                        }

                        payload.append(data_point) # For Supabase
                        latest_prices_cache.append(data_point) # For JSON

                        # --- 3. HISTORY CHART (OPTIMIZED + AUTO-RESET) ---
                        
                        # A. Filter: STRICTLY TODAY ONLY
                        # This drops yesterday's data instantly when a new day starts
                        last_ts = stock_df.index[-1]
                        todays_data = stock_df[stock_df.index.normalize() == last_ts.normalize()].copy()
                        
                        # B. Resample: Fix gaps (e.g. missing 5 mins) so implicit indexing works
                        # This ensures chart width is correct even if volume is low
                        todays_data = todays_data.resample(RESAMPLE_INTERVAL).asfreq()
                        
                        # C. Format: { s: timestamp, p: [price, price...] }
                        if not todays_data.empty:
                            start_timestamp = int(todays_data.index[0].timestamp())
                            
                            # Create list of prices (None for gaps)
                            prices_list = [
                                round(x, 2) if pd.notna(x) else None 
                                for x in todays_data['Close'].tolist()
                            ]

                            history_cache[symbol] = {
                                "s": start_timestamp,     
                                "p": prices_list          
                            }

                    except Exception as inner_e:
                        continue

                # --- UPSERT TO SUPABASE ---
                if payload:
                    db_payload = [{
                        "symbol": p["symbol"],
                        "price": p["price"],
                        "change_percent": p["change_percent"],
                        "change_value": p["change_value"],
                        "recorded_at": p["recorded_at"]
                    } for p in payload]

                    supabase.table("stock_prices").upsert(
                        db_payload, on_conflict="symbol, recorded_at", ignore_duplicates=False
                    ).execute()
                    total_upserted += len(payload)
                
                time.sleep(0.5)

            except Exception as e:
                print(f"⚠️ Batch failed: {e}")

    except KeyboardInterrupt:
        print("\n🛑 Interrupted.")
    except Exception as e:
        print(f"\n❌ Script crashed: {e}")
    finally:
        # --- SAVE JSON FILES (ALWAYS RUNS) ---
        print("\n💾 Saving JSON Files...")
        
        # 1. Save Latest Prices (One big file is fine for lists/search)
        try:
            with open('latest_prices.json', 'w') as f:
                json.dump(latest_prices_cache, f)
            print("✅ latest_prices.json saved.")
        except Exception as e:
            print(f"❌ Error saving latest_prices: {e}")

        # 2. SHARDING HISTORY LOGIC
        print("⚡ Sharding history files...")
        shards = {}
        
        for symbol, data in history_cache.items():
            # Get first character (e.g., 'A' from 'AAPL', 'B' from 'BTC-USD')
            first_char = symbol[0].upper()
            
            # Determine bucket name
            if first_char.isalpha():
                shard_name = f"history_{first_char}" # history_A.json
            else:
                shard_name = "history_0-9" # history_0-9.json
            
            if shard_name not in shards:
                shards[shard_name] = {}
            
            shards[shard_name][symbol] = data

        # 3. Save Shards
        for shard_name, shard_data in shards.items():
            filename = f"{shard_name}.json"
            try:
                with open(filename, 'w') as f:
                    # separators removes whitespace to save bytes
                    json.dump(shard_data, f, separators=(',', ':'))
            except Exception as e:
                print(f"❌ Error saving {filename}: {e}")
        
        print(f"✅ Saved {len(shards)} history shards.")

        # --- CLEANUP ---
        print("🧹 Cleaning up old database entries...")
        try: 
            supabase.rpc("delete_old_prices").execute()
        except: pass
        
        print(f"--- 🏁 Done. Upserted {total_upserted} candles. ---")

if __name__ == "__main__":
    fetch_and_store()
