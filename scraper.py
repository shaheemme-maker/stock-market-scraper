import os
import time
import json
from datetime import datetime
import yfinance as yf
from supabase import create_client, Client

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ CRITICAL ERROR: GitHub Secrets are missing.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_and_store():
    print("--- 🚀 Starting Smart Scrape + Metadata Caching ---")
    
    # 1. Fetch Symbols AND Metadata (Company Name, Market)
    all_stocks = [] # List of dictionaries: {symbol, name, market}
    start = 0
    fetch_size = 1000
    
    while True:
        # Fetching specific columns to pass to the frontend
        response = supabase.table("stock_profiles").select("symbol, name, market").range(start, start + fetch_size - 1).execute()
        rows = response.data
        if not rows: break
        
        for r in rows:
            all_stocks.append({
                "symbol": r['symbol'],
                "company_name": r.get('name', ''), # Handle missing names
                "market": r.get('market', 'US')    # Default to US
            })
            
        start += fetch_size
    
    print(f"✅ Found {len(all_stocks)} stocks.")
    
    # 2. Process Batch
    BATCH_SIZE = 50 
    total_upserted = 0
    
    # --- CACHE STORAGE ---
    latest_prices_cache = [] 
    history_cache = {} 

    # Create a map for easy lookup of metadata by symbol
    metadata_map = {s['symbol']: s for s in all_stocks}
    
    # Extract just the list of symbols for Yahoo
    symbol_list = [s['symbol'] for s in all_stocks]

    for batch in chunks(symbol_list, BATCH_SIZE):
        try:
            yahoo_map = {s: s.replace('.', '-') if 'NS' not in s else s for s in batch}
            tickers_for_yahoo = " ".join(yahoo_map.values())
            
            # Fetch 5 days history
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
                    yahoo_symbol = yahoo_map[symbol]
                    if len(batch) == 1: stock_df = data
                    else:
                        if yahoo_symbol not in data.columns.levels[0]: continue
                        stock_df = data[yahoo_symbol]
                    
                    if stock_df.empty: continue
                    stock_df = stock_df.dropna(subset=['Close'])
                    if stock_df.empty: continue

                    # --- 1. LATEST PRICE LOGIC ---
                    last_candle = stock_df.iloc[-1]
                    current_price = float(last_candle['Close'])
                    current_time = last_candle.name 
                    
                    prev_data = stock_df[stock_df.index.normalize() < current_time.normalize()]
                    prev_close = float(prev_data.iloc[-1]['Close']) if not prev_data.empty else float(stock_df.iloc[0]['Open'])

                    change_value = current_price - prev_close
                    change_pct = (change_value / prev_close) * 100 if prev_close != 0 else 0.0

                    # Retrieve metadata for this stock
                    meta = metadata_map.get(symbol, {})

                    data_point = {
                        "symbol": symbol,
                        "company_name": meta.get('company_name', ''), # <--- ADDED THIS
                        "market": meta.get('market', 'US'),           # <--- ADDED THIS
                        "price": round(current_price, 2),
                        "change_percent": round(change_pct, 2),
                        "change_value": round(change_value, 2),
                        "recorded_at": current_time.to_pydatetime().isoformat()
                    }
                    payload.append(data_point)
                    latest_prices_cache.append(data_point)

                    # --- 2. HISTORY CHART LOGIC ---
                    history_df = stock_df.tail(90)
                    chart_data = []
                    for index, row in history_df.iterrows():
                        ts = int(index.timestamp())
                        price = round(float(row['Close']), 2)
                        chart_data.append([ts, price])
                    
                    history_cache[symbol] = chart_data

                except Exception:
                    continue

            # (Optional) We still save to Supabase as backup, but frontend won't use it
            if payload:
                # We only upsert the price fields to keep DB clean
                db_payload = [{k: v for k, v in p.items() if k in ['symbol', 'price', 'change_percent', 'change_value', 'recorded_at']} for p in payload]
                supabase.table("stock_prices").upsert(db_payload, on_conflict="symbol, recorded_at", ignore_duplicates=False).execute()
                total_upserted += len(payload)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Batch failed: {e}")

    # --- SAVE FILES ---
    print("💾 Saving JSON Caches...")
    
    with open('latest_prices.json', 'w') as f:
        json.dump(latest_prices_cache, f)

    with open('history.json', 'w') as f:
        json.dump(history_cache, f)
    
    try: supabase.rpc("delete_old_prices").execute()
    except: pass
    
    print(f"--- 🏁 Completed. Upserted {total_upserted} candles. generated JSON files. ---")

if __name__ == "__main__":
    fetch_and_store()
