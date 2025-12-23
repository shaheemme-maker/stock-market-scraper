import os
import time
import math
from datetime import datetime
import yfinance as yf
import pandas as pd # Needed for date manipulation
from supabase import create_client, Client

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ CRITICAL ERROR: GitHub Secrets are missing.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- HELPER: SPLIT LIST INTO CHUNKS ---
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_and_store():
    print("--- 🚀 Starting Smart Scrape (Corrected Calculations) ---")
    
    # 1. Fetch Symbols
    all_symbols = []
    start = 0
    fetch_size = 1000
    
    while True:
        response = supabase.table("stock_profiles").select("symbol").range(start, start + fetch_size - 1).execute()
        rows = response.data
        if not rows: break
        all_symbols.extend([r['symbol'] for r in rows])
        start += fetch_size
    
    print(f"✅ Found {len(all_symbols)} stocks.")
    
    # 2. Process Batch
    BATCH_SIZE = 50 
    total_upserted = 0

    for batch in chunks(all_symbols, BATCH_SIZE):
        try:
            # Handle BRK.B mapping
            yahoo_map = {s: s.replace('.', '-') if 'NS' not in s else s for s in batch}
            tickers_for_yahoo = " ".join(yahoo_map.values())
            print(f"Checking batch: {batch[:3]}...")
            
            # Fetch 5 days to ensure we can find "Yesterday's Close"
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

                    # Drop NaN rows
                    stock_df = stock_df.dropna(subset=['Close'])
                    if stock_df.empty: continue

                    # --- CORRECTED CALCULATION LOGIC ---
                    
                    # 1. Get the Current Price (Last available candle)
                    last_candle = stock_df.iloc[-1]
                    current_price = float(last_candle['Close'])
                    current_time = last_candle.name # This is a timezone-aware Timestamp
                    
                    # 2. Find Previous Day's Close
                    # We filter the dataframe to find rows strictly BEFORE the current day
                    # normalize() sets the time to 00:00:00, effectively comparing dates
                    prev_data = stock_df[stock_df.index.normalize() < current_time.normalize()]
                    
                    if not prev_data.empty:
                        # The last row of 'prev_data' is the closing candle of the previous trading session
                        prev_close = float(prev_data.iloc[-1]['Close'])
                    else:
                        # Fallback: If no history exists (e.g. IPO today), compare to Today's Open
                        # This avoids "division by zero" or "variable undefined" errors
                        prev_close = float(stock_df.iloc[0]['Open'])

                    # 3. Calculate Change
                    change_value = current_price - prev_close
                    change_pct = 0.0
                    if prev_close != 0:
                        change_pct = (change_value / prev_close) * 100

                    # -----------------------------------

                    payload.append({
                        "symbol": symbol,
                        "price": round(current_price, 2),
                        "change_percent": round(change_pct, 2),
                        "change_value": round(change_value, 2),
                        "recorded_at": current_time.to_pydatetime().isoformat()
                    })
                    
                except Exception as inner_e:
                    # print(f"Error on {symbol}: {inner_e}") # Uncomment for deep debugging
                    continue

            if payload:
                # Upsert to prevent duplicate data for the same timestamp
                supabase.table("stock_prices").upsert(payload, on_conflict="symbol, recorded_at", ignore_duplicates=True).execute()
                total_upserted += len(payload)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Batch failed: {e}")

    print(f"--- 🏁 Completed. Checked/Upserted {total_upserted} candles. ---")
    
    try: supabase.rpc("delete_old_prices").execute()
    except: pass

if __name__ == "__main__":
    fetch_and_store()
