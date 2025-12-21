import os
import time
import math
from datetime import datetime
import yfinance as yf
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
    print("--- 🚀 Starting Bulk Scrape ---")
    
    # 1. Fetch Symbols
    all_symbols = []
    start = 0
    fetch_size = 1000
    
    print("Reading symbols from 'stock_profiles' table...")
    while True:
        response = supabase.table("stock_profiles").select("symbol").range(start, start + fetch_size - 1).execute()
        rows = response.data
        if not rows:
            break
        all_symbols.extend([r['symbol'] for r in rows])
        start += fetch_size
    
    print(f"✅ Found {len(all_symbols)} stocks to track.")
    
    # 2. Process in batches
    BATCH_SIZE = 50
    total_inserted = 0
    record_time = datetime.utcnow().isoformat()

    for batch in chunks(all_symbols, BATCH_SIZE):
        try:
            # FIX 1: Yahoo expects 'BRK-B', not 'BRK.B'. 
            # We create a map to send correct format to Yahoo but save original format to DB.
            yahoo_map = {s: s.replace('.', '-') if 'NS' not in s else s for s in batch}
            # (Note: We DON'T replace dot for Indian stocks like RELIANCE.NS, only US ones)
            
            tickers_for_yahoo = " ".join(yahoo_map.values())
            print(f"Fetching batch: {batch[:3]}... (+{len(batch)-3} more)")
            
            # FIX 2: Added auto_adjust=False to fix the warning and ensure stable column names
            data = yf.download(
                tickers_for_yahoo, 
                period="5d", 
                interval="15m", 
                progress=False, 
                group_by='ticker',
                threads=True,
                auto_adjust=False 
            )
            
            payload = []
            
            for symbol in batch:
                try:
                    yahoo_symbol = yahoo_map[symbol]
                    
                    # Handle structure differences
                    if len(batch) == 1:
                        stock_df = data
                    else:
                        if yahoo_symbol not in data.columns.levels[0]:
                            print(f"   ⚠️ No data found for {symbol}")
                            continue
                        stock_df = data[yahoo_symbol]
                    
                    if stock_df.empty:
                        print(f"   ⚠️ DataFrame empty for {symbol}")
                        continue

                    # FIX 3: Drop Empty Rows! 
                    # Sometimes Yahoo returns a row of NaNs at the end. We remove them.
                    stock_df = stock_df.dropna(subset=['Close'])

                    if stock_df.empty:
                         print(f"   ⚠️ All rows were NaN for {symbol}")
                         continue

                    # Get the absolute last VALID row
                    last_candle = stock_df.iloc[-1]
                    
                    price = float(last_candle['Close'])
                    
                    # Calculate change
                    open_price = float(last_candle['Open'])
                    change_value = price - open_price
                    change_pct = 0.0
                    if open_price != 0:
                        change_pct = (change_value / open_price) * 100

                    payload.append({
                        "symbol": symbol, # Store the original symbol (BRK.B), not Yahoo's (BRK-B)
                        "price": round(price, 2),
                        "change_percent": round(change_pct, 2),
                        "change_value": round(change_value, 2),
                        "recorded_at": record_time
                    })
                    
                except Exception as inner_e:
                    print(f"   ❌ Error processing {symbol}: {inner_e}")
                    continue

            if payload:
                supabase.table("stock_prices").insert(payload).execute()
                total_inserted += len(payload)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Batch failed: {e}")

    print(f"--- 🏁 Scrape Complete. Total Inserted: {total_inserted} ---")
    
    try:
        supabase.rpc("delete_old_prices").execute()
        print("🧹 Cleaned up old data.")
    except:
        pass

if __name__ == "__main__":
    fetch_and_store()
