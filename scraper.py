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
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_and_store():
    print("--- 🚀 Starting Bulk Scrape ---")
    
    # 1. Fetch ALL symbols from the database (Pagination required for >1000 rows)
    all_symbols = []
    start = 0
    fetch_size = 1000
    
    print("Reading symbols from 'stock_profiles' table...")
    while True:
        # Fetching only the 'symbol' column to save bandwidth
        response = supabase.table("stock_profiles").select("symbol").range(start, start + fetch_size - 1).execute()
        rows = response.data
        if not rows:
            break
        all_symbols.extend([r['symbol'] for r in rows])
        start += fetch_size
    
    print(f"✅ Found {len(all_symbols)} stocks to track.")
    
    # 2. Process in batches
    # 50 is a safe batch size for yfinance to avoid missing data
    BATCH_SIZE = 50
    total_inserted = 0
    
    # Use one timestamp for the entire batch so charts line up perfectly
    record_time = datetime.utcnow().isoformat()

    for batch in chunks(all_symbols, BATCH_SIZE):
        try:
            tickers_str = " ".join(batch)
            print(f"Fetching batch: {batch[:3]}... (+{len(batch)-3} more)")
            
            # 'download' is faster for bulk than Ticker()
            # threads=True uses multiple cores to download faster
            data = yf.download(
                tickers_str, 
                period="4d", 
                interval="15m", 
                progress=False, 
                group_by='ticker',
                threads=True 
            )
            
            payload = []
            
            for symbol in batch:
                try:
                    # Handle single-stock result vs multi-stock result structure
                    if len(batch) == 1:
                        stock_df = data
                    else:
                        # Skip if symbol returned no data (delisted/error)
                        if symbol not in data.columns.levels[0]: 
                            continue
                        stock_df = data[symbol]
                    
                    if stock_df.empty:
                        continue

                    # Get the very last row (latest 15m candle)
                    last_candle = stock_df.iloc[-1]
                    
                    # Check for NaN (Not a Number) which happens if market is closed/no trade
                    if math.isnan(last_candle['Close']):
                        continue

                    price = float(last_candle['Close'])
                    
                    # Calculate change from the OPEN of this 15-minute candle
                    # (This gives instant intraday momentum)
                    open_price = float(last_candle['Open'])
                    
                    change_value = price - open_price
                    change_pct = 0.0
                    if open_price != 0:
                        change_pct = (change_value / open_price) * 100

                    payload.append({
                        "symbol": symbol,
                        "price": round(price, 2),
                        "change_percent": round(change_pct, 2),
                        "change_value": round(change_value, 2),
                        "recorded_at": record_time
                    })
                    
                except Exception as inner_e:
                    continue # One failure shouldn't crash the whole batch

            # 3. Insert Batch into Supabase
            if payload:
                # Using 'insert' because we want a history of prices
                supabase.table("stock_prices").insert(payload).execute()
                total_inserted += len(payload)
            
            # Tiny sleep to be nice to Yahoo's servers
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Batch failed: {e}")

    print(f"--- 🏁 Scrape Complete. Total Inserted: {total_inserted} ---")
    
    # 4. Maintenance: Delete data older than 48 hours to save space
    try:
        supabase.rpc("delete_old_prices").execute()
        print("🧹 Cleaned up old data.")
    except Exception as e:
        print(f"⚠️ Cleanup warning (ignore if function doesn't exist): {e}")

if __name__ == "__main__":
    fetch_and_store()
