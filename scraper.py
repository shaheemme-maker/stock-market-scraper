import yfinance as yf
import json
import time
import os
import pytz
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client, Client

# --- CONFIGURATION ---
OUTPUT_DIR = "data"

# 1. ENTER YOUR SUPABASE CREDENTIALS HERE
#    (Or set them as environment variables: export SUPABASE_URL=...)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "YOUR_SUPABASE_URL_HERE")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "YOUR_SUPABASE_KEY_HERE")
DB_TABLE_NAME = "stock_profile" # Ensure this matches your table name exactly
DB_SYMBOL_COLUMN = "symbol"     # Ensure this matches your column name (e.g., 'ticker' or 'symbol')

def get_db_symbols():
    """Fetches all stock symbols from Supabase, handling pagination > 1000 rows"""
    if "YOUR_SUPABASE" in SUPABASE_URL:
        print("⚠️  WARNING: Supabase credentials not set. Using fallback list.")
        return ["AAPL", "GOOGL", "MSFT"] # Fallback if no credentials

    print("Connecting to Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        all_symbols = []
        start = 0
        batch_size = 1000 # Supabase default limit is 1000
        
        while True:
            # Fetch in batches
            response = supabase.table(DB_TABLE_NAME)\
                .select(DB_SYMBOL_COLUMN)\
                .range(start, start + batch_size - 1)\
                .execute()
            
            rows = response.data
            if not rows:
                break
                
            # Extract symbols from rows
            batch_symbols = [row[DB_SYMBOL_COLUMN] for row in rows if row[DB_SYMBOL_COLUMN]]
            all_symbols.extend(batch_symbols)
            
            if len(rows) < batch_size:
                break
                
            start += batch_size
            
        print(f"✅ Successfully fetched {len(all_symbols)} symbols from Supabase.")
        return all_symbols

    except Exception as e:
        print(f"❌ Error fetching from Supabase: {e}")
        return []

def fetch_data(symbols):
    if not symbols:
        print("No symbols to fetch.")
        return [], {}

    print(f"Fetching market data for {len(symbols)} symbols...")
    
    # Download data for all symbols at once (1 day period, 5 minute interval)
    # Using 'threads=True' for faster downloading
    # Chunking: If list is huge (1200+), yfinance handles it, but splitting helps avoid URL length errors.
    chunk_size = 500
    all_tickers_data = {}
    
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        print(f"  Downloading chunk {i} to {i+len(chunk)}...")
        
        try:
            data = yf.download(
                tickers=" ".join(chunk), 
                period="1d", 
                interval="5m", 
                group_by='ticker', 
                threads=True,
                progress=False
            )
            
            # Merge chunk data into main dictionary
            # yfinance returns a DataFrame with MultiIndex if >1 ticker, or single DF if 1 ticker
            if len(chunk) == 1:
                all_tickers_data[chunk[0]] = data
            else:
                for sym in chunk:
                    # Check if symbol exists in columns (handling delisted/errors)
                    if sym in data.columns.levels[0]: 
                         all_tickers_data[sym] = data[sym]
        except Exception as e:
            print(f"  Error downloading chunk: {e}")

    latest_prices = []
    history_data = {}
    
    current_time = datetime.now(pytz.utc).isoformat()
    ny_tz = pytz.timezone('America/New_York')
    
    # Process each symbol
    for symbol in symbols:
        try:
            if symbol not in all_tickers_data:
                # This happens if yfinance failed to find the symbol (delisted, wrong ticker)
                # We skip it silently or log a small warning
                continue
                
            ticker_data = all_tickers_data[symbol].copy()
            
            # Skip if empty
            if ticker_data.empty or len(ticker_data) == 0:
                continue

            # --- DATA NORMALIZATION (Resampling) ---
            if ticker_data.index.tz is None:
                ticker_data.index = ticker_data.index.tz_localize(ny_tz)
            else:
                ticker_data.index = ticker_data.index.tz_convert(ny_tz)

            market_date = ticker_data.index[0].normalize()
            market_open = market_date + timedelta(hours=9, minutes=30)
            market_close = market_date + timedelta(hours=16, minutes=0)

            full_schedule = pd.date_range(
                start=market_open, 
                end=market_close, 
                freq='5min', 
                tz=ny_tz
            )

            ticker_data = ticker_data.reindex(full_schedule)
            ticker_data = ticker_data.ffill()
            ticker_data = ticker_data.bfill()

            now_ny = datetime.now(ny_tz)
            ticker_data = ticker_data[ticker_data.index <= now_ny]
            # --- END NORMALIZATION ---

            if len(ticker_data) == 0:
                continue

            last_point = ticker_data.iloc[-1]
            current_price = float(last_point['Close'])
            open_price = float(ticker_data.iloc[0]['Open']) 
            
            price_change = current_price - open_price
            percent_change = (price_change / open_price) * 100 if open_price != 0 else 0
            
            stock_info = {
                "symbol": symbol,
                "company_name": symbol, # Ideally fetch this from DB too if available
                "price": round(current_price, 2),
                "change_value": round(price_change, 2),
                "change_percent": round(percent_change, 2),
                "previous_close": round(open_price, 2), 
                "market": "US",
                "last_updated": current_time
            }
            
            latest_prices.append(stock_info)
            
            start_timestamp = int(ticker_data.index[0].timestamp())
            prices_list = [round(p, 2) for p in ticker_data['Close'].tolist()]
            
            history_data[symbol] = {
                "s": start_timestamp,
                "p": prices_list
            }
            
        except Exception as e:
            # print(f"Error processing {symbol}: {e}") # Uncomment to debug specific stocks
            continue
            
    return latest_prices, history_data

def save_json(filename, data):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f)
    print(f"Saved {filename}")

def main():
    print(f"Starting scrape at {datetime.now()}")
    
    # 1. Fetch symbols from Supabase
    symbols = get_db_symbols()
    
    if not symbols:
        print("❌ No symbols found in DB or connection failed.")
        return

    try:
        # 2. Scrape data for those symbols
        latest_prices, history = fetch_data(symbols)
        
        if latest_prices:
            save_json('latest_prices.json', latest_prices)
            save_json('history.json', history)
            
            with open(os.path.join(OUTPUT_DIR, 'last_update.txt'), 'w') as f:
                f.write(datetime.now().isoformat())
                
            print(f"✅ Scrape completed. Updated {len(latest_prices)} stocks.")
        else:
            print("⚠️ Scrape finished but no data was returned.")
        
    except Exception as e:
        print(f"❌ Scrape failed: {e}")

if __name__ == "__main__":
    main()
