import yfinance as yf
import json
import os
import pytz
import pandas as pd
from datetime import datetime, time as dt_time, timedelta
from supabase import create_client, Client

# --- CONFIGURATION ---
OUTPUT_DIR = "data"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "YOUR_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "YOUR_SUPABASE_KEY")
DB_TABLE_NAME = "stock_profiles"

# --- MARKET DEFINITIONS ---
# Keys match the 'market' column in your seeder.py
MARKET_CONFIG = {
    'US': {
        'timezone': 'America/New_York',
        'open': dt_time(9, 30),
        'close': dt_time(16, 0)
    },
    'INDIA': {
        'timezone': 'Asia/Kolkata',
        'open': dt_time(9, 15),
        'close': dt_time(15, 30)
    },
    'UK': {
        'timezone': 'Europe/London',
        'open': dt_time(8, 0),
        'close': dt_time(16, 30)
    },
    'GERMANY': {
        'timezone': 'Europe/Berlin',
        'open': dt_time(9, 0),
        'close': dt_time(17, 30)
    },
    'FRANCE': {
        'timezone': 'Europe/Paris',
        'open': dt_time(9, 0),
        'close': dt_time(17, 30)
    },
    'INDEX': {
        # Indices vary, but often follow US or local major market hours
        # Defaulting to US hours for simplicity, or add specific logic if needed
        'timezone': 'America/New_York', 
        'open': dt_time(9, 30),
        'close': dt_time(16, 0)
    },
    # Fallback
    'DEFAULT': {
        'timezone': 'UTC',
        'open': dt_time(9, 0),
        'close': dt_time(17, 0)
    }
}

def get_db_stocks():
    """
    Fetches symbol AND market from Supabase.
    Returns: List of dicts [{'symbol': 'AAPL', 'market': 'US'}, ...]
    """
    if "YOUR_SUPABASE" in SUPABASE_URL:
        print("⚠️  Credentials missing. Using fallback.")
        return [
            {'symbol': 'AAPL', 'market': 'US'},
            {'symbol': 'RELIANCE.NS', 'market': 'INDIA'}
        ]

    print(f"Connecting to Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        all_stocks = []
        start = 0
        batch_size = 1000
        
        while True:
            # Fetch symbol and market columns
            response = supabase.table(DB_TABLE_NAME)\
                .select("symbol, market")\
                .range(start, start + batch_size - 1)\
                .execute()
            
            rows = response.data
            if not rows: break
            
            # Filter valid rows
            batch_stocks = [
                {'symbol': row['symbol'], 'market': row.get('market', 'US')} 
                for row in rows if row.get('symbol')
            ]
            all_stocks.extend(batch_stocks)
            
            if len(rows) < batch_size: break
            start += batch_size
            
        print(f"✅ Fetched {len(all_stocks)} stocks with market tags.")
        return all_stocks
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return []

def process_single_ticker(symbol, market_tag, raw_df):
    if raw_df is None or raw_df.empty:
        return None

    # 1. Get Config from DB Tag
    config = MARKET_CONFIG.get(market_tag, MARKET_CONFIG['DEFAULT'])
    tz = pytz.timezone(config['timezone'])
    
    # 2. Localize DF
    if raw_df.index.tz is None:
        raw_df.index = raw_df.index.tz_localize('UTC').tz_convert(tz)
    else:
        raw_df.index = raw_df.index.tz_convert(tz)

    # 3. Filter for "Today" (Last available trading date)
    last_trade_time = raw_df.index[-1]
    trade_date = last_trade_time.normalize()

    # 4. Construct Perfect Grid
    market_open = tz.localize(datetime.combine(trade_date.date(), config['open']))
    market_close = tz.localize(datetime.combine(trade_date.date(), config['close']))
    
    full_schedule = pd.date_range(start=market_open, end=market_close, freq='5min')
    
    # 5. Reindex & Fill
    df_resampled = raw_df['Close'].reindex(full_schedule)
    
    # Forward fill valid prices
    df_resampled = df_resampled.ffill()
    
    # Handle Future: If today is active, nullify future points
    current_time_in_market = datetime.now(tz)
    if trade_date.date() == current_time_in_market.date():
        df_resampled[df_resampled.index > current_time_in_market] = None

    # 6. Calculate Stats
    valid_prices = df_resampled.dropna()
    if valid_prices.empty:
        return None
        
    current_price = valid_prices.iloc[-1]
    
    # Try to get Open price from raw data for "Previous Close" reference
    # Ideally, we want yesterday's close, but Open is a safe fallback for calculation
    try:
        prev_close = raw_df['Open'].iloc[0]
    except:
        prev_close = current_price

    change_value = current_price - prev_close
    change_percent = (change_value / prev_close) * 100 if prev_close != 0 else 0

    # 7. Format Output
    price_list = df_resampled.where(pd.notnull(df_resampled), None).tolist()
    
    return {
        "symbol": symbol,
        "company_name": symbol, # App can enhance this name later
        "price": round(current_price, 2),
        "change_value": round(change_value, 2),
        "change_percent": round(change_percent, 2),
        "previous_close": round(prev_close, 2),
        "market": market_tag, # Pass the tag back to frontend
        "last_updated": datetime.now(pytz.utc).isoformat(),
        "market_open_ts": int(market_open.timestamp()),
        "market_close_ts": int(market_close.timestamp()),
        "chart_data": price_list
    }

def main():
    print(f"Starting scrape at {datetime.now()}")
    
    # 1. Get List of Dicts [{'symbol': 'AAPL', 'market': 'US'}, ...]
    db_stocks = get_db_stocks()
    if not db_stocks: return

    # 2. Map Symbols to their Market for easy lookup later
    # Format: {'AAPL': 'US', 'RELIANCE.NS': 'INDIA'}
    stock_map = {item['symbol']: item['market'] for item in db_stocks}
    all_tickers = list(stock_map.keys())

    chunk_size = 500
    latest_prices = []
    
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    for i in range(0, len(all_tickers), chunk_size):
        chunk = all_tickers[i:i + chunk_size]
        print(f"Downloading chunk {i} ({len(chunk)} stocks)...")
        
        try:
            data = yf.download(chunk, period="5d", interval="5m", group_by='ticker', threads=True, progress=False)
            
            for sym in chunk:
                try:
                    # Get correct market tag from our map
                    market_tag = stock_map.get(sym, 'US')
                    
                    if len(chunk) == 1:
                        df = data
                    else:
                        if sym not in data.columns.levels[0]: continue
                        df = data[sym]
                        
                    # Pass market_tag to processor
                    stock_data = process_single_ticker(sym, market_tag, df)
                    
                    if stock_data:
                        latest_prices.append(stock_data)
                        
                except Exception as e:
                    # Silent fail for individual stocks to keep loop moving
                    continue
                    
        except Exception as e:
            print(f"Chunk error: {e}")

    if latest_prices:
        with open(os.path.join(OUTPUT_DIR, 'latest_prices.json'), 'w') as f:
            json.dump(latest_prices, f)
        print(f"✅ Saved {len(latest_prices)} stocks.")
    else:
        print("⚠️ No data saved.")

if __name__ == "__main__":
    main()
