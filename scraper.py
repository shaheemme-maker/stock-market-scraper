import os
import yfinance as yf
from supabase import create_client, Client
from datetime import datetime

# Config
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# List your stocks here
STOCKS = [
    "AAPL", "GOOGL", "MSFT",        # US Stocks
    "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS" # Indian Stocks (Note the .NS)
]

def fetch_data():
    print(f"Fetching data for {len(STOCKS)} stocks...")
    
    # Fetch all at once
    tickers = yf.Tickers(" ".join(STOCKS))
    
    payload = []
    
    for symbol in STOCKS:
        try:
            ticker = tickers.tickers[symbol]
            # 'fast_info' is faster/reliable for real-time prices
            info = ticker.fast_info 
            
            # Check if market is roughly active by looking at last trade time
            # (Optional: You can skip this check if you just want to log everything)
            
            price = info.last_price
            prev_close = info.previous_close
            
            # Calculate changes
            change_value = price - prev_close
            change_pct = (change_value / prev_close) * 100
            
            payload.append({
                "symbol": symbol,
                "price": round(price, 2),
                "change_percent": round(change_pct, 2),
                "change_value": round(change_value, 2),
                "recorded_at": datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            print(f"Skipping {symbol}: {e}")

    if payload:
        # Insert data
        supabase.table("stock_prices").insert(payload).execute()
        print(f"Inserted {len(payload)} rows.")
        
        # Cleanup old data (Auto-maintenance)
        supabase.rpc("delete_old_prices").execute()
        print("Cleaned up old data.")

if __name__ == "__main__":
    fetch_data()
