import os
import yfinance as yf
from supabase import create_client, Client
from datetime import datetime

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Define your stocks AND their names here to keep the script fast
STOCK_MAP = {
    "AAPL": "Apple Inc.",
    "GOOGL": "Alphabet Inc.",
    "MSFT": "Microsoft Corp",
    "TSLA": "Tesla Inc.",
    "RELIANCE.NS": "Reliance Industries",
    "TCS.NS": "Tata Consultancy Services",
    "INFY.NS": "Infosys Ltd"
}

def fetch_data():
    print(f"Fetching data for {len(STOCK_MAP)} stocks...")
    
    # Create a space-separated string of symbols for yfinance
    symbols_list = list(STOCK_MAP.keys())
    tickers = yf.Tickers(" ".join(symbols_list))
    
    payload = []
    
    for symbol, company_name in STOCK_MAP.items():
        try:
            # fast_info is very fast and reliable for prices
            ticker = tickers.tickers[symbol]
            info = ticker.fast_info 
            
            price = info.last_price
            prev_close = info.previous_close
            
            # Calculate changes
            change_value = price - prev_close
            change_pct = (change_value / prev_close) * 100
            
            payload.append({
                "symbol": symbol,
                "company_name": company_name,  # Insert the name from our list
                "price": round(price, 2),
                "change_percent": round(change_pct, 2),
                "change_value": round(change_value, 2),
                "recorded_at": datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            print(f"Skipping {symbol}: {e}")

    if payload:
        # Insert data
        data, count = supabase.table("stock_prices").insert(payload).execute()
        print(f"Inserted {len(payload)} rows.")
        
        # Cleanup old data (Keep DB small)
        try:
            supabase.rpc("delete_old_prices").execute()
        except Exception as e:
            print(f"Cleanup warning: {e}")

if __name__ == "__main__":
    fetch_data()
