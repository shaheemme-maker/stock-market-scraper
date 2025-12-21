import pandas as pd
from supabase import create_client
import os
import io
import requests

# --- CONFIG ---
# RUN THIS LOCALLY ON YOUR COMPUTER, NOT GITHUB ACTIONS
SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SERVICE_ROLE_KEY" # Needed for bulk inserts
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_all_us_stocks():
    print("Fetching full list of US stocks (NASDAQ, NYSE, AMEX)...")
    
    # We use a reliable public dataset for all US tickers to avoid manual typing
    # This URL is a common source for a raw CSV of all tickers
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    
    try:
        response = requests.get(url)
        # The file is just a list of symbols: AAPL, MSFT, etc.
        symbols = response.text.splitlines()
        
        # Clean the list (remove empty lines, remove test stocks like 'ZYZZ')
        clean_symbols = [s.strip() for s in symbols if s.strip().isalpha()]
        
        print(f"Found {len(clean_symbols)} US stocks.")
        return clean_symbols
    except Exception as e:
        print(f"Error fetching US list: {e}")
        return []

def seed_database():
    master_list = []

    # 1. Get ALL US Stocks automatically
    us_symbols = get_all_us_stocks()
    for sym in us_symbols:
        master_list.append({
            "symbol": sym,
            "company_name": sym, # We use Symbol as placeholder name to save API calls. 
                                 # Real names will auto-update later if you want.
            "market": "US"
        })

    # 2. Add your Manual India/Index list here
    manual_additions = [
        # Indexes
        {"symbol": "^GSPC", "name": "S&P 500", "market": "INDEX"},
        {"symbol": "^NSEI", "name": "Nifty 50", "market": "INDEX"},
        # India Top Stocks
        {"symbol": "RELIANCE.NS", "name": "Reliance Industries", "market": "IN"},
        {"symbol": "TCS.NS", "name": "TCS", "market": "IN"},
        {"symbol": "HDFCBANK.NS", "name": "HDFC Bank", "market": "IN"},
        # ... Add as many as you want here
    ]
    
    for item in manual_additions:
        master_list.append({
            "symbol": item['symbol'],
            "company_name": item.get('name', item['symbol']),
            "market": item['market']
        })

    # 3. Bulk Insert/Upsert into Supabase
    print(f"Uploading {len(master_list)} profiles to Supabase...")
    
    # Upsert in chunks of 500
    chunk_size = 500
    for i in range(0, len(master_list), chunk_size):
        chunk = master_list[i:i + chunk_size]
        try:
            supabase.table("stock_profiles").upsert(chunk, on_conflict="symbol").execute()
            print(f"Batch {i} - {i+len(chunk)} uploaded.")
        except Exception as e:
            print(f"Error on batch {i}: {e}")

if __name__ == "__main__":
    seed_database()
