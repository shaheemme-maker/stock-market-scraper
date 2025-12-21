import os
import requests
import pandas as pd
from io import StringIO
from supabase import create_client, Client

# --- SETUP & AUTHENTICATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ CRITICAL ERROR: GitHub Secrets are missing.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_sp500_stocks():
    print("Fetching S&P 500 list from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    
    # FIX: Add a User-Agent header so Wikipedia thinks we are a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        # 1. Fetch HTML manually with headers
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Check for other errors
        
        # 2. Feed the HTML text into Pandas
        # Wrap in StringIO because pandas expects a file-like object
        tables = pd.read_html(StringIO(response.text))
        sp500_df = tables[0]
        
        stock_list = []
        for index, row in sp500_df.iterrows():
            stock_list.append({
                "symbol": row['Symbol'],
                "company_name": row['Security'],
                "market": "US"
            })
            
        print(f"✅ Found {len(stock_list)} S&P 500 companies.")
        return stock_list
    except Exception as e:
        print(f"⚠️ Error fetching S&P 500: {e}")
        return []

def seed_database():
    master_list = []

    # 1. Get S&P 500 (US)
    sp500 = get_sp500_stocks()
    master_list.extend(sp500)

    # 2. Add India / Indexes (Manual List)
    manual_additions = [
        {"symbol": "^GSPC", "name": "S&P 500 Index", "market": "INDEX"},
        {"symbol": "^NSEI", "name": "Nifty 50 Index", "market": "INDEX"},
        {"symbol": "RELIANCE.NS", "name": "Reliance Industries", "market": "IN"},
        {"symbol": "TCS.NS", "name": "Tata Consultancy Services", "market": "IN"},
        {"symbol": "HDFCBANK.NS", "name": "HDFC Bank", "market": "IN"},
        {"symbol": "INFY.NS", "name": "Infosys", "market": "IN"},
        {"symbol": "ICICIBANK.NS", "name": "ICICI Bank", "market": "IN"},
        {"symbol": "HINDUNILVR.NS", "name": "Hindustan Unilever", "market": "IN"},
        {"symbol": "ITC.NS", "name": "ITC Limited", "market": "IN"}
    ]
    
    for item in manual_additions:
        master_list.append({
            "symbol": item['symbol'],
            "company_name": item['name'],
            "market": item['market']
        })

    # 3. Upload to Supabase
    print(f"🚀 Uploading {len(master_list)} profiles...")
    
    chunk_size = 100
    for i in range(0, len(master_list), chunk_size):
        chunk = master_list[i:i + chunk_size]
        try:
            supabase.table("stock_profiles").upsert(chunk, on_conflict="symbol").execute()
            print(f"   Batch {i}-{i+len(chunk)} uploaded.")
        except Exception as e:
            print(f"❌ Error on batch {i}: {e}")

if __name__ == "__main__":
    seed_database()
