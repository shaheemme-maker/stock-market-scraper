import os
import pandas as pd
from supabase import create_client, Client

# --- 1. SETUP & AUTHENTICATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Fix for the "Invalid URL" crash:
# This checks if the secrets are missing BEFORE trying to connect.
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ CRITICAL ERROR: GitHub Secrets are missing. Please add SUPABASE_URL and SUPABASE_KEY to your Repository Secrets.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_sp500_stocks():
    print("Fetching S&P 500 list from Wikipedia...")
    try:
        # Pandas can read tables directly from websites
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        sp500_df = tables[0] # The first table is the S&P 500 list
        
        stock_list = []
        for index, row in sp500_df.iterrows():
            stock_list.append({
                "symbol": row['Symbol'],
                "company_name": row['Security'], # This gives the real name like "Apple Inc."
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
    # Since there is no easy Wikipedia table for "All Nifty 500" with symbols formatted for Yahoo,
    # we add the major ones manually here.
    manual_additions = [
        # Global Indexes
        {"symbol": "^GSPC", "name": "S&P 500 Index", "market": "INDEX"},
        {"symbol": "^NSEI", "name": "Nifty 50 Index", "market": "INDEX"},
        
        # India Top Stocks (You can paste more here)
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
            # upsert=True updates the name if the symbol already exists
            supabase.table("stock_profiles").upsert(chunk, on_conflict="symbol").execute()
            print(f"   Batch {i}-{i+len(chunk)} uploaded.")
        except Exception as e:
            print(f"❌ Error on batch {i}: {e}")

if __name__ == "__main__":
    seed_database()
