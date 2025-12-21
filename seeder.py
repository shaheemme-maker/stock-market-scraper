import os
import requests
from supabase import create_client, Client

# --- AUTHENTICATION ---
# This pulls the secrets from GitHub Actions environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Safety check to prevent "Invalid URL" errors if secrets are missing
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Error: Missing Supabase credentials. Check your GitHub Secrets (SUPABASE_URL and SUPABASE_KEY).")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_all_us_stocks():
    print("Fetching full list of US stocks (NASDAQ, NYSE, AMEX)...")
    
    # Raw list of all tickers
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    
    try:
        response = requests.get(url)
        symbols = response.text.splitlines()
        # Clean the list: remove spaces and keep only valid alphabet tickers
        clean_symbols = [s.strip() for s in symbols if s.strip().isalpha()]
        
        print(f"✅ Found {len(clean_symbols)} US stocks.")
        return clean_symbols
    except Exception as e:
        print(f"⚠️ Error fetching US list: {e}")
        return []

def seed_database():
    master_list = []

    # 1. Get ALL US Stocks automatically
    us_symbols = get_all_us_stocks()
    for sym in us_symbols:
        master_list.append({
            "symbol": sym,
            "company_name": sym, # Using symbol as placeholder name
            "market": "US"
        })

    # 2. Add your Manual India/Index list here
    manual_additions = [
        # Indexes
        {"symbol": "^GSPC", "name": "S&P 500", "market": "INDEX"},
        {"symbol": "^NSEI", "name": "Nifty 50", "market": "INDEX"},
        # India Top Stocks (Add more here as needed)
        {"symbol": "RELIANCE.NS", "name": "Reliance Industries", "market": "IN"},
        {"symbol": "TCS.NS", "name": "TCS", "market": "IN"},
        {"symbol": "HDFCBANK.NS", "name": "HDFC Bank", "market": "IN"},
        {"symbol": "INFY.NS", "name": "Infosys", "market": "IN"},
        {"symbol": "TATAMOTORS.NS", "name": "Tata Motors", "market": "IN"}
    ]
    
    for item in manual_additions:
        master_list.append({
            "symbol": item['symbol'],
            "company_name": item.get('name', item['symbol']),
            "market": item['market']
        })

    # 3. Bulk Insert/Upsert into Supabase
    print(f"🚀 Uploading {len(master_list)} profiles to Supabase...")
    
    # Upsert in chunks of 500 to prevent timeouts
    chunk_size = 500
    for i in range(0, len(master_list), chunk_size):
        chunk = master_list[i:i + chunk_size]
        try:
            # Upsert ensures we don't create duplicates
            supabase.table("stock_profiles").upsert(chunk, on_conflict="symbol").execute()
            print(f"Batch {i} - {i+len(chunk)} uploaded.")
        except Exception as e:
            print(f"❌ Error on batch {i}: {e}")

if __name__ == "__main__":
    seed_database()
