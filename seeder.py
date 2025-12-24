import os
import requests
import pandas as pd
from io import StringIO
from supabase import create_client, Client

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ CRITICAL ERROR: GitHub Secrets are missing.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def fetch_wiki_table(url, match_text):
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        # Find table containing specific text
        tables = pd.read_html(StringIO(response.text), match=match_text)
        return tables[0]
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None

# --- REGIONAL FUNCTIONS ---

def get_sp500():
    print("🇺🇸 Fetching S&P 500 (US)...")
    df = fetch_wiki_table('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol')
    if df is None: return []
    stocks = []
    for _, row in df.iterrows():
        symbol = str(row['Symbol']).replace('.', '-')
        stocks.append({"symbol": symbol, "company_name": row['Security'], "market": "US"})
    return stocks

def get_nifty500():
    print("🇮🇳 Fetching Nifty 500 (India)...")
    # Using the NIFTY 500 Wikipedia page which lists all 500 companies
    df = fetch_wiki_table('https://en.wikipedia.org/wiki/NIFTY_500', 'Symbol')
    if df is None: return []
    
    stocks = []
    # Dynamic column finding to handle Wiki format changes
    symbol_col = next((c for c in df.columns if 'Symbol' in c), 'Symbol')
    name_col = next((c for c in df.columns if 'Company' in c), 'Company Name')
    
    for _, row in df.iterrows():
        # Clean symbol and add suffix
        raw_symbol = str(row[symbol_col]).strip()
        symbol = f"{raw_symbol}.NS"
        
        stocks.append({
            "symbol": symbol,
            "company_name": row[name_col],
            "market": "INDIA"
        })
    return stocks

def get_ftse100():
    print("🇬🇧 Fetching FTSE 100 (UK)...")
    df = fetch_wiki_table('https://en.wikipedia.org/wiki/FTSE_100_Index', 'Ticker')
    if df is None: return []
    stocks = []
    for _, row in df.iterrows():
        ticker = str(row['Ticker'])
        if not ticker.endswith('.L'): ticker += '.L'
        stocks.append({"symbol": ticker, "company_name": row['Company'], "market": "UK"})
    return stocks

def get_dax40():
    print("🇩🇪 Fetching DAX 40 (Germany)...")
    df = fetch_wiki_table('https://en.wikipedia.org/wiki/DAX', 'Ticker')
    if df is None: return []
    stocks = []
    for _, row in df.iterrows():
        ticker = str(row['Ticker'])
        if not ticker.endswith('.DE'): ticker += '.DE'
        stocks.append({"symbol": ticker, "company_name": row['Company'], "market": "GERMANY"})
    return stocks

def get_cac40():
    print("🇫🇷 Fetching CAC 40 (France)...")
    df = fetch_wiki_table('https://en.wikipedia.org/wiki/CAC_40', 'Ticker')
    if df is None: return []
    stocks = []
    for _, row in df.iterrows():
        ticker = str(row['Ticker'])
        if not ticker.endswith('.PA'): ticker += '.PA'
        stocks.append({"symbol": ticker, "company_name": row['Company'], "market": "FRANCE"})
    return stocks

def get_global_indexes():
    print("🌍 Adding Global Indices...")
    return [
        {"symbol": "^GSPC", "company_name": "S&P 500", "market": "INDEX"},
        {"symbol": "^DJI", "company_name": "Dow Jones Industrial Average", "market": "INDEX"},
        {"symbol": "^IXIC", "company_name": "NASDAQ Composite", "market": "INDEX"},
        {"symbol": "^NSEI", "company_name": "Nifty 50", "market": "INDEX"},
        {"symbol": "^BSESN", "company_name": "BSE SENSEX", "market": "INDEX"},
        {"symbol": "^FTSE", "company_name": "FTSE 100", "market": "INDEX"},
        {"symbol": "^GDAXI", "company_name": "DAX Performance Index", "market": "INDEX"},
        {"symbol": "^FCHI", "company_name": "CAC 40", "market": "INDEX"},
        {"symbol": "^N225", "company_name": "Nikkei 225", "market": "INDEX"},
        {"symbol": "^HSI", "company_name": "Hang Seng Index", "market": "INDEX"},
    ]

# --- MAIN EXECUTION ---

def seed_database():
    master_list = []

    # 1. Aggregate Regional Stocks
    master_list.extend(get_sp500())
    master_list.extend(get_nifty500()) # <--- NOW FETCHES 500 STOCKS
    master_list.extend(get_ftse100())
    master_list.extend(get_dax40())
    master_list.extend(get_cac40())

    # 2. Add Global Indexes
    master_list.extend(get_global_indexes())

    print(f"\n📦 Total Items Found: {len(master_list)}")

    # 3. Upload to Supabase in Batches
    chunk_size = 100
    for i in range(0, len(master_list), chunk_size):
        chunk = master_list[i:i + chunk_size]
        try:
            supabase.table("stock_profiles").upsert(chunk, on_conflict="symbol").execute()
            print(f"   Batch {i}-{i+len(chunk)} uploaded.")
        except Exception as e:
            print(f"❌ Error on batch {i}: {e}")

    print("✅ Database seeding complete!")

if __name__ == "__main__":
    seed_database()
