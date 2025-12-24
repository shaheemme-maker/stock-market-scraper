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
        tables = pd.read_html(StringIO(response.text), match=match_text)
        return tables[0]
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None

# --- REGIONAL STOCKS ---

def get_sp500():
    print("🇺🇸 Fetching S&P 500 (US)...")
    df = fetch_wiki_table('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'Symbol')
    if df is None: return []
    stocks = []
    for _, row in df.iterrows():
        symbol = str(row['Symbol']).replace('.', '-')
        stocks.append({"symbol": symbol, "company_name": row['Security'], "market": "US"})
    return stocks

def get_nifty50():
    print("🇮🇳 Fetching Nifty 50 (India)...")
    df = fetch_wiki_table('https://en.wikipedia.org/wiki/NIFTY_50', 'Symbol')
    if df is None: return []
    stocks = []
    symbol_col = next((c for c in df.columns if 'Symbol' in c), 'Symbol')
    name_col = next((c for c in df.columns if 'Company' in c), 'Company Name')
    for _, row in df.iterrows():
        stocks.append({"symbol": f"{row[symbol_col]}.NS", "company_name": row[name_col], "market": "INDIA"})
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

# --- NEW: ALL GLOBAL INDEXES ---

def get_global_indexes():
    print("🌍 Adding Global Indices...")
    return [
        # --- UNITED STATES ---
        {"symbol": "^GSPC", "company_name": "S&P 500", "market": "INDEX"},
        {"symbol": "^DJI", "company_name": "Dow Jones Industrial Average", "market": "INDEX"},
        {"symbol": "^IXIC", "company_name": "NASDAQ Composite", "market": "INDEX"},
        {"symbol": "^RUT", "company_name": "Russell 2000", "market": "INDEX"},
        {"symbol": "^VIX", "company_name": "CBOE Volatility Index", "market": "INDEX"},

        # --- ASIA PACIFIC ---
        {"symbol": "^NSEI", "company_name": "Nifty 50", "market": "INDEX"},  # India
        {"symbol": "^BSESN", "company_name": "BSE SENSEX", "market": "INDEX"}, # India
        {"symbol": "^N225", "company_name": "Nikkei 225", "market": "INDEX"}, # Japan
        {"symbol": "^HSI", "company_name": "Hang Seng Index", "market": "INDEX"}, # Hong Kong
        {"symbol": "000001.SS", "company_name": "SSE Composite Index", "market": "INDEX"}, # China
        {"symbol": "^AXJO", "company_name": "S&P/ASX 200", "market": "INDEX"}, # Australia
        {"symbol": "^KS11", "company_name": "KOSPI Composite", "market": "INDEX"}, # South Korea
        {"symbol": "^TWII", "company_name": "TSEC weighted index", "market": "INDEX"}, # Taiwan

        # --- EUROPE ---
        {"symbol": "^FTSE", "company_name": "FTSE 100", "market": "INDEX"}, # UK
        {"symbol": "^GDAXI", "company_name": "DAX Performance Index", "market": "INDEX"}, # Germany
        {"symbol": "^FCHI", "company_name": "CAC 40", "market": "INDEX"}, # France
        {"symbol": "^STOXX50E", "company_name": "ESTX 50 PR.EUR", "market": "INDEX"}, # Eurozone
        {"symbol": "^SSMI", "company_name": "SMI PR", "market": "INDEX"}, # Switzerland

        # --- AMERICAS ---
        {"symbol": "^GSPTSE", "company_name": "S&P/TSX Composite", "market": "INDEX"}, # Canada
        {"symbol": "^BVSP", "company_name": "IBOVESPA", "market": "INDEX"}, # Brazil
        {"symbol": "^MXX", "company_name": "IPC MEXICO", "market": "INDEX"}, # Mexico
    ]

# --- MAIN EXECUTION ---

def seed_database():
    master_list = []

    # 1. Aggregate Regional Stocks
    master_list.extend(get_sp500())
    master_list.extend(get_nifty50())
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
