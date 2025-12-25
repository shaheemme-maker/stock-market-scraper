import os
import json
import yfinance as yf
from supabase import create_client, Client
import pandas as pd

# 1. Setup Supabase
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def fetch_and_process():
    # 2. Get Tickers from Supabase
    # Pagination might be needed if >1000, but fetch_all typically handles reasonable sizes
    response = supabase.table('stock_profiles').select("symbol, company_name").execute()
    data = response.data
    
    if not data:
        print("No stocks found in Supabase.")
        return

    # Create a map for easy lookup { "AAPL": "Apple Inc", ... }
    ticker_map = {item['symbol']: item['company_name'] for item in data}
    all_symbols = list(ticker_map.keys())

    # 3. Batch Fetch Data (Chunking to be safe)
    # yfinance can handle many tickers, but let's chunk by 100 to be robust
    chunk_size = 100
    final_output = {}

    for i in range(0, len(all_symbols), chunk_size):
        chunk = all_symbols[i:i + chunk_size]
        print(f"Processing chunk {i} to {i+chunk_size}...")
        
        # Fetch 1 day of data with 5-minute intervals for sparklines
        try:
            tickers_data = yf.download(chunk, period="1d", interval="5m", group_by='ticker', progress=False)
            
            for symbol in chunk:
                try:
                    # Handle cases where data might be missing
                    if symbol not in tickers_data.columns.levels[0]:
                        continue

                    df = tickers_data[symbol]
                    df = df.dropna()
                    
                    if df.empty:
                        continue

                    # Get latest close and previous close for calculations
                    current_price = df['Close'].iloc[-1]
                    start_price = df['Open'].iloc[0] # Or previous close if available
                    
                    price_change = current_price - start_price
                    percent_change = (price_change / start_price) * 100

                    # 4. Generate Sparkline (Simplify data)
                    # We don't need timestamps for a sparkline, just the normalized curve
                    # Take last 20-30 points to keep JSON small, or all points if you want high res
                    sparkline_data = df['Close'].tolist()

                    final_output[symbol] = {
                        "name": ticker_map.get(symbol),
                        "price": round(float(current_price), 2),
                        "change": round(float(price_change), 2),
                        "changePercent": round(float(percent_change), 2),
                        "sparkline": [round(x, 2) for x in sparkline_data] # Round to reduce file size
                    }
                except Exception as e:
                    print(f"Error processing {symbol}: {e}")

        except Exception as batch_e:
            print(f"Batch failed: {batch_e}")

    # 5. Save to JSON
    with open('stocks.json', 'w') as f:
        json.dump(final_output, f)
    
    print("Update complete.")

if __name__ == "__main__":
    fetch_and_process()
