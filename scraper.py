import yfinance as yf
import json
import time
import os
import pytz
import pandas as pd
from datetime import datetime, timedelta

# Configuration
SYMBOLS = [
    "AAPL", "GOOGL", "MSFT", "AMZN", "META", "TSLA", "NVDA", "NFLX", 
    "AMD", "INTC", "CRM", "ADBE", "PYPL", "CSCO", "PEP", "AVGO", "TXN", "QCOM", 
    "COST", "TMUS", "CMCSA", "AMGN", "HON", "SBUX", "GILD", "INTU", "MDLZ", 
    "BKNG", "ADI", "ISRG", "VRTX", "REGN", "LRCX", "ATVI", "PANW", "SNPS", 
    "CDNS", "KLAC", "ASML", "NXPI", "MAR", "CTAS", "ORLY", "FTNT", "CHTR", 
    "KDP", "PAYX", "MCHP", "AEP", "MRNA", "AZN", "EXC", "BIIB", "IDXX", 
    "DXCM", "XEL", "PCAR", "ROST", "WBD", "SGEN", "DLTR", "ODFL", "FAST", 
    "BKR", "GFS", "FANG", "VRSK", "CSGP", "ON", "CDW", "ANSS", "WBA", 
    "ALGN", "ILMN", "SIRI", "EBAY", "TEAM", "ZM", "ZS", "LCID", "RIVN", 
    "DDOG", "SNOW", "PLTR", "SOFI", "HOOD", "COIN", "DKNG", "ROKU", "U", 
    "AFRM", "OPEN", "SPCE", "NIO", "XPEV", "LI", "BABA", "JD", "PDD", 
    "BIDU", "TCEHY", "SE", "MELI", "CPNG", "GRAB", "NU", "PATH", "GTLB", 
    "MDB", "NET", "CRWD", "OKTA", "DOCU", "TWLO", "SPLK", "WDAY", "VEEV", 
    "HUBS", "BILL", "CFLT", "HCP", "SENT", "IOT", "AMPL", "AI", "UPST", 
    "LMND", "FVRR", "PTON", "CHWY", "CVNA", "ETSY", "WAY", "DASH", "ABNB"
]

OUTPUT_DIR = "data"

def fetch_data():
    print(f"Fetching data for {len(SYMBOLS)} symbols...")
    
    # Download data for all symbols at once (1 day period, 5 minute interval)
    # Using 'threads=True' for faster downloading
    data = yf.download(
        tickers=" ".join(SYMBOLS), 
        period="1d", 
        interval="5m", 
        group_by='ticker', 
        threads=True,
        progress=False
    )
    
    latest_prices = []
    history_data = {}
    
    current_time = datetime.now(pytz.utc).isoformat()
    
    # Define US Eastern timezone for consistent market hours
    ny_tz = pytz.timezone('America/New_York')
    
    for symbol in SYMBOLS:
        try:
            # Handle the case where yf.download returns a MultiIndex or single DataFrame
            if len(SYMBOLS) > 1:
                ticker_data = data[symbol].copy()
            else:
                ticker_data = data.copy()
            
            # Skip if empty
            if ticker_data.empty or len(ticker_data) == 0:
                print(f"No data for {symbol}")
                continue

            # --- START DATA NORMALIZATION ---
            
            # 1. Ensure timezone awareness (yfinance usually returns localized, but safe to check)
            if ticker_data.index.tz is None:
                ticker_data.index = ticker_data.index.tz_localize(ny_tz)
            else:
                ticker_data.index = ticker_data.index.tz_convert(ny_tz)

            # 2. Define today's Market Open (9:30 AM) and Close (4:00 PM)
            # We take the date from the first data point
            market_date = ticker_data.index[0].normalize()
            market_open = market_date + timedelta(hours=9, minutes=30)
            market_close = market_date + timedelta(hours=16, minutes=0)

            # 3. Create a perfect 5-minute timeline for the full trading day
            full_schedule = pd.date_range(
                start=market_open, 
                end=market_close, 
                freq='5min', 
                tz=ny_tz
            )

            # 4. Reindex: Force data to fit this timeline
            #    - This creates NaNs for missing timestamps (gaps)
            #    - This drops timestamps outside market hours (pre/post market)
            ticker_data = ticker_data.reindex(full_schedule)

            # 5. Forward Fill: Propagate last valid price to fill gaps
            ticker_data = ticker_data.ffill()

            # 6. Back Fill: If data started late (e.g. 9:45), fill beginning with first price
            ticker_data = ticker_data.bfill()

            # 7. Trim Future: Don't show flat line for the rest of the day if it's currently 12:00 PM
            #    We only keep points <= current time (plus buffer for API delays)
            now_ny = datetime.now(ny_tz)
            ticker_data = ticker_data[ticker_data.index <= now_ny]

            # --- END DATA NORMALIZATION ---

            # If after filtering we have no data, skip
            if len(ticker_data) == 0:
                continue

            # Extract basic info using the last available valid point
            last_point = ticker_data.iloc[-1]
            current_price = float(last_point['Close'])
            open_price = float(ticker_data.iloc[0]['Open']) # Use first point as "Open" for the chart
            
            # yfinance often gives 'Close' of previous day as 'Previous Close' via Ticker object, 
            # but in bulk download we calculate change relative to the first available point 
            # or we can try to get it from info if we want to be slow. 
            # FAST METHOD: Use the difference between current and first point of the day as "Change" 
            # OR assume the first point roughly equals previous close if we lack other data.
            # Better: `price - open_price` is "Day Change".
            
            price_change = current_price - open_price
            percent_change = (price_change / open_price) * 100 if open_price != 0 else 0
            
            # Structure for latest_prices.json
            stock_info = {
                "symbol": symbol,
                "price": round(current_price, 2),
                "change_value": round(price_change, 2),
                "change_percent": round(percent_change, 2),
                "previous_close": round(open_price, 2), # Using Open as proxy for reference
                "market": "US",
                "last_updated": current_time
            }
            
            # Try to get company name (this is slow in a loop, ideally cached or hardcoded)
            # For this scraper, we'll skip the heavy API call and use Symbol as fallback
            # unless you have a separate mapping list.
            stock_info["company_name"] = symbol 
            
            latest_prices.append(stock_info)
            
            # Structure for history.json (Optimized)
            # s: Start Timestamp (Unix seconds)
            # p: List of prices
            start_timestamp = int(ticker_data.index[0].timestamp())
            
            # Round prices to 2 decimals to save space
            prices_list = [round(p, 2) for p in ticker_data['Close'].tolist()]
            
            history_data[symbol] = {
                "s": start_timestamp,
                "p": prices_list
            }
            
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue
            
    return latest_prices, history_data

def save_json(filename, data):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f)
    print(f"Saved {filename}")

def main():
    print(f"Starting scrape at {datetime.now()}")
    
    try:
        latest_prices, history = fetch_data()
        
        if latest_prices:
            save_json('latest_prices.json', latest_prices)
            save_json('history.json', history)
            
            # Update timestamp file
            with open(os.path.join(OUTPUT_DIR, 'last_update.txt'), 'w') as f:
                f.write(datetime.now().isoformat())
                
        print("Scrape completed successfully")
        
    except Exception as e:
        print(f"Scrape failed: {e}")

if __name__ == "__main__":
    main()
