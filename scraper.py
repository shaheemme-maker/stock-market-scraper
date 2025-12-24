# In scraper.py, inside the `fetch_data` function loop:

import pandas as pd
# ... (existing imports)

# ... inside the loop ...
    try:
        # Get data for this specific symbol
        ticker_data = data[symbol].copy()
        
        # ---------------------------------------------------------
        # NEW: NORMALIZE THE DATA (The "Easy" Way)
        # ---------------------------------------------------------
        if len(ticker_data) > 0:
            # 1. Ensure we have a timezone-aware index (usually America/New_York for US)
            if ticker_data.index.tz is None:
                ticker_data.index = ticker_data.index.tz_localize('America/New_York')
            
            # 2. Define the full trading day (9:30 AM to 4:00 PM) for the current date
            current_date = ticker_data.index[0].normalize()
            market_open = current_date + pd.Timedelta(hours=9, minutes=30)
            market_close = current_date + pd.Timedelta(hours=16, minutes=0)
            
            # 3. Create a perfect 5-minute timeline
            full_schedule = pd.date_range(start=market_open, end=market_close, freq='5min', tz=ticker_data.index.tz)
            
            # 4. Reindex: Force our data to fit this timeline
            #    ffill(): Forward fill fills gaps with the last known price
            ticker_data = ticker_data.reindex(full_schedule).ffill()
            
            # 5. Handle cases where data starts LATE (e.g. first trade at 9:45)
            #    bfill(): Backfill the start using the first available price
            ticker_data = ticker_data.bfill()
            
            # 6. Trim: Remove future empty points if the market is still open
            now = pd.Timestamp.now(tz=ticker_data.index.tz)
            ticker_data = ticker_data[ticker_data.index <= now]
        # ---------------------------------------------------------

        # Continue with your existing logic, but use the normalized `ticker_data`
        current_price = ticker_data['Close'].iloc[-1]
        # ...
