# Filename: main.py

from fastapi import FastAPI, HTTPException
import uvicorn
import yfinance as yf
import pandas as pd

# Initialize FastAPI app
app = FastAPI(
    title="Stock Data API",
    description="API to fetch current stock prices and historical data using yfinance.",
    version="1.0.0",
)

# --- Helper Functions ---

def get_stock_price(symbol: str) -> float | None:
    """
    Fetches the current stock price for a given symbol using yfinance.
    Tries to find the most relevant price from 'info' dictionary.
    Returns the price as a float, or None if not found.
    """
    try:
        ticker = yf.Ticker(symbol)
        # 'info' dictionary contains various data points including price
        info = ticker.info

        # Attempt to get the price from common keys, prioritizing more real-time data
        price = info.get('regularMarketPrice')
        if price is None:
            price = info.get('currentPrice')
        if price is None:
            price = info.get('previousClose')
        
        # If after checking common keys, price is still None, it might mean
        # the symbol is valid but current/recent price data isn't readily available in 'info'.
        # A more robust check might involve fetching a short history, but for current price, 'info' is usually best.
        # We'll let the endpoint handler decide if None price means error or just unavailable data for that symbol.
        return price
        
    except Exception as e:
        # Log the error for debugging on the server side
        print(f"Error fetching price for {symbol}: {e}")
        # yfinance might raise errors for invalid tickers or network issues.
        # If it's an invalid ticker, info might be empty or lack 'symbol'.
        if not info or 'symbol' not in info:
            raise ValueError(f"Stock symbol '{symbol}' not found or invalid.")
        else:
            raise ValueError(f"Could not retrieve price data for '{symbol}'.")


def get_stock_history(symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame | None:
    """
    Fetches historical stock data for a given symbol, period, and interval.
    Returns a pandas DataFrame or None if no data is found.
    """
    try:
        ticker = yf.Ticker(symbol)
        history_df = ticker.history(period=period, interval=interval)
        
        if history_df.empty:
            return None
        
        # yfinance returns data as a pandas DataFrame, which FastAPI can serialize to JSON.
        # Convert index (Date) to string for better JSON compatibility if needed, but DataFrame handles it well.
        # history_df.index = history_df.index.strftime('%Y-%m-%d %H:%M:%S') # Example: convert index to string
        return history_df
        
    except Exception as e:
        # Log the error for debugging on the server side
        print(f"Error fetching history for {symbol} (period={period}, interval={interval}): {e}")
        # yfinance might raise errors for invalid tickers, incompatible periods/intervals, or network issues.
        # Let's check if the symbol itself seems valid first.
        ticker = yf.Ticker(symbol)
        if not ticker.info or 'symbol' not in ticker.info: # Basic check for symbol validity
            raise ValueError(f"Stock symbol '{symbol}' not found or invalid.")
        else:
            # If symbol is valid, the error is likely with period/interval combination or data retrieval
            raise ValueError(f"Could not retrieve historical data for '{symbol}' with period='{period}' and interval='{interval}'.")


# --- API Endpoints ---

@app.get("/stock/{symbol}/price")
async def stock_price(symbol: str):
    """
    Retrieves the current stock price for a given symbol.
    
    Args:
        symbol (str): The stock ticker symbol (e.g., AAPL, GOOG). Case-insensitive.
    
    Returns:
        dict: A JSON object containing the symbol and its current price.
        Example: {"symbol": "AAPL", "price": 170.50}
    """
    try:
        # Normalize symbol to uppercase for consistency
        symbol = symbol.upper()
        
        price = get_stock_price(symbol)
        
        if price is None:
            # If price is None, it implies the symbol might be valid but no current price data
            # is directly available in the 'info' dictionary or it's an unusual stock.
            # Let's perform a quick check to see if the symbol itself is recognized by yfinance.
            try:
                yf.Ticker(symbol).info # This call might be slow if it's a valid but obscure symbol
                # If the above line didn't raise an error but price is still None, then price data is truly unavailable.
                raise HTTPException(status_code=404, detail=f"Current price data not available for stock symbol '{symbol}'.")
            except Exception:
                # If yf.Ticker(symbol).info raised an error, it's likely an invalid symbol.
                raise HTTPException(status_code=404, detail=f"Stock symbol '{symbol}' not found or is invalid.")

        return {"symbol": symbol, "price": price}
        
    except ValueError as ve:
        # Catch specific errors raised by our helper functions
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        # Catch any other unexpected errors during processing
        print(f"An unexpected error occurred for /stock/{symbol}/price: {e}") # Log for debugging
        raise HTTPException(status_code=500, detail=f"An internal server error occurred: {str(e)}")

@app.get("/stock/{symbol}/history")
async def stock_history(symbol: str, period: str = "1y", interval: str = "1d"):
    """
    Retrieves historical stock data for a given symbol.
    
    Args:
        symbol (str): The stock ticker symbol (e.g., AAPL, GOOG). Case-insensitive.
        period (str): The period for which to fetch data.
                      Valid periods: "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max".
                      Defaults to "1y".
        interval (str): The interval of the data.
                        Valid intervals: "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo".
                        Note: Not all intervals are available for all periods. For example, '1m' interval is only available for '1d' or '5d' periods.
                        Defaults to "1d".
    
    Returns:
        dict: A JSON object containing historical stock data (Open, High, Low, Close, Volume, etc.).
              The data is returned as a list of records, each representing a time interval.
    """
    try:
        # Normalize symbol to uppercase for consistency
        symbol = symbol.upper()
        
        # yfinance has specific limitations on period/interval combinations.
        # For instance, minute intervals ('1m', '5m', etc.) are only supported for periods up to '5d'.
        # '1h' interval is supported up to '2y'. Daily intervals ('1d', '5d', etc.) are more flexible.
        
        # Basic validation for common intervals and periods to provide clearer errors
        valid_intervals = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"]
        valid_periods = ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]

        if interval not in valid_intervals:
            raise HTTPException(status_code=400, detail=f"Invalid interval '{interval}'. Supported intervals are: {', '.join(valid_intervals)}")
        if period not in valid_periods:
             raise HTTPException(status_code=400, detail=f"Invalid period '{period}'. Supported periods are: {', '.join(valid_periods)}")

        # Check for incompatible period/interval combinations before calling yfinance
        if 'm' in interval or 'h' in interval: # Minute or hour intervals
            if period not in ["1d", "5d", "1mo", "2y"]: # Adjusted for common yfinance behavior; 1mo/2y for hourly
                if interval.endswith('m'): # Minute intervals are strictly for short periods
                     raise HTTPException(status_code=400, detail=f"Minute intervals ('{interval}') are only supported for periods '1d' or '5d'.")
                elif interval.endswith('h'): # Hourly intervals
                     raise HTTPException(status_code=400, detail=f"Hourly intervals ('{interval}') are typically supported for periods up to '2y'.")

        history_data = get_stock_history(symbol, period=period, interval=interval)
        
        if history_data is None:
            # If get_stock_history returns None, it means the symbol is valid but no data for the given params
            try:
                yf.Ticker(symbol).info # Quick check if symbol is generally valid
                raise HTTPException(status_code=404, detail=f"No historical data found for stock symbol '{symbol}' with period='{period}' and interval='{interval}'.")
            except Exception:
                 raise HTTPException(status_code=404, detail=f"Stock symbol '{symbol}' not found or is invalid.")
        
        # Convert DataFrame to a JSON-serializable format (list of dictionaries)
        # The index (Date) is converted to a string in ISO format.
        # .to_dict(orient='records') converts the DataFrame into a list of dicts.
        historical_records = history_data.reset_index().to_dict(orient='records')
        
        # Format the date/datetime for better JSON representation if it's not already a string
        # yfinance's index is typically a DatetimeIndex. to_dict('records') should handle this.
        # For explicit control:
        formatted_records = []
        for record in historical_records:
            # 'Date' is usually the name of the index column after reset_index()
            if 'Date' in record and isinstance(record['Date'], pd.Timestamp):
                record['Date'] = record['Date'].isoformat()
            formatted_records.append(record)

        return {
            "symbol": symbol,
            "period": period,
            "interval": interval,
            "historical_data": formatted_records
        }
        
    except ValueError as ve:
        # Catch specific errors raised by our helper functions
        raise HTTPException(status_code=400, detail=str(ve)) # Use 400 for bad requests (like invalid params)
    except HTTPException as http_ex:
        # Re-raise HTTPE
        raise http_ex
    except Exception as e:
        # Catch any other unexpected errors during processing
        print(f"An unexpected error occurred for /stock/{symbol}/history: {e}") # Log for debugging
        raise HTTPException(status_code=500, detail=f"An internal server error occurred: {str(e)}")