import pandas as pd
from datetime import datetime, timedelta
import yfinance as yf
import time

# --- 1. YFINANCE FETCH FUNCTION ---

def fetch_historical_inr_rate(date_str: str) -> float:
    """
    Fetches the historical USD to INR exchange rate for a given date 
    using Yahoo Finance via yfinance library.
    
    Args:
        date_str: Date string in 'YYYY-MM-DD' format
        
    Returns:
        Exchange rate as float, or 83.0 as fallback
    """
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            print(f"  > Attempt {attempt + 1}: Fetching rate for {date_str} from Yahoo Finance...")
            
            # Convert date to datetime object
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Get data for a range (current date + 1 day to ensure we get the data)
            start_date = (date_obj - timedelta(days=5)).strftime('%Y-%m-%d')  # Buffer for weekends
            end_date = (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Fetch USD/INR rate from Yahoo Finance
            ticker = yf.Ticker("USDINR=X")
            hist = ticker.history(start=start_date, end=end_date)
            
            if not hist.empty:
                # Try to get the exact date
                hist.index = hist.index.tz_localize(None)  # Remove timezone for comparison
                target_date = pd.Timestamp(date_str)
                
                if target_date in hist.index:
                    rate = hist.loc[target_date, 'Close']
                    print(f"  > Success: Exact rate for {date_str} = {rate}")
                    return round(rate, 4)
                else:
                    # If exact date not found (weekend/holiday), use the last available rate
                    hist_filtered = hist[hist.index <= target_date]
                    if not hist_filtered.empty:
                        rate = hist_filtered['Close'].iloc[-1]
                        actual_date = hist_filtered.index[-1].strftime('%Y-%m-%d')
                        print(f"  > Using closest rate from {actual_date} for {date_str} = {rate}")
                        return round(rate, 4)
                    else:
                        # Use first available rate if target date is before all data
                        rate = hist['Close'].iloc[0]
                        actual_date = hist.index[0].strftime('%Y-%m-%d')
                        print(f"  > Using nearest rate from {actual_date} for {date_str} = {rate}")
                        return round(rate, 4)
            else:
                raise ValueError(f"No historical data found for {date_str}")

        except Exception as e:
            print(f"  > Error on attempt {attempt + 1}: {e}")
            
            # If not the last attempt, wait before retrying
            if attempt < max_retries - 1:
                delay = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                print(f"  > Retrying in {delay} second(s)...")
                time.sleep(delay)
    
    print(f"Failed to fetch rate for {date_str} after {max_retries} attempts. Using fixed fallback rate (83.0).")
    return 83.0


# --- 2. LOAD DATA ---

file_path = "Transfers.xlsx"
print(f"Attempting to read data from: {file_path}")
print("=" * 60)

try:
    # Read the Excel file
    df = pd.read_excel(file_path)
    print(f"✓ Successfully loaded {len(df)} rows.")

    # Check for required columns
    required_cols = ['Date', 'Cash Amount (in USD)']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"✗ Error: Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        exit()
    
    print(f"✓ Required columns found: {required_cols}")

except FileNotFoundError:
    print(f"✗ CRITICAL ERROR: File '{file_path}' not found.")
    exit()
except Exception as e:
    print(f"✗ Unexpected error while reading file: {e}")
    exit()


# --- 3. DATA PREPROCESSING ---

print("\n" + "=" * 60)
print("PREPROCESSING DATA")
print("=" * 60)

# Convert 'Date' column to string format YYYY-MM-DD if not already
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

# Ensure 'Cash Amount (in USD)' is numeric
df['Cash Amount (in USD)'] = pd.to_numeric(df['Cash Amount (in USD)'], errors='coerce')

# Remove rows with invalid USD amounts
initial_rows = len(df)
df.dropna(subset=['Cash Amount (in USD)'], inplace=True)
removed_rows = initial_rows - len(df)

if removed_rows > 0:
    print(f"⚠ Removed {removed_rows} row(s) with invalid USD amounts")

print(f"✓ Processing {len(df)} valid rows")


# --- 4. FETCH EXCHANGE RATES ---

print("\n" + "=" * 60)
print("FETCHING HISTORICAL EXCHANGE RATES")
print("=" * 60)

# Get unique dates to minimize API calls
unique_dates = sorted(df['Date'].unique())
print(f"Found {len(unique_dates)} unique dates to fetch")
print()

# Fetch and cache exchange rates
rate_cache = {}
for i, date_str in enumerate(unique_dates, 1):
    print(f"[{i}/{len(unique_dates)}] Date: {date_str}")
    rate = fetch_historical_inr_rate(date_str)
    rate_cache[date_str] = rate
    print(f"  ✓ Cached rate: {rate} INR per USD")
    print()
    
    # Small delay to be respectful to Yahoo Finance servers
    if i < len(unique_dates):
        time.sleep(0.5)


# --- 5. CALCULATE INR AMOUNTS ---

print("=" * 60)
print("CALCULATING INR AMOUNTS")
print("=" * 60)

# Map exchange rates to DataFrame
df['Exchange Rate (USD to INR)'] = df['Date'].map(rate_cache)

# Calculate INR amounts
df['Cash Amount (in INR)'] = (
    df['Cash Amount (in USD)'] * df['Exchange Rate (USD to INR)']
).round(2)

print(f"✓ Successfully calculated INR amounts for {len(df)} transactions")


# --- 6. DISPLAY RESULTS ---

print("\n" + "=" * 60)
print("PREVIEW OF CONVERTED DATA")
print("=" * 60)

# Select relevant columns for display
display_cols = ['Date', 'Activity', 'Cash Amount (in USD)', 
                'Exchange Rate (USD to INR)', 'Cash Amount (in INR)']

# Only show columns that exist
display_cols = [col for col in display_cols if col in df.columns]
print(df[display_cols].to_string(index=False))


# --- 7. SUMMARY STATISTICS ---

print("\n" + "=" * 60)
print("SUMMARY STATISTICS")
print("=" * 60)

total_usd = df['Cash Amount (in USD)'].sum()
total_inr = df['Cash Amount (in INR)'].sum()
avg_rate = df['Exchange Rate (USD to INR)'].mean()
min_rate = df['Exchange Rate (USD to INR)'].min()
max_rate = df['Exchange Rate (USD to INR)'].max()

print(f"Total USD Amount:     ${total_usd:,.2f}")
print(f"Total INR Amount:     ₹{total_inr:,.2f}")
print(f"Average Exchange Rate: {avg_rate:.4f}")
print(f"Min Exchange Rate:     {min_rate:.4f}")
print(f"Max Exchange Rate:     {max_rate:.4f}")


# --- 8. SAVE RESULTS ---

print("\n" + "=" * 60)
print("SAVING RESULTS")
print("=" * 60)

output_file_name = 'transfers_with_inr.csv'
df.to_csv(output_file_name, index=False)
print(f"✓ Results saved to: {output_file_name}")

# Also save as Excel if openpyxl is available
try:
    output_excel = 'transfers_with_inr.xlsx'
    df.to_excel(output_excel, index=False)
    print(f"✓ Results also saved to: {output_excel}")
except ImportError:
    print("ℹ Install openpyxl to save as Excel: pip install openpyxl")

print("\n" + "=" * 60)
print("PROCESSING COMPLETE!")
print("=" * 60)