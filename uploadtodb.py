#!/usr/bin/env python3
"""
Upload TradingView Data to Supabase - Direct Upload Version
- No MySQL dependency
- Direct Supabase upload
- Proper column mapping from CSV to DB
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import logging
import platform

# Get current script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from .env file
if platform.system() == 'Darwin':  # macOS
    load_dotenv('/Users/jaykrish/Documents/digitalocean/.env')
else:  # Server (Linux)
    load_dotenv('/root/.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'uploadtodb.log')),
        logging.StreamHandler()
    ]
)

# Get the absolute path to downloads directory
downloads_dir = os.path.join(SCRIPT_DIR, 'tradingview_downloads')

# Create downloads directory if it doesn't exist
if not os.path.exists(downloads_dir):
    os.makedirs(downloads_dir)
    logging.info(f"Created downloads directory: {downloads_dir}")

# Find the most recent technicals CSV file
csv_files = [f for f in os.listdir(downloads_dir) if f.startswith('Technicals') and f.endswith('.csv')]
if not csv_files:
    logging.error(f"No technicals CSV files found in {downloads_dir}")
    print(f"Error: No technicals CSV files found in {downloads_dir}")
    exit(1)

# Sort files by modification time (newest first)
csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(downloads_dir, x)), reverse=True)

# Use the most recent file
csv_file_name = csv_files[0]
csv_file_path = os.path.join(downloads_dir, csv_file_name)
logging.info(f"Using CSV file: {csv_file_path}")

# Supabase configuration - use correct project
supabase_url = os.getenv('SUPABASE_URL') or 'https://aisqbjjpdztnuerniefl.supabase.co'
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY') or 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFpc3FiampwZHp0bnVlcm5pZWZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI5NzYzNzMsImV4cCI6MjA0ODU1MjM3M30.RbCzw1ImiKD4_khZEj3FaXOkfcHfIWzdcrFha5CxHlE'

# Validate we have the correct project URL
if 'jlfyjqgxtwywvdwmhufe' in supabase_url:
    # Old project URL in .env - use the correct one
    supabase_url = 'https://aisqbjjpdztnuerniefl.supabase.co'
    supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFpc3FiampwZHp0bnVlcm5pZWZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI5NzYzNzMsImV4cCI6MjA0ODU1MjM3M30.RbCzw1ImiKD4_khZEj3FaXOkfcHfIWzdcrFha5CxHlE'
    logging.info("Using correct Supabase project URL")

supabase: Client = create_client(supabase_url, supabase_key)

# Column mapping from CSV to database
COLUMN_MAPPING = {
    'Symbol': 'symbol',
    'Description': 'description',
    'Technical Rating 1 day': 'technical_rating_1_day',
    'Moving Averages Rating 1 day': 'moving_averages_rating_1_day',
    'Oscillators Rating 1 day': 'oscillators_rating_1_day',
    'Relative Strength Index (14) 1 day': 'rsi_14_1_day',
    'Momentum (10) 1 day': 'momentum_10_1_day',
    'Awesome Oscillator 1 day': 'awesome_oscillator_1_day',
    'Commodity Channel Index (20) 1 day': 'cci_20_1_day',
    'Stochastic (14,3,3) 1 day, %K': 'stochastic_k_14_3_3_1_day',
    'Stochastic (14,3,3) 1 day, %D': 'stochastic_d_14_3_3_1_day',
    'Candlestick Pattern 1 day': 'candlestick_pattern_1_day',
    'Rate of Change (9) 1 day': 'roc_9_1_day',
    'Moving Average Convergence Divergence (12,26) 1 day, Level': 'macd_12_26_level_1_day',
    'Moving Average Convergence Divergence (12,26) 1 day, Signal': 'macd_12_26_signal_1_day',
    'Average Directional Index (14) 1 day': 'adx_14_1_day',
    'Ultimate Oscillator (7,14,28) 1 day': 'ultimate_oscillator_7_14_28_1_day',
    'Technical Rating 1 week': 'technical_rating_1_week',
    'Sector': 'sector',
    'Industry': 'industry',
    'Analyst Rating': 'analyst_rating',
    'Performance % Year to date': 'performance_ytd',
    'Performance % 1 year': 'performance_1_year',
    'Performance % 6 months': 'performance_6_months',
    'Performance % 3 months': 'performance_3_months',
    'Performance % 1 month': 'performance_1_month',
    'Performance % 1 week': 'performance_1_week',
    'Target price 1 year': 'target_price_1_year',
    'Target price 1 year - Currency': 'target_price_1_year_currency',
    'Target price performance % 1 year': 'target_price_performance_1_year',
    'Price': 'price',
    'Price - Currency': 'price_currency',
    'Simple Moving Average (50) 1 day': 'sma_50_1_day',
    'Simple Moving Average (200) 1 day': 'sma_200_1_day',
    'Bollinger Bands (20) 1 day, Upper': 'bollinger_upper_20_1_day',
    'Bollinger Bands (20) 1 day, Basis': 'bollinger_basis_20_1_day',
    'Bollinger Bands (20) 1 day, Lower': 'bollinger_lower_20_1_day',
    'Williams Percent Range (14) 1 day': 'williams_percent_range_14_1_day',
    'Moving Average Convergence Divergence (12,26) 1 day, Level.1': 'macd_12_26_level_1_day_2',
    'Moving Average Convergence Divergence (12,26) 1 day, Signal.1': 'macd_12_26_signal_1_day_2',
    'Chaikin Money Flow (20) 1 day': 'chaikin_money_flow_20_1_day',
    'Chaikin Money Flow (20) 1 week': 'chaikin_money_flow_20_1_week',
    'Chaikin Money Flow (20) 1 month': 'chaikin_money_flow_20_1_month',
    'Market capitalization': 'market_capitalization',
    'Market capitalization - Currency': 'market_capitalization_currency',
    'Beta 1 year': 'beta_1_year',
    'Volatility 1 month': 'volatility_1_month',
    'Volatility 1 week': 'volatility_1w',
    'Index': 'index_memberships'
}

def clean_value(value):
    """Clean and convert values for database insertion."""
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, (np.float64, np.float32, float)):
        if np.isnan(value) or np.isinf(value):
            return None
        return round(float(value), 2)
    if isinstance(value, (np.int64, np.int32)):
        return int(value)
    if isinstance(value, str):
        return value.strip() if value.strip() else None
    return value

def load_and_prepare_data(csv_path):
    """Load CSV and prepare data for upload."""
    logging.info(f"Loading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    logging.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    # Rename columns using mapping
    df_renamed = df.rename(columns=COLUMN_MAPPING)

    # Only keep columns that exist in our mapping
    valid_columns = [col for col in df_renamed.columns if col in COLUMN_MAPPING.values()]
    df_renamed = df_renamed[valid_columns]

    # Add last_modified_date
    df_renamed['last_modified_date'] = datetime.now().isoformat()

    logging.info(f"Prepared {len(df_renamed)} rows with {len(df_renamed.columns)} columns")
    return df_renamed

def upload_to_supabase(df):
    """Upload DataFrame to Supabase with upsert."""
    success_count = 0
    error_count = 0
    batch_size = 100

    # Convert DataFrame to list of dicts
    records = df.to_dict('records')

    # Clean all values
    cleaned_records = []
    for record in records:
        cleaned_record = {k: clean_value(v) for k, v in record.items()}
        # Skip if no symbol
        if cleaned_record.get('symbol'):
            cleaned_records.append(cleaned_record)

    logging.info(f"Uploading {len(cleaned_records)} records to Supabase...")

    # Upload in batches
    for i in range(0, len(cleaned_records), batch_size):
        batch = cleaned_records[i:i+batch_size]
        try:
            result = supabase.table('stock_data').upsert(batch, on_conflict='symbol').execute()
            success_count += len(batch)
            logging.info(f"Uploaded batch {i//batch_size + 1}: {len(batch)} records (total: {success_count})")
        except Exception as e:
            error_count += len(batch)
            logging.error(f"Error uploading batch {i//batch_size + 1}: {str(e)}")
            # Try individual records on batch failure
            for record in batch:
                try:
                    supabase.table('stock_data').upsert(record, on_conflict='symbol').execute()
                    success_count += 1
                    error_count -= 1
                except Exception as row_error:
                    logging.error(f"Error uploading {record.get('symbol', 'unknown')}: {str(row_error)}")

    logging.info(f"Upload complete: {success_count} successful, {error_count} errors")
    return success_count, error_count

def main():
    try:
        logging.info("=" * 50)
        logging.info("Starting TradingView data upload to Supabase...")

        # Load and prepare data
        df = load_and_prepare_data(csv_file_path)

        # Upload to Supabase
        success, errors = upload_to_supabase(df)

        print(f"\nUpload Summary:")
        print(f"  - Total records: {len(df)}")
        print(f"  - Successful: {success}")
        print(f"  - Errors: {errors}")

        if errors == 0:
            logging.info("Upload completed successfully!")
            return True
        else:
            logging.warning(f"Upload completed with {errors} errors")
            return True  # Still return True if some records uploaded

    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
