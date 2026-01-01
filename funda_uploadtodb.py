#!/usr/bin/env python3
"""
Upload TradingView Fundamentals Data to Supabase
Merges fundamental data with existing stock_data table
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import logging
import platform

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment
if platform.system() == 'Darwin':
    load_dotenv('/Users/jaykrish/Documents/digitalocean/.env')
else:
    load_dotenv('/root/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'funda_uploadtodb.log')),
        logging.StreamHandler()
    ]
)

downloads_dir = os.path.join(SCRIPT_DIR, 'tradingview_downloads')

if not os.path.exists(downloads_dir):
    os.makedirs(downloads_dir)

# Find most recent funda CSV
csv_files = [f for f in os.listdir(downloads_dir) if f.startswith('funda') and f.endswith('.csv')]
if not csv_files:
    logging.error(f"No funda CSV files found in {downloads_dir}")
    exit(1)

csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(downloads_dir, x)), reverse=True)
csv_file_name = csv_files[0]
csv_file_path = os.path.join(downloads_dir, csv_file_name)
logging.info(f"Using CSV: {csv_file_path}")

# Supabase config
supabase_url = os.getenv('SUPABASE_URL') or 'https://aisqbjjpdztnuerniefl.supabase.co'
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')

if 'jlfyjqgxtwywvdwmhufe' in supabase_url:
    supabase_url = 'https://aisqbjjpdztnuerniefl.supabase.co'
    supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFpc3FiampwZHp0bnVlcm5pZWZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI5NzYzNzMsImV4cCI6MjA0ODU1MjM3M30.RbCzw1ImiKD4_khZEj3FaXOkfcHfIWzdcrFha5CxHlE'

supabase: Client = create_client(supabase_url, supabase_key)

# Column mapping
COLUMN_MAPPING = {
    'Symbol': 'symbol',
    'Description': 'description',
    'Price': 'price',
    'Price - Currency': 'price_currency',
    'Market capitalization': 'market_capitalization',
    'Market capitalization - Currency': 'market_capitalization_currency',
    'Sector': 'sector',
    'Industry': 'industry',
    'Analyst Rating': 'analyst_rating',
    'Price to earnings ratio': 'pe_ratio',
    'Price to book ratio': 'price_to_book_ratio',
    'Price to sales ratio': 'price_to_sales_ratio',
    'Price to earning to growth, Trailing 12 months': 'price_to_earnings_growth_ttm',
    'Price to cash flow ratio': 'price_to_free_cash_flow_ratio',
    'Enterprise value': 'enterprise_value',
    'Enterprise value to revenue ratio, Trailing 12 months': 'enterprise_value_to_revenue_ttm',
    'Enterprise value to EBITDA ratio, Trailing 12 months': 'enterprise_value_to_ebitda_ttm',
    'Return on equity %, Trailing 12 months': 'return_on_equity_ttm',
    'Return on assets %, Trailing 12 months': 'return_on_assets_ttm',
    'Return on invested capital %, Trailing 12 months': 'return_on_invested_capital_ttm',
    'Gross margin %, Annual': 'gross_margin_annual',
    'Operating margin %, Annual': 'operating_margin_ttm',
    'Net margin %, Trailing 12 months': 'net_margin_ttm',
    'Earnings per share diluted, Trailing 12 months': 'eps_diluted_ttm',
    'Earnings per share basic, Trailing 12 months': 'basic_eps_ttm',
    'Earnings per share diluted growth %, TTM YoY': 'eps_diluted_growth_ttm_yoy',
    'Earnings per share diluted growth %, Annual YoY': 'eps_diluted_growth_annual_yoy',
    'Earnings per share diluted growth %, Quarterly YoY': 'eps_diluted_growth_quarterly_yoy',
    'Earnings per share diluted growth %, Quarterly QoQ': 'eps_diluted_growth_quarterly_qoq',
    'Earnings per share estimate, Quarterly': 'eps_forecast_quarterly',
    'Total revenue, Annual': 'total_revenue_annual',
    'Net income, Annual': 'net_income_annual',
    'Net income, Trailing 12 months': 'net_income_ttm',
    'EBITDA, Trailing 12 months': 'ebitda_ttm',
    'Revenue growth %, Annual YoY': 'revenue_growth_annual_yoy',
    'Revenue growth %, Quarterly YoY': 'revenue_growth_quarterly_yoy',
    'Revenue growth %, Quarterly QoQ': 'revenue_growth_quarterly_qoq',
    'Net income growth %, Annual YoY': 'net_income_growth_annual_yoy',
    'Free cash flow, Annual': 'free_cash_flow_annual',
    'Operating cash flow per share, Trailing 12 months': 'cash_from_operating_activities_ttm',
    'Total debt, Quarterly': 'total_debt_quarterly',
    'Debt to equity ratio, Quarterly': 'debt_to_equity_ratio_quarterly',
    'Debt to EBITDA ratio, Annual': 'debt_to_ebitda_ratio_annual',
    'Interest coverage, Trailing 12 months': 'ebitda_interest_coverage_ttm',
    'Current ratio, Quarterly': 'current_ratio_quarterly',
    'Quick ratio, Quarterly': 'quick_ratio_quarterly',
    'Dividend yield %, Trailing 12 months': 'dividend_yield_ttm',
    'Cash & equivalents, Annual': 'cash_and_equivalents_annual',
    'Total common shares outstanding': 'shares_outstanding',
    'Free float %': 'float_percent'
}

def clean_value(value, column_name=None):
    """Clean values for database"""
    if pd.isna(value) or value is None:
        return None

    # Handle shares_outstanding specially - convert float to int
    if column_name == 'shares_outstanding':
        if isinstance(value, (np.float64, np.float32, float)):
            if np.isnan(value) or np.isinf(value):
                return None
            return int(float(value))
        if isinstance(value, (np.int64, np.int32)):
            return int(value)
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
    """Load and prepare fundamental data"""
    logging.info(f"Loading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    logging.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    # Rename columns
    df_renamed = df.rename(columns=COLUMN_MAPPING)

    # Keep only mapped columns
    valid_columns = [col for col in df_renamed.columns if col in COLUMN_MAPPING.values()]
    df_renamed = df_renamed[valid_columns]

    df_renamed['last_modified_date'] = datetime.now().isoformat()

    logging.info(f"Prepared {len(df_renamed)} rows with {len(df_renamed.columns)} columns")
    return df_renamed

def upload_to_supabase(df):
    """Upload to Supabase with upsert"""
    success_count = 0
    error_count = 0
    batch_size = 100

    records = df.to_dict('records')

    cleaned_records = []
    for record in records:
        cleaned_record = {k: clean_value(v, k) for k, v in record.items()}
        if cleaned_record.get('symbol'):
            cleaned_records.append(cleaned_record)

    logging.info(f"Uploading {len(cleaned_records)} records...")

    for i in range(0, len(cleaned_records), batch_size):
        batch = cleaned_records[i:i+batch_size]
        try:
            result = supabase.table('stock_data').upsert(batch, on_conflict='symbol').execute()
            success_count += len(batch)
            logging.info(f"Batch {i//batch_size + 1}: {len(batch)} records (total: {success_count})")
        except Exception as e:
            error_count += len(batch)
            logging.error(f"Batch {i//batch_size + 1} error: {str(e)}")
            for record in batch:
                try:
                    supabase.table('stock_data').upsert(record, on_conflict='symbol').execute()
                    success_count += 1
                    error_count -= 1
                except Exception as row_error:
                    logging.error(f"Error uploading {record.get('symbol')}: {str(row_error)}")

    logging.info(f"Upload complete: {success_count} successful, {error_count} errors")
    return success_count, error_count

def main():
    try:
        logging.info("=" * 50)
        logging.info("Starting fundamentals upload to Supabase...")

        df = load_and_prepare_data(csv_file_path)
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
            return True

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
