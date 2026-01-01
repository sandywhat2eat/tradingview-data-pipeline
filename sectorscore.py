#!/usr/bin/env python3
"""
Sector Scores Calculator - Server Ready Version
- Fixed paths for server deployment
- Environment variables from .env file
- Improved error handling and logging
- Server environment optimized
"""

import pandas as pd
import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime

# Get current script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from root .env file
load_dotenv('/root/.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'sectorscore.log')),
        logging.StreamHandler()
    ]
)

# Supabase configuration from environment variables
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([url, key]):
    logging.error("Missing Supabase environment variables. Please check .env file.")
    print("Error: Missing Supabase environment variables. Please check .env file.")
    exit(1)

supabase: Client = create_client(url, key)

# Define the weights for different holding periods
weights_3m = {
    'change_pct': 20, 'perf_1w': 10, 'perf_1m': 20, 'perf_3m': 30,
    'perf_6m': 10, 'perf_ytd': 5, 'perf_1y': 5, 'market_cap': 0, 'stocks': 0
}

weights_6m = {
    'change_pct': 15, 'perf_1w': 8, 'perf_1m': 15, 'perf_3m': 20,
    'perf_6m': 25, 'perf_ytd': 10, 'perf_1y': 7, 'market_cap': 0, 'stocks': 0
}

weights_1y = {
    'change_pct': 10, 'perf_1w': 5, 'perf_1m': 10, 'perf_3m': 15,
    'perf_6m': 20, 'perf_ytd': 10, 'perf_1y': 15, 'market_cap': 10, 'stocks': 5
}

def clean_and_convert(value):
    """Clean and convert string values to numeric"""
    if isinstance(value, str):
        # Remove various symbols and convert to numeric
        cleaned = value.replace('−', '-').replace('%', '').replace(',', '').replace('T INR', '').replace('B INR', '').replace('+', '').strip()
        return pd.to_numeric(cleaned, errors='coerce')
    return value

def min_max_normalize(series):
    """Normalize series using min-max normalization"""
    min_val = series.min()
    max_val = series.max()
    if min_val == max_val:
        return pd.Series(1, index=series.index)
    return (series - min_val) / (max_val - min_val)

def calculate_normalized_weighted_score(row, weights):
    """Calculate weighted score based on normalized values"""
    return sum(row[f'normalized_{col}'] * weight for col, weight in weights.items() if f'normalized_{col}' in row.index)

def get_timestamp():
    """Get current timestamp"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    try:
        logging.info("Starting sector score calculation...")
        print(f"[{get_timestamp()}] Starting sector score calculation.")
        
        # Fetch data from Supabase
        logging.info("Fetching data from Supabase...")
        print(f"[{get_timestamp()}] Fetching data from Supabase...")
        response = supabase.table('sector_data').select('*').execute()
        
        if not response.data:
            logging.warning("No data found in sector_data table.")
            print(f"[{get_timestamp()}] No data found in sector_data table.")
            return False
        
        # Convert to DataFrame
        sector_data = pd.DataFrame(response.data)
        logging.info(f"Fetched {len(sector_data)} records from sector_data table.")
        print(f"[{get_timestamp()}] Fetched {len(sector_data)} records from sector_data table.")

        # Clean and convert data
        columns_to_clean = ['market_cap', 'change_pct', 'perf_1w', 'perf_1m', 'perf_3m', 
                            'perf_6m', 'perf_ytd', 'perf_1y', 'stocks']
        
        logging.info("Cleaning and converting data...")
        print(f"[{get_timestamp()}] Cleaning and converting data...")
        for col in columns_to_clean:
            if col in sector_data.columns:
                sector_data[col] = sector_data[col].apply(clean_and_convert)

        # Normalize relevant columns
        columns_to_normalize = ['market_cap', 'change_pct', 'perf_1w', 'perf_1m', 'perf_3m', 
                                'perf_6m', 'perf_ytd', 'perf_1y', 'stocks']
        
        logging.info("Normalizing data...")
        print(f"[{get_timestamp()}] Normalizing data...")
        for col in columns_to_normalize:
            if col in sector_data.columns:
                sector_data[f'normalized_{col}'] = min_max_normalize(sector_data[col])

        # Calculate normalized scores
        logging.info("Calculating normalized scores...")
        print(f"[{get_timestamp()}] Calculating normalized scores...")
        sector_data['normalized_score_3m'] = sector_data.apply(lambda row: calculate_normalized_weighted_score(row, weights_3m), axis=1)
        sector_data['normalized_score_6m'] = sector_data.apply(lambda row: calculate_normalized_weighted_score(row, weights_6m), axis=1)
        sector_data['normalized_score_1y'] = sector_data.apply(lambda row: calculate_normalized_weighted_score(row, weights_1y), axis=1)

        # Update the database with new scores
        logging.info("Updating Supabase with normalized scores...")
        print(f"[{get_timestamp()}] Updating Supabase with normalized scores...")
        
        update_count = 0
        error_count = 0
        
        for index, row in sector_data.iterrows():
            try:
                # Handle NaN values by converting to None
                score_3m = row['normalized_score_3m']
                score_6m = row['normalized_score_6m']
                score_1y = row['normalized_score_1y']
                
                update_data = {
                    'normalized_score_3m': round(float(score_3m), 4) if pd.notna(score_3m) else None,
                    'normalized_score_6m': round(float(score_6m), 4) if pd.notna(score_6m) else None,
                    'normalized_score_1y': round(float(score_1y), 4) if pd.notna(score_1y) else None,
                    'updated_at': datetime.now().isoformat()
                }
                
                # Update record in Supabase
                result = supabase.table('sector_data').update(update_data).eq('sector', row['sector']).execute()
                
                if result.data:
                    logging.info(f"Updated scores for sector: {row['sector']}")
                    print(f"[{get_timestamp()}] Updated scores for sector: {row['sector']}")
                    update_count += 1
                else:
                    logging.warning(f"Failed to update sector: {row['sector']}")
                    print(f"[{get_timestamp()}] Failed to update sector: {row['sector']}")
                    error_count += 1
                    
            except Exception as e:
                logging.error(f"Error updating sector {row.get('sector', 'unknown')}: {str(e)}")
                error_count += 1

        logging.info(f"Successfully updated {update_count} sector scores in Supabase. {error_count} errors.")
        print(f"[{get_timestamp()}] Successfully updated {update_count} sector scores in Supabase. {error_count} errors.")
        
        return True

    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        print(f"[{get_timestamp()}] Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    logging.info("Script execution started")
    print(f"[{get_timestamp()}] Script execution started.")
    
    success = main()
    
    if success:
        logging.info("Script execution completed successfully")
        print(f"[{get_timestamp()}] Script execution completed successfully.")
    else:
        logging.error("Script execution failed")
        print(f"[{get_timestamp()}] Script execution failed.")
        exit(1)