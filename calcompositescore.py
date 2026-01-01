#!/usr/bin/env python3
"""
Composite Score Calculator - Supabase Direct Version
- No MySQL dependency
- Direct Supabase queries and updates
- Hierarchical normalization by market cap, sector, industry
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import logging
from collections import defaultdict
import platform

# Get current script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables
if platform.system() == 'Darwin':  # macOS
    load_dotenv('/Users/jaykrish/Documents/digitalocean/.env')
else:  # Server (Linux)
    load_dotenv('/root/.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'calcompositescore.log')),
        logging.StreamHandler()
    ]
)

# Supabase configuration - use correct project
supabase_url = os.getenv('SUPABASE_URL') or 'https://aisqbjjpdztnuerniefl.supabase.co'
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY') or 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFpc3FiampwZHp0bnVlcm5pZWZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI5NzYzNzMsImV4cCI6MjA0ODU1MjM3M30.RbCzw1ImiKD4_khZEj3FaXOkfcHfIWzdcrFha5CxHlE'

# Validate we have the correct project URL
if 'jlfyjqgxtwywvdwmhufe' in supabase_url:
    supabase_url = 'https://aisqbjjpdztnuerniefl.supabase.co'
    supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFpc3FiampwZHp0bnVlcm5pZWZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI5NzYzNzMsImV4cCI6MjA0ODU1MjM3M30.RbCzw1ImiKD4_khZEj3FaXOkfcHfIWzdcrFha5CxHlE'
    logging.info("Using correct Supabase project URL")

supabase: Client = create_client(supabase_url, supabase_key)

def fetch_stock_data():
    """Fetch stock data from Supabase"""
    logging.info("Fetching stock data from Supabase...")

    # Fetch all records with pagination (Supabase default limit is 1000)
    all_data = []
    offset = 0
    batch_size = 1000

    while True:
        response = supabase.table('stock_data').select('*').range(offset, offset + batch_size - 1).execute()
        if not response.data:
            break
        all_data.extend(response.data)
        if len(response.data) < batch_size:
            break
        offset += batch_size

    if not all_data:
        logging.error("No data found in stock_data table")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    logging.info(f"Fetched {len(df)} records from stock_data")
    return df

def fetch_sector_data():
    """Fetch sector scores from Supabase"""
    try:
        response = supabase.table('sector_data').select('sector, normalized_score_3m').execute()
        if response.data:
            return pd.DataFrame(response.data)
    except Exception as e:
        logging.warning(f"Could not fetch sector_data: {e}")
    return pd.DataFrame()

def fetch_industry_data():
    """Fetch industry scores from Supabase"""
    try:
        response = supabase.table('industry_data').select('industry, normalized_score_3m').execute()
        if response.data:
            return pd.DataFrame(response.data)
    except Exception as e:
        logging.warning(f"Could not fetch industry_data: {e}")
    return pd.DataFrame()

def calculate_composite_score(data):
    """Calculate the composite score using hierarchical normalization"""

    # Fill missing analyst ratings
    data['analyst_rating'] = data['analyst_rating'].fillna('Hold')

    # Define the scoring for Analyst Ratings
    analyst_rating_scores = {
        'Strong Buy': 5, 'Strong buy': 5, 'Buy': 4, 'Hold': 3,
        'Neutral': 3, 'Sell': 2, 'Strong Sell': 1, 'Strong sell': 1
    }

    # Map the ratings to scores
    data['analyst_rating_score'] = data['analyst_rating'].map(analyst_rating_scores).fillna(3)

    # Calculate relative differences for Moving Averages and Bollinger Bands
    data['sma50_relative'] = np.where(
        data['price'] != 0,
        (data['sma_50_1_day'] - data['price']) / data['price'],
        0
    )
    data['sma200_relative'] = np.where(
        data['price'] != 0,
        (data['sma_200_1_day'] - data['price']) / data['price'],
        0
    )
    data['bollinger_upper_relative'] = np.where(
        data['price'] != 0,
        (data['bollinger_upper_20_1_day'] - data['price']) / data['price'],
        0
    )
    data['bollinger_middle_relative'] = np.where(
        data['price'] != 0,
        (data['bollinger_basis_20_1_day'] - data['price']) / data['price'],
        0
    )
    data['bollinger_lower_relative'] = np.where(
        data['price'] != 0,
        (data['bollinger_lower_20_1_day'] - data['price']) / data['price'],
        0
    )

    # Calculate MACD histogram
    data['macd_histogram'] = data['macd_12_26_level_1_day'].fillna(0) - data['macd_12_26_signal_1_day'].fillna(0)

    # Perform hierarchical normalization
    normalized_scores, data = hierarchical_normalize(data)

    # Create a temporary DataFrame to store normalized values
    temp_df = pd.DataFrame(index=data.index)

    # Group the normalized scores by metric
    for key, value in normalized_scores.items():
        parts = key.split('_', 2)
        if len(parts) >= 3:
            symbol, level, metric = parts[0], parts[1], '_'.join(parts[2:])
            if f'{metric}_normalized' not in temp_df.columns:
                temp_df[f'{metric}_normalized'] = np.nan
            temp_df.loc[data['symbol'] == symbol, f'{metric}_normalized'] = value

    # Update the main DataFrame with normalized values
    for col in temp_df.columns:
        data[col] = temp_df[col]

    # Define the weights for each factor
    weights = {
        "rsi_14_1_day": 3.0,
        "momentum_10_1_day": 3.0,
        "roc_9_1_day": 3.0,
        "macd_12_26_level_1_day": 3.0,
        "adx_14_1_day": 3.0,
        "stochastic_k_14_3_3_1_day": 2.5,
        "cci_20_1_day": 2.5,
        "sma50_relative": 2.0,
        "sma200_relative": 2.0,
        "bollinger_upper_relative": 2.0,
        "bollinger_middle_relative": 2.0,
        "bollinger_lower_relative": 2.0,
        "chaikin_money_flow_20_1_day": 3.0,
        "performance_ytd": 2.0,
        "performance_1_year": 2.0,
        "performance_6_months": 3.0,
        "performance_3_months": 3.0,
        "performance_1_month": 4.0,
        "performance_1_week": 5.0,
        "analyst_rating_score": 5.0,
        "target_price_performance_1_year": 3.0,
        "macd_histogram": 3.0,
        "chaikin_money_flow_20_1_week": 2.0,
        "chaikin_money_flow_20_1_month": 2.5,
    }

    # Calculate the weighted scores
    weighted_scores = pd.DataFrame(index=data.index)
    for field, weight in weights.items():
        col_name = f'{field}_normalized'
        if col_name in data.columns:
            weighted_scores[field] = data[col_name].fillna(0) * weight
        else:
            weighted_scores[field] = 0

    # Calculate the composite score
    data['composite_score'] = weighted_scores.sum(axis=1).round(2)

    return data, weights

def hierarchical_normalize(data):
    """Three-level hierarchical normalization"""
    logging.info("Starting hierarchical normalization...")

    # Sort by market cap and assign categories
    data = data.sort_values('market_capitalization', ascending=False, na_position='last').reset_index(drop=True)

    logging.info(f"Total stocks before categorization: {len(data)}")

    # Assign categories
    data['market_cap_category'] = 'Micro Cap'
    data.loc[data.index < 100, 'market_cap_category'] = 'Large Cap'
    data.loc[(data.index >= 100) & (data.index < 250), 'market_cap_category'] = 'Mid Cap'
    data.loc[(data.index >= 250) & (data.index < 500), 'market_cap_category'] = 'Small Cap'

    # Handle NaN market caps
    data.loc[pd.isna(data['market_capitalization']), 'market_cap_category'] = None

    logging.info(f"Market Cap Distribution:\n{data['market_cap_category'].value_counts()}")

    # Hierarchical weights
    hierarchical_weights = {
        'industry': 1.0,
        'sector': 0.8,
    }

    normalized_scores = {}

    def normalize_peer_group(group_data, level):
        """Normalize a group of stocks"""
        group_scores = {}

        metrics = [
            'rsi_14_1_day', 'momentum_10_1_day', 'roc_9_1_day', 'macd_12_26_level_1_day',
            'adx_14_1_day', 'stochastic_k_14_3_3_1_day', 'cci_20_1_day',
            'sma50_relative', 'sma200_relative', 'bollinger_upper_relative',
            'bollinger_middle_relative', 'bollinger_lower_relative',
            'chaikin_money_flow_20_1_day', 'chaikin_money_flow_20_1_week',
            'chaikin_money_flow_20_1_month', 'macd_histogram',
            'performance_ytd', 'performance_1_year', 'performance_6_months',
            'performance_3_months', 'performance_1_month', 'performance_1_week',
            'analyst_rating_score', 'target_price_performance_1_year'
        ]

        for metric in metrics:
            if metric not in group_data.columns:
                continue

            valid_data = group_data[metric].dropna()
            min_peers = 3

            if len(valid_data) >= min_peers:
                mean = valid_data.mean()
                std = valid_data.std()

                if std != 0:
                    for symbol in group_data['symbol'].unique():
                        try:
                            value = group_data.loc[group_data['symbol'] == symbol, metric].iloc[0]
                            if pd.notna(value):
                                normalized_value = (value - mean) / std
                                normalized_value *= hierarchical_weights[level]
                                group_scores[f"{symbol}_{level}_{metric}"] = normalized_value
                            else:
                                group_scores[f"{symbol}_{level}_{metric}"] = 0.0
                        except Exception:
                            group_scores[f"{symbol}_{level}_{metric}"] = 0.0

        return group_scores

    # Process each market cap category
    for cap_category in data['market_cap_category'].unique():
        if pd.isna(cap_category):
            continue

        cap_peers = data[data['market_cap_category'] == cap_category]
        logging.info(f"Processing {cap_category}: {len(cap_peers)} stocks")

        # Process each sector within market cap
        for sector in cap_peers['sector'].unique():
            if pd.isna(sector):
                continue

            sector_peers = cap_peers[cap_peers['sector'] == sector]

            # Process each industry within sector
            for industry in sector_peers['industry'].unique():
                if pd.isna(industry):
                    continue

                industry_peers = sector_peers[sector_peers['industry'] == industry]

                if len(industry_peers) >= 2:
                    scores = normalize_peer_group(industry_peers, 'industry')
                else:
                    scores = normalize_peer_group(sector_peers, 'sector')

                normalized_scores.update(scores)

    logging.info("Hierarchical normalization completed")
    return normalized_scores, data

def update_stock_rankings(results):
    """Update composite_score in Supabase stock_rankings table using batch upsert"""
    logging.info("Updating stock_rankings in Supabase (batch mode)...")

    update_time = datetime.now().isoformat()

    # Prepare all records for batch upsert
    records = []
    for _, row in results.iterrows():
        record = {
            'symbol': row['symbol'],
            'composite_score': float(round(row['composite_score'], 2)) if pd.notna(row['composite_score']) else None,
            'update_date': update_time
        }
        if 'market_cap_category' in row and pd.notna(row.get('market_cap_category')):
            record['market_cap_category'] = row['market_cap_category']
        records.append(record)

    # Batch upsert in chunks of 500
    batch_size = 500
    success_count = 0
    error_count = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            response = supabase.table('stock_rankings').upsert(
                batch,
                on_conflict='symbol'
            ).execute()
            success_count += len(batch)
            logging.info(f"Batch {i//batch_size + 1}: Updated {len(batch)} records")
        except Exception as e:
            error_count += len(batch)
            logging.error(f"Batch {i//batch_size + 1} failed: {str(e)}")

    logging.info(f"Update complete: {success_count} successful, {error_count} errors")
    return success_count, error_count

def main():
    try:
        logging.info("=" * 50)
        logging.info("Starting composite score calculation...")

        # Fetch data from Supabase
        data = fetch_stock_data()

        if data.empty:
            logging.error("No data found in database")
            return False

        logging.info(f"Fetched {len(data)} records")

        # Fetch sector and industry data for additional context
        sector_data = fetch_sector_data()
        industry_data = fetch_industry_data()

        # Merge sector and industry scores if available
        if not sector_data.empty:
            data = data.merge(sector_data, on='sector', how='left')
            data.rename(columns={'normalized_score_3m': 'sector_score'}, inplace=True)

        if not industry_data.empty:
            data = data.merge(industry_data, on='industry', how='left')
            data.rename(columns={'normalized_score_3m': 'industry_score'}, inplace=True)

        # Calculate composite scores
        logging.info("Calculating composite scores...")
        analyzed_data, weights = calculate_composite_score(data)

        # Prepare results for update - market_cap_category is added during calculation
        results = analyzed_data[['symbol', 'composite_score']].copy()
        if 'market_cap_category' in analyzed_data.columns:
            results['market_cap_category'] = analyzed_data['market_cap_category']
        else:
            # Re-assign market cap categories if missing
            results = results.merge(data[['symbol', 'market_capitalization']], on='symbol', how='left')
            results = results.sort_values('market_capitalization', ascending=False, na_position='last').reset_index(drop=True)
            results['market_cap_category'] = 'Micro Cap'
            results.loc[results.index < 100, 'market_cap_category'] = 'Large Cap'
            results.loc[(results.index >= 100) & (results.index < 250), 'market_cap_category'] = 'Mid Cap'
            results.loc[(results.index >= 250) & (results.index < 500), 'market_cap_category'] = 'Small Cap'
            results.drop(columns=['market_capitalization'], inplace=True, errors='ignore')

        # Update stock_rankings
        success, errors = update_stock_rankings(results)

        print(f"\nComposite Score Update Summary:")
        print(f"  - Total processed: {len(results)}")
        print(f"  - Successful: {success}")
        print(f"  - Errors: {errors}")

        # Print top 10 by composite score
        top_10 = analyzed_data.nlargest(10, 'composite_score')[['symbol', 'composite_score', 'market_cap_category', 'sector']]
        print(f"\nTop 10 by Composite Score:")
        print(top_10.to_string(index=False))

        logging.info("Composite score calculation completed successfully")
        return True

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
