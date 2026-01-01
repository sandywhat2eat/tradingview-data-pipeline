#!/usr/bin/env python3
"""
Fundamental Score Calculator
- Industry-relative percentile scoring
- Fallback hierarchy: Industry → Sector → All stocks
- Weights: Quality 40%, Growth 30%, Valuation 20%, Financial Health 10%
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import logging
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
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'calfundamentalscore.log')),
        logging.StreamHandler()
    ]
)

# Supabase configuration
supabase_url = os.getenv('SUPABASE_URL') or 'https://aisqbjjpdztnuerniefl.supabase.co'
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY') or 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFpc3FiampwZHp0bnVlcm5pZWZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI5NzYzNzMsImV4cCI6MjA0ODU1MjM3M30.RbCzw1ImiKD4_khZEj3FaXOkfcHfIWzdcrFha5CxHlE'

# Validate correct project URL
if 'jlfyjqgxtwywvdwmhufe' in supabase_url:
    supabase_url = 'https://aisqbjjpdztnuerniefl.supabase.co'
    supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFpc3FiampwZHp0bnVlcm5pZWZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI5NzYzNzMsImV4cCI6MjA0ODU1MjM3M30.RbCzw1ImiKD4_khZEj3FaXOkfcHfIWzdcrFha5CxHlE'
    logging.info("Using correct Supabase project URL")

supabase: Client = create_client(supabase_url, supabase_key)

# Minimum peers required for relative scoring
MIN_PEERS = 5

# =============================================================================
# METRIC DEFINITIONS
# =============================================================================

# Quality Metrics (40% total weight) - Higher is better
QUALITY_METRICS = {
    'return_on_equity_ttm': {'weight': 0.12, 'higher_is_better': True},
    'return_on_invested_capital_ttm': {'weight': 0.10, 'higher_is_better': True},
    'operating_margin_ttm': {'weight': 0.08, 'higher_is_better': True},
    'net_margin_ttm': {'weight': 0.06, 'higher_is_better': True},
    'gross_margin_annual': {'weight': 0.04, 'higher_is_better': True},
}

# Growth Metrics (30% total weight) - Higher is better
GROWTH_METRICS = {
    'eps_diluted_growth_ttm_yoy': {'weight': 0.10, 'higher_is_better': True},
    'revenue_growth_annual_yoy': {'weight': 0.08, 'higher_is_better': True},
    'eps_diluted_growth_annual_yoy': {'weight': 0.06, 'higher_is_better': True},
    'net_income_growth_annual_yoy': {'weight': 0.06, 'higher_is_better': True},
}

# Valuation Metrics (20% total weight) - Lower is better (inverted)
VALUATION_METRICS = {
    'pe_ratio': {'weight': 0.07, 'higher_is_better': False},
    'price_to_earnings_growth_ttm': {'weight': 0.05, 'higher_is_better': False},
    'enterprise_value_to_ebitda_ttm': {'weight': 0.04, 'higher_is_better': False},
    'price_to_book_ratio': {'weight': 0.02, 'higher_is_better': False},
    'price_to_sales_ratio': {'weight': 0.02, 'higher_is_better': False},
}

# Financial Health Metrics (10% total weight)
HEALTH_METRICS = {
    'current_ratio_quarterly': {'weight': 0.03, 'higher_is_better': True, 'cap': 3.0},
    'debt_to_equity_ratio_quarterly': {'weight': 0.03, 'higher_is_better': False},
    'quick_ratio_quarterly': {'weight': 0.02, 'higher_is_better': True, 'cap': 2.0},
    'ebitda_interest_coverage_ttm': {'weight': 0.02, 'higher_is_better': True, 'cap': 10.0},
}

ALL_METRICS = {**QUALITY_METRICS, **GROWTH_METRICS, **VALUATION_METRICS, **HEALTH_METRICS}


def fetch_stock_data():
    """Fetch stock data with fundamental metrics from Supabase"""
    logging.info("Fetching stock data from Supabase...")

    # Select only required columns
    columns = ['symbol', 'sector', 'industry', 'market_capitalization'] + list(ALL_METRICS.keys())
    columns_str = ','.join(columns)

    response = supabase.table('stock_data').select(columns_str).execute()

    if not response.data:
        logging.error("No data found in stock_data table")
        return pd.DataFrame()

    df = pd.DataFrame(response.data)
    logging.info(f"Fetched {len(df)} records from stock_data")
    return df


def fetch_market_cap_categories():
    """Fetch market cap categories from stock_rankings"""
    logging.info("Fetching market cap categories from stock_rankings...")

    response = supabase.table('stock_rankings').select('symbol, market_cap_category').execute()

    if not response.data:
        logging.warning("No market cap categories found")
        return pd.DataFrame()

    return pd.DataFrame(response.data)


def calculate_percentile(values: pd.Series, value: float, higher_is_better: bool) -> float:
    """
    Calculate percentile score (0-100) for a value within a series.

    For higher_is_better=True: higher values get higher percentile
    For higher_is_better=False: lower values get higher percentile (inverted)
    """
    if pd.isna(value):
        return np.nan

    valid_values = values.dropna()
    if len(valid_values) < 2:
        return 50.0  # Not enough data, assign neutral score

    if higher_is_better:
        # Count how many values are less than this value
        rank = (valid_values < value).sum()
    else:
        # Count how many values are greater than this value (inverted)
        rank = (valid_values > value).sum()

    percentile = (rank / len(valid_values)) * 100
    return round(percentile, 2)


def get_peer_group(df: pd.DataFrame, symbol: str, sector: str, industry: str) -> tuple:
    """
    Determine the peer group for a stock using hierarchical fallback.
    Returns (peer_df, peer_level) where peer_level is 'industry', 'sector', or 'all'
    """
    # Try industry first
    if pd.notna(industry):
        industry_peers = df[df['industry'] == industry]
        if len(industry_peers) >= MIN_PEERS:
            return industry_peers, 'industry'

    # Fallback to sector
    if pd.notna(sector):
        sector_peers = df[df['sector'] == sector]
        if len(sector_peers) >= MIN_PEERS:
            return sector_peers, 'sector'

    # Fallback to all stocks
    return df, 'all'


def apply_caps(df: pd.DataFrame) -> pd.DataFrame:
    """Apply caps to metrics that have maximum meaningful values"""
    df = df.copy()

    for metric, config in ALL_METRICS.items():
        if 'cap' in config and metric in df.columns:
            cap_value = config['cap']
            df[metric] = df[metric].clip(upper=cap_value)
            logging.debug(f"Applied cap of {cap_value} to {metric}")

    return df


def filter_valid_valuation(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    For valuation metrics, exclude negative values (loss-making companies).
    Negative PE, PEG, EV/EBITDA don't make sense for comparison.
    """
    if metric in VALUATION_METRICS:
        return df[df[metric] > 0]
    return df


def calculate_category_score(scores: dict, category_metrics: dict) -> float:
    """
    Calculate weighted average score for a category.
    Redistributes weights if some metrics are missing.
    """
    available_scores = []
    available_weights = []

    for metric, config in category_metrics.items():
        if metric in scores and not pd.isna(scores[metric]):
            available_scores.append(scores[metric])
            available_weights.append(config['weight'])

    if not available_scores:
        return np.nan

    # Normalize weights to sum to 1
    total_weight = sum(available_weights)
    normalized_weights = [w / total_weight for w in available_weights]

    # Calculate weighted average
    weighted_sum = sum(s * w for s, w in zip(available_scores, normalized_weights))
    return round(weighted_sum, 2)


def calculate_fundamental_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function to calculate fundamental scores for all stocks.
    Uses industry-relative percentile scoring with fallback hierarchy.
    """
    logging.info("Starting fundamental score calculation...")

    # Apply caps to relevant metrics
    df = apply_caps(df)

    # Initialize score columns
    for metric in ALL_METRICS:
        df[f'{metric}_percentile'] = np.nan

    df['quality_score'] = np.nan
    df['growth_score'] = np.nan
    df['valuation_score'] = np.nan
    df['health_score'] = np.nan
    df['fundamental_score'] = np.nan
    df['peer_level'] = ''

    # Track peer group usage
    peer_level_counts = {'industry': 0, 'sector': 0, 'all': 0}

    # Calculate percentile scores for each stock
    for idx, row in df.iterrows():
        symbol = row['symbol']
        sector = row.get('sector')
        industry = row.get('industry')

        # Get peer group
        peers, peer_level = get_peer_group(df, symbol, sector, industry)
        df.at[idx, 'peer_level'] = peer_level
        peer_level_counts[peer_level] += 1

        # Calculate percentile for each metric
        metric_scores = {}

        for metric, config in ALL_METRICS.items():
            if metric not in df.columns:
                continue

            value = row.get(metric)

            # For valuation metrics, exclude negative values
            if metric in VALUATION_METRICS:
                if pd.notna(value) and value <= 0:
                    # Loss-making company - assign 0 score for valuation
                    metric_scores[metric] = 0.0
                    df.at[idx, f'{metric}_percentile'] = 0.0
                    continue
                # Filter peers to only positive values
                valid_peers = filter_valid_valuation(peers, metric)
                peer_values = valid_peers[metric]
            else:
                peer_values = peers[metric]

            # Calculate percentile
            percentile = calculate_percentile(
                peer_values,
                value,
                config['higher_is_better']
            )

            metric_scores[metric] = percentile
            df.at[idx, f'{metric}_percentile'] = percentile

        # Calculate category scores
        df.at[idx, 'quality_score'] = calculate_category_score(metric_scores, QUALITY_METRICS)
        df.at[idx, 'growth_score'] = calculate_category_score(metric_scores, GROWTH_METRICS)
        df.at[idx, 'valuation_score'] = calculate_category_score(metric_scores, VALUATION_METRICS)
        df.at[idx, 'health_score'] = calculate_category_score(metric_scores, HEALTH_METRICS)

        # Calculate final fundamental score (weighted combination)
        quality = df.at[idx, 'quality_score'] if pd.notna(df.at[idx, 'quality_score']) else 50
        growth = df.at[idx, 'growth_score'] if pd.notna(df.at[idx, 'growth_score']) else 50
        valuation = df.at[idx, 'valuation_score'] if pd.notna(df.at[idx, 'valuation_score']) else 50
        health = df.at[idx, 'health_score'] if pd.notna(df.at[idx, 'health_score']) else 50

        fundamental_score = (
            quality * 0.40 +
            growth * 0.30 +
            valuation * 0.20 +
            health * 0.10
        )
        df.at[idx, 'fundamental_score'] = round(fundamental_score, 2)

    logging.info(f"Peer group distribution: {peer_level_counts}")
    logging.info("Fundamental score calculation completed")

    return df


def calculate_fundamental_ranks(df: pd.DataFrame, market_cap_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate fundamental rank within each market cap category"""
    logging.info("Calculating fundamental ranks within market cap categories...")

    # Merge market cap categories
    if not market_cap_df.empty:
        df = df.merge(market_cap_df, on='symbol', how='left')
    else:
        # If no market cap categories, assign based on market_capitalization
        df = df.sort_values('market_capitalization', ascending=False, na_position='last').reset_index(drop=True)
        df['market_cap_category'] = 'Micro Cap'
        df.loc[df.index < 100, 'market_cap_category'] = 'Large Cap'
        df.loc[(df.index >= 100) & (df.index < 250), 'market_cap_category'] = 'Mid Cap'
        df.loc[(df.index >= 250) & (df.index < 500), 'market_cap_category'] = 'Small Cap'

    # Calculate rank within each category
    df['fundamental_rank'] = 0

    for category in df['market_cap_category'].dropna().unique():
        mask = df['market_cap_category'] == category
        category_df = df[mask].sort_values('fundamental_score', ascending=False)
        df.loc[category_df.index, 'fundamental_rank'] = range(1, len(category_df) + 1)

    logging.info(f"Ranks calculated for {len(df['market_cap_category'].dropna().unique())} categories")

    return df


def update_stock_rankings(df: pd.DataFrame):
    """Update fundamental scores in stock_rankings table using batch upsert"""
    logging.info("Updating stock_rankings with fundamental scores (batch mode)...")

    update_time = datetime.now().isoformat()

    # Prepare all records for batch upsert
    records = []
    for _, row in df.iterrows():
        record = {
            'symbol': row['symbol'],
            'fundamental_score': float(round(row['fundamental_score'], 2)) if pd.notna(row['fundamental_score']) else None,
            'fundamental_rank': int(row['fundamental_rank']) if pd.notna(row['fundamental_rank']) and row['fundamental_rank'] > 0 else None,
            'quality_score': float(round(row['quality_score'], 2)) if pd.notna(row['quality_score']) else None,
            'growth_score': float(round(row['growth_score'], 2)) if pd.notna(row['growth_score']) else None,
            'valuation_score': float(round(row['valuation_score'], 2)) if pd.notna(row['valuation_score']) else None,
            'health_score': float(round(row['health_score'], 2)) if pd.notna(row['health_score']) else None,
            'fundamental_update_date': update_time
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
        logging.info("Starting fundamental score calculation...")

        # Fetch data
        df = fetch_stock_data()
        if df.empty:
            logging.error("No data found in database")
            return False

        market_cap_df = fetch_market_cap_categories()

        # Log data coverage
        logging.info(f"Data coverage:")
        for metric in ALL_METRICS:
            if metric in df.columns:
                coverage = df[metric].notna().sum()
                pct = round(coverage / len(df) * 100, 1)
                logging.info(f"  {metric}: {coverage}/{len(df)} ({pct}%)")

        # Calculate fundamental scores
        df = calculate_fundamental_scores(df)

        # Calculate ranks within market cap categories
        df = calculate_fundamental_ranks(df, market_cap_df)

        # Update database
        success, errors = update_stock_rankings(df)

        # Print summary
        print(f"\n{'='*50}")
        print("FUNDAMENTAL SCORE UPDATE SUMMARY")
        print(f"{'='*50}")
        print(f"Total processed: {len(df)}")
        print(f"Successful: {success}")
        print(f"Errors: {errors}")

        # Score distribution
        print(f"\nScore Distribution:")
        print(f"  Mean: {df['fundamental_score'].mean():.2f}")
        print(f"  Std:  {df['fundamental_score'].std():.2f}")
        print(f"  Min:  {df['fundamental_score'].min():.2f}")
        print(f"  Max:  {df['fundamental_score'].max():.2f}")

        # Peer level distribution
        print(f"\nPeer Group Distribution:")
        print(df['peer_level'].value_counts().to_string())

        # Top 10 by fundamental score
        top_10 = df.nlargest(10, 'fundamental_score')[
            ['symbol', 'fundamental_score', 'quality_score', 'growth_score',
             'valuation_score', 'health_score', 'industry', 'peer_level']
        ]
        print(f"\nTop 10 by Fundamental Score:")
        print(top_10.to_string(index=False))

        # Bottom 10 by fundamental score
        bottom_10 = df.nsmallest(10, 'fundamental_score')[
            ['symbol', 'fundamental_score', 'quality_score', 'growth_score',
             'valuation_score', 'health_score', 'industry', 'peer_level']
        ]
        print(f"\nBottom 10 by Fundamental Score:")
        print(bottom_10.to_string(index=False))

        logging.info("Fundamental score calculation completed successfully")
        return True

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
