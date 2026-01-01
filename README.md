# TradingView Data Pipeline

Automated pipeline to download technical and fundamental data from TradingView screeners and load into Supabase.

## Pipeline Overview

### 1. Technical Data Pipeline
Downloads technical indicators and calculates composite scores.

**Flow:**
```
tradingview_downloader.py → uploadtodb.py → calcompositescore.py
```

**What it does:**
- Downloads technical data from "Technicals M" screener (wgJk2W66)
- Uploads to `stock_data` table
- Calculates composite scores and updates `stock_rankings` table

**Columns:** 49 technical indicators + performance metrics

### 2. Fundamental Data Pipeline
Downloads fundamental metrics from funda screener.

**Flow:**
```
funda_downloader.py → funda_uploadtodb.py
```

**What it does:**
- Downloads fundamental data from "funda" screener (0BmeuiW6)
- Merges with existing `stock_data` table (upsert)

**Columns:** 49 fundamental metrics including:
- Valuation: P/E, P/B, P/S, PEG, EV multiples
- Profitability: ROE, ROA, ROIC, margins
- Growth: EPS growth, Revenue growth (all timeframes)
- Financial Health: Cash, Debt ratios, liquidity ratios
- Ownership: Shares outstanding, Float %

## Server Deployment & Cron Scheduling

### Prerequisites
1. Python 3.8+
2. Chrome/Chromium installed
3. Xvfb (for headless operation on Linux)
4. Supabase credentials in `/root/.env`

### Setup on Server

```bash
# 1. Transfer files to server
scp -r tradingview_pipeline/ root@YOUR_SERVER:/root/

# 2. Install dependencies
cd /root/tradingview_pipeline
pip install -r requirements.txt

# 3. Install Xvfb (if not installed)
apt-get update && apt-get install -y xvfb

# 4. Test scripts
python3 tradingview_downloader.py
python3 funda_downloader.py
```

### Cron Schedule

Add to crontab (`crontab -e`):

```bash
# Technical data: Daily at 6 PM IST (after market close)
0 18 * * 1-5 cd /root/tradingview_pipeline && /usr/bin/python3 tradingview_downloader.py >> /root/logs/technical_pipeline.log 2>&1

# Fundamental data: Weekly on Sunday at 8 PM IST
0 20 * * 0 cd /root/tradingview_pipeline && /usr/bin/python3 funda_downloader.py >> /root/logs/funda_pipeline.log 2>&1
```

**Notes:**
- Technical data runs Mon-Fri (daily market data)
- Fundamental data runs weekly (less frequent updates)
- `tradingview_downloader.py` automatically calls `uploadtodb.py` and `calcompositescore.py`
- `funda_downloader.py` automatically calls `funda_uploadtodb.py`

### Create Log Directory

```bash
mkdir -p /root/logs
```

### Manual Runs

```bash
# Technical pipeline (full chain)
python3 tradingview_downloader.py

# Fundamentals pipeline
python3 funda_downloader.py
```

## Database Schema

### stock_data Table
Combined technical + fundamental data (1,384+ stocks)

**Key Fields:**
- Technical: RSI, MACD, Momentum, Moving Averages, Bollinger Bands
- Fundamental: All profitability, valuation, growth, and financial health metrics
- Ownership: shares_outstanding, float_percent
- Cash: cash_and_equivalents_annual, net_income_ttm

### stock_rankings Table
Composite scores calculated from stock_data

**Fields:**
- composite_score (normalized 0-100)
- market_cap_category
- sector scores

## Data Quality

**Coverage:**
- 1,384 stocks from NSE
- 86-99% data completeness on fundamental metrics
- Missing data normal for loss-making/recent IPOs

**Decimal Precision:**
- All numeric values: 2 decimal places (e.g., 9.80%, not 9.800000)
- Large values supported (up to 15 digits)

## Troubleshooting

**Download timeout:**
- Check cookies.json is valid
- TradingView may require re-login

**Upload errors:**
- Check Supabase credentials in .env
- Verify network connectivity

**Schema errors:**
- Run migrations if adding new columns
