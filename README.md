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

## Folder Structure & Git Setup

### Current Deployment
The pipeline is deployed on server with space optimization:

```
/root/tradingview_pipeline/             ← SYMLINK (shortcut for easy access)
                    ↓
/mnt/volume_blr1_01/tradingview_pipeline/ ← ACTUAL FILES (main location)
                    ↓
git repo: https://github.com/sandywhat2eat/tradingview-data-pipeline.git
```

**Why symlink?**
- Root disk is limited (24G total, ~76% full)
- `/mnt/volume_blr1_01` has 25G with 48% used (plenty of space)
- Symlink allows access via `/root/tradingview_pipeline` while files live on `/mnt`

### Directory Layout

```
/mnt/volume_blr1_01/
├── tradingview_pipeline/           ← Main project folder
│   ├── .git/                       ← Git repository (connected to GitHub)
│   ├── tradingview_downloader.py   ← Technical data downloader
│   ├── funda_downloader.py         ← Fundamental data downloader
│   ├── uploadtodb.py               ← Upload to Supabase
│   ├── calcompositescore.py        ← Calculate composite scores
│   ├── run_technical.sh            ← Cron wrapper (technical pipeline)
│   ├── run_fundamentals.sh         ← Cron wrapper (fundamentals pipeline)
│   ├── setup_crons.sh              ← Cron installation script
│   ├── tradingview_downloads/      ← Downloaded CSV files
│   ├── cookies.json                ← TradingView session cookies
│   └── logs/                       ← Execution logs (from cron jobs)
└── venv/                           ← Python virtual environment
    └── bin/python3                 ← Python executable (Python 3.12.7)
```

### Working with Git

**From GitHub (making changes on laptop):**

1. Clone the repo:
```bash
git clone https://github.com/sandywhat2eat/tradingview-data-pipeline.git
cd tradingview-data-pipeline
```

2. Make changes (edit files like `tradingview_downloader.py`)

3. Commit and push:
```bash
git add .
git commit -m "Your changes here"
git push origin main
```

**On Server (fetch and deploy changes):**

```bash
# Navigate to the project
cd /mnt/volume_blr1_01/tradingview_pipeline
# OR use symlink
cd /root/tradingview_pipeline

# Check current status
git status

# Fetch latest changes from GitHub
git fetch origin

# View what changed
git diff origin/main

# Update to latest version
git pull origin main

# Then run the updated script
/mnt/volume_blr1_01/venv/bin/python3 tradingview_downloader.py
```

**Quick Reference:**

| Task | Command |
|------|---------|
| Check git status | `git -C /mnt/volume_blr1_01/tradingview_pipeline status` |
| Fetch from GitHub | `git -C /mnt/volume_blr1_01/tradingview_pipeline pull origin main` |
| View git log | `git -C /mnt/volume_blr1_01/tradingview_pipeline log --oneline -5` |
| Check remote URL | `git -C /mnt/volume_blr1_01/tradingview_pipeline remote -v` |

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

**Current Setup (Installed):**

```bash
# Technical data: Mon-Fri at 12:30 IST (7:00 AM UTC) - Morning run
0 7 * * 1-5 /mnt/volume_blr1_01/tradingview_pipeline/run_technical.sh >> /mnt/volume_blr1_01/logs/technical_pipeline.log 2>&1

# Fundamental data: Every Sunday at 2:30 PM UTC (8:00 PM IST) - Weekly run
30 14 * * 0 /mnt/volume_blr1_01/tradingview_pipeline/run_fundamentals.sh >> /mnt/volume_blr1_01/logs/funda_pipeline.log 2>&1
```

**To add manually:**

```bash
crontab -e

# Then add the above lines
```

**To view current crons:**

```bash
crontab -l
```

**Notes:**
- Technical data runs Mon-Fri at 12:30 IST (7:00 AM UTC) - uses wrapper script
- Fundamental data runs Sunday at 8:00 PM IST (2:30 PM UTC) - uses wrapper script
- `run_technical.sh` and `run_fundamentals.sh` automatically activate venv and run scripts
- `tradingview_downloader.py` automatically calls `uploadtodb.py` and `calcompositescore.py`
- `funda_downloader.py` automatically calls `funda_uploadtodb.py`
- Logs saved to `/mnt/volume_blr1_01/logs/` (accessible via `/root/tradingview_pipeline/logs/`)

### Wrapper Scripts

**run_technical.sh:**
```bash
#!/bin/bash
source /mnt/volume_blr1_01/venv/bin/activate
cd /mnt/volume_blr1_01/tradingview_pipeline
/mnt/volume_blr1_01/venv/bin/python3 tradingview_downloader.py
```

**run_fundamentals.sh:**
```bash
#!/bin/bash
source /mnt/volume_blr1_01/venv/bin/activate
cd /mnt/volume_blr1_01/tradingview_pipeline
/mnt/volume_blr1_01/venv/bin/python3 funda_downloader.py
```

### Manual Runs

```bash
# Using venv directly
/mnt/volume_blr1_01/venv/bin/python3 /mnt/volume_blr1_01/tradingview_pipeline/tradingview_downloader.py

# OR using wrapper script
/mnt/volume_blr1_01/tradingview_pipeline/run_technical.sh

# Fundamentals pipeline
/mnt/volume_blr1_01/venv/bin/python3 /mnt/volume_blr1_01/tradingview_pipeline/funda_downloader.py

# OR using wrapper script
/mnt/volume_blr1_01/tradingview_pipeline/run_fundamentals.sh
```

### View Logs

```bash
# Technical pipeline logs
tail -f /mnt/volume_blr1_01/logs/technical_pipeline.log

# Fundamentals pipeline logs
tail -f /mnt/volume_blr1_01/logs/funda_pipeline.log

# Detailed script logs
tail -f /mnt/volume_blr1_01/tradingview_pipeline/tradingview_downloader.log
tail -f /mnt/volume_blr1_01/tradingview_pipeline/funda_downloader.log
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
