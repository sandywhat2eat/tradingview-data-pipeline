#!/bin/bash
# Fundamental Data Pipeline - Weekly (Sunday at 8 PM IST)

WEBHOOK_URL="https://discord.com/api/webhooks/1456182405982453824/j2_lZZ-ywLRl-7ZiPMu8N0ZMAAP2pGBG0b4UD1gYAdstXi9ykVqY2PsKySwOjd3qZMTT"
SCRIPT_DIR="/mnt/volume_blr1_01/tradingview_pipeline"
LOG_FILE="/mnt/volume_blr1_01/logs/funda_pipeline.log"

# Activate venv and run script
source /mnt/volume_blr1_01/venv/bin/activate
cd $SCRIPT_DIR

# Run the script (automatically calls funda_uploadtodb.py and calfundamentalscore.py)
OUTPUT=$(/mnt/volume_blr1_01/venv/bin/python3 funda_downloader.py 2>&1)
EXIT_CODE=$?

# Extract key info from logs
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
RECORDS=$(grep -o "Total records: [0-9]*" /mnt/volume_blr1_01/tradingview_pipeline/funda_downloader.log | tail -1)
UPLOADED=$(grep -o "Successful: [0-9]*" /mnt/volume_blr1_01/tradingview_pipeline/funda_uploadtodb.log | tail -1)

# Send Discord notification
if [ $EXIT_CODE -eq 0 ]; then
  MESSAGE="✅ **Fundamentals Pipeline Success** ($TIMESTAMP) | $RECORDS | $UPLOADED | Status: Completed"

  curl -X POST "$WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    --data-raw "{\"content\":\"$MESSAGE\"}" 2>/dev/null
else
  MESSAGE="❌ **Fundamentals Pipeline Failed** ($TIMESTAMP) | Exit Code: $EXIT_CODE | Check logs for details"

  curl -X POST "$WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    --data-raw "{\"content\":\"$MESSAGE\"}" 2>/dev/null
fi
