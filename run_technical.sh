#!/bin/bash
# Technical Data Pipeline - Daily (Mon-Fri at 12:30 IST)

WEBHOOK_URL="https://discord.com/api/webhooks/1456182405982453824/j2_lZZ-ywLRl-7ZiPMu8N0ZMAAP2pGBG0b4UD1gYAdstXi9ykVqY2PsKySwOjd3qZMTT"
SCRIPT_DIR="/mnt/volume_blr1_01/tradingview_pipeline"
LOG_FILE="/mnt/volume_blr1_01/logs/technical_pipeline.log"

# Activate venv and run script
source /mnt/volume_blr1_01/venv/bin/activate
cd $SCRIPT_DIR

# Run the script and capture output
OUTPUT=$(/mnt/volume_blr1_01/venv/bin/python3 tradingview_downloader.py 2>&1)
EXIT_CODE=$?

# Extract key info from logs
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
RECORDS=$(grep -o "Total records: [0-9]*" /mnt/volume_blr1_01/tradingview_pipeline/tradingview_downloader.log | tail -1)
UPLOADED=$(grep -o "Successful: [0-9]*" /mnt/volume_blr1_01/tradingview_pipeline/uploadtodb.log | tail -1)

# Send Discord notification
if [ $EXIT_CODE -eq 0 ]; then
  MESSAGE="✅ **Technical Pipeline Success** ($TIMESTAMP)

$RECORDS
$UPLOADED
Status: All steps completed"

  curl -X POST "$WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    -d "{\"content\":\"$MESSAGE\"}" 2>/dev/null
else
  MESSAGE="❌ **Technical Pipeline Failed** ($TIMESTAMP)

Exit Code: $EXIT_CODE
Check logs: /mnt/volume_blr1_01/logs/technical_pipeline.log"

  curl -X POST "$WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    -d "{\"content\":\"$MESSAGE\"}" 2>/dev/null
fi
