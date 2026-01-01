# Discord Notifications Integration Guide

**Developer Guide** - How to add Discord webhook notifications to your automated scripts and pipelines.

---

## Quick Start

### 1. Create a Discord Webhook

**In your Discord server:**
1. Go to `Server Settings` → `Webhooks`
2. Click `Create Webhook`
3. Name it (e.g., "Pipeline Alerts")
4. Copy the webhook URL
5. Use the format: `https://discord.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}`

**Keep this URL secret!** Store in environment or script.

---

## Integration Patterns

### Pattern A: Bash Scripts (Recommended for Cron)

**Location:** `run_technical.sh`, `run_fundamentals.sh`

**How it works:**

```bash
#!/bin/bash

# 1. Define webhook URL
WEBHOOK_URL="https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN"

# 2. Run your script
OUTPUT=$(/path/to/script.py 2>&1)
EXIT_CODE=$?

# 3. Extract key metrics from logs
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
RECORDS=$(grep -o "Total records: [0-9]*" /path/to/script.log | tail -1)
SUCCESS=$(grep -o "Successful: [0-9]*" /path/to/upload.log | tail -1)

# 4. Build message based on exit code
if [ $EXIT_CODE -eq 0 ]; then
  MESSAGE="✅ **Pipeline Success** ($TIMESTAMP) | $RECORDS | $SUCCESS | Status: Completed"
else
  MESSAGE="❌ **Pipeline Failed** ($TIMESTAMP) | Exit Code: $EXIT_CODE | Check logs"
fi

# 5. Send to Discord
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  --data-raw "{\"content\":\"$MESSAGE\"}" 2>/dev/null
```

**Key Points:**
- Keep message single-line (avoids JSON escaping issues)
- Use `--data-raw` instead of `-d` for safer JSON
- Extract metrics from log files (grep)
- Check exit codes to determine success/failure

---

### Pattern B: Python Scripts

**How to add to your Python script:**

```python
import subprocess
import json
from datetime import datetime

def send_discord_notification(webhook_url, status, records=None, errors=None):
    """Send notification to Discord webhook"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if status == 'success':
        message = f"✅ **Script Success** ({timestamp}) | Records: {records} | Status: Completed"
    else:
        message = f"❌ **Script Failed** ({timestamp}) | Errors: {errors}"

    payload = {
        "content": message
    }

    try:
        result = subprocess.run(
            ['curl', '-X', 'POST', webhook_url,
             '-H', 'Content-Type: application/json',
             '--data-raw', json.dumps(payload)],
            capture_output=True,
            timeout=10
        )
        return result.returncode == 0
    except Exception as e:
        logging.error(f"Discord notification failed: {e}")
        return False

# In your main script:
if __name__ == "__main__":
    try:
        # Your script logic here
        records = run_script()
        send_discord_notification(
            webhook_url="YOUR_WEBHOOK_URL",
            status='success',
            records=records
        )
    except Exception as e:
        send_discord_notification(
            webhook_url="YOUR_WEBHOOK_URL",
            status='failure',
            errors=str(e)
        )
```

---

## Current Implementation (This Project)

### Technical Pipeline (`run_technical.sh`)

**Flow:**
```
1. tradingview_downloader.py
   ├─ Calls: uploadtodb.py (automatic subroutine)
   └─ Calls: calcompositescore.py (automatic subroutine)
2. Extract metrics from logs
3. Check exit code
4. Send Discord notification
```

**Example Success Message:**
```
✅ **Technical Pipeline Success** (2026-01-01 13:04:00) | Total records: 1384 | Successful: 1383 | Status: Completed
```

**Example Failure Message:**
```
❌ **Technical Pipeline Failed** (2026-01-01 13:05:00) | Exit Code: 1 | Check logs
```

---

### Fundamentals Pipeline (`run_fundamentals.sh`)

**Flow:**
```
1. funda_downloader.py
   ├─ Calls: funda_uploadtodb.py (automatic subroutine)
   └─ Calls: calfundamentalscore.py (automatic subroutine)
2. Extract metrics from logs
3. Check exit code
4. Send Discord notification
```

**Metrics Extracted:**
- Total records downloaded
- Successful uploads
- Timestamps
- Error details (if any)

---

## Adding Notifications to Your Scripts

### Step 1: Get Webhook URL

```bash
# Add to your script or environment
WEBHOOK_URL="https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN"
```

### Step 2: Add to Bash Script (Template)

```bash
#!/bin/bash

WEBHOOK_URL="YOUR_WEBHOOK_URL"
SCRIPT_NAME="Your Script Name"

# Run your command
COMMAND_OUTPUT=$(your_command 2>&1)
EXIT_CODE=$?

# Send notification
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
if [ $EXIT_CODE -eq 0 ]; then
  MESSAGE="✅ **$SCRIPT_NAME Success** ($TIMESTAMP) | Status: OK"
else
  MESSAGE="❌ **$SCRIPT_NAME Failed** ($TIMESTAMP) | Exit Code: $EXIT_CODE"
fi

curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  --data-raw "{\"content\":\"$MESSAGE\"}" 2>/dev/null
```

### Step 3: Extract Custom Metrics

**From log files:**
```bash
# Extract specific values using grep
TOTAL=$(grep -o "Total records: [0-9]*" /path/to/file.log | tail -1)
ERRORS=$(grep -o "Errors: [0-9]*" /path/to/file.log | tail -1)

# Use in message
MESSAGE="✅ **Pipeline** | $TOTAL | $ERRORS | Status: Completed"
```

**From script output:**
```bash
# Capture output and parse
OUTPUT=$(/path/to/script.py 2>&1)
RESULT=$(echo "$OUTPUT" | grep "Total:" | awk '{print $NF}')

MESSAGE="✅ **Pipeline** | $RESULT"
```

---

## Message Format Best Practices

### Do's ✅
- Keep messages on single line (easier JSON escaping)
- Use emojis for status (✅ ❌ ⚠️ ℹ️)
- Include timestamp
- Use pipes `|` as separators
- Include key metrics (records, errors, duration)
- Add actionable info (log location, error code)

### Don'ts ❌
- Avoid newlines in JSON (use pipes instead)
- Don't send huge logs (use Discord link instead)
- Don't include secrets/passwords
- Don't send duplicate messages
- Avoid special characters without escaping

---

## Examples

### Example 1: Simple Success

```bash
MESSAGE="✅ **Daily Export** ($(date '+%H:%M')) | 1000 records | Status: Completed"
curl -X POST "$WEBHOOK_URL" -H "Content-Type: application/json" \
  --data-raw "{\"content\":\"$MESSAGE\"}" 2>/dev/null
```

### Example 2: With Error Details

```bash
if [ $EXIT_CODE -ne 0 ]; then
  ERROR_MSG=$(tail -1 /path/to/error.log)
  MESSAGE="❌ **Data Import** | Error: $ERROR_MSG | Check: /mnt/volume_blr1_01/logs/"
  curl -X POST "$WEBHOOK_URL" -H "Content-Type: application/json" \
    --data-raw "{\"content\":\"$MESSAGE\"}" 2>/dev/null
fi
```

### Example 3: With Duration

```bash
START=$(date +%s)
your_script.py
END=$(date +%s)
DURATION=$((END - START))

MESSAGE="✅ **Pipeline** | Duration: ${DURATION}s | Records: 1384 | Status: OK"
curl -X POST "$WEBHOOK_URL" -H "Content-Type: application/json" \
  --data-raw "{\"content\":\"$MESSAGE\"}" 2>/dev/null
```

### Example 4: Python with Context

```python
import requests
from datetime import datetime

def notify_discord(webhook_url, status, **kwargs):
    timestamp = datetime.now().strftime('%H:%M:%S')
    emoji = "✅" if status == "success" else "❌"

    # Build message from kwargs
    parts = [f"{emoji} **{kwargs.get('title', 'Task')}** ({timestamp})"]
    for key, value in kwargs.items():
        if key != 'title':
            parts.append(f"{key}: {value}")

    message = " | ".join(parts)

    try:
        requests.post(webhook_url, json={"content": message}, timeout=5)
    except Exception as e:
        print(f"Discord notification failed: {e}")

# Usage:
notify_discord(
    webhook_url="YOUR_URL",
    status="success",
    title="Data Pipeline",
    records="1384",
    errors="0",
    duration="3m 15s"
)
```

---

## Troubleshooting

### "Invalid JSON" Error

**Problem:** Message contains newlines or special characters

**Solution:** Use single-line format with pipe separators
```bash
# ❌ Wrong (newlines)
MESSAGE="Success
Records: 1384
Errors: 0"

# ✅ Right (single line)
MESSAGE="Success | Records: 1384 | Errors: 0"
```

### Webhook URL Not Working

**Check:**
1. URL is correct and not expired
2. Webhook channel still exists
3. Bot has permissions
4. Using `--data-raw` not `-d` with curl

**Test:**
```bash
curl -X POST "YOUR_WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  --data-raw '{"content":"Test message"}' -v
```

### Characters Not Displaying

**Emojis showing as boxes?**
- Ensure shell uses UTF-8: `export LANG=en_US.UTF-8`
- Test: `echo "✅"`

**Special chars breaking JSON?**
- Use `json.dumps()` in Python
- Use `--data-raw` in curl (not `-d`)
- Avoid unescaped quotes

---

## Environment Variables

**Store webhook URL safely:**

```bash
# In your .env file
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN"

# Load in script
source /path/to/.env
curl -X POST "$DISCORD_WEBHOOK_URL" ...
```

**Or as cron environment:**
```bash
# In crontab
DISCORD_WEBHOOK="https://discord.com/api/webhooks/ID/TOKEN"
0 12 * * * /path/to/script.sh
```

---

## Security Notes ⚠️

1. **Never commit webhook URLs** to git
2. **Use environment variables** or secure vaults
3. **Rotate URLs periodically** if exposed
4. **Don't log webhook URLs** in output
5. **Use HTTPS** (already enforced by Discord)
6. **Test with dummy URL first** before deploying

**In .gitignore:**
```
.env
.env.local
secrets.txt
webhook_url
```

---

## Advanced: Formatted Messages (Embeds)

**Discord supports embeds for richer formatting:**

```bash
WEBHOOK_URL="YOUR_URL"
PAYLOAD=$(cat <<EOF
{
  "embeds": [{
    "title": "Pipeline Execution Report",
    "color": 3066993,
    "fields": [
      {"name": "Status", "value": "✅ Success", "inline": true},
      {"name": "Records", "value": "1384", "inline": true},
      {"name": "Duration", "value": "3m 15s", "inline": true},
      {"name": "Errors", "value": "0", "inline": true},
      {"name": "Timestamp", "value": "$(date)", "inline": false}
    ]
  }]
}
EOF
)

curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  --data-raw "$PAYLOAD"
```

---

## Related Files

- `run_technical.sh` - Technical pipeline with Discord notifications
- `run_fundamentals.sh` - Fundamentals pipeline with Discord notifications
- `tradingview_downloader.py` - Runs uploadtodb.py and calcompositescore.py (automatic)
- `funda_downloader.py` - Runs funda_uploadtodb.py and calfundamentalscore.py (automatic)

---

## Questions?

**Check the logs:**
```bash
tail -f /mnt/volume_blr1_01/logs/technical_pipeline.log
tail -f /mnt/volume_blr1_01/logs/funda_pipeline.log
tail -f /mnt/volume_blr1_01/tradingview_pipeline/tradingview_downloader.log
tail -f /mnt/volume_blr1_01/tradingview_pipeline/funda_downloader.log
```

**Test webhook directly:**
```bash
curl -X POST "YOUR_WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  --data-raw '{"content":"Test from $(hostname) - $(date)"}' -v
```

---

**Last Updated:** 2026-01-01
**Author:** Claude Code
**Version:** 1.0
