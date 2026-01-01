#!/usr/bin/env python3
"""
TradingView News Flow Scraper
- Scrapes India stock news headlines from TradingView News Flow
- Uses cookies.json for authentication
- Stores headlines in Supabase
"""

import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import sys
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import json
from datetime import datetime
import platform

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('newsflow.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables based on platform
if platform.system() == 'Darwin':  # macOS
    load_dotenv('/Users/jaykrish/Documents/digitalocean/.env')
else:  # Server (Linux)
    load_dotenv('/root/.env')

# Supabase configuration
supabase_url = os.getenv('SUPABASE_URL') or "https://aisqbjjpdztnuerniefl.supabase.co"
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')

if not supabase_key:
    logging.error("SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY not found in environment variables")
    sys.exit(1)

supabase: Client = create_client(supabase_url, supabase_key)
print(f"Using Supabase database: {supabase_url}")

# Configuration
COOKIES_FILE_NAME = "cookies.json"
TARGET_URL = "https://www.tradingview.com/news-flow/j1vPNkYi?market_country=in&market=stock,economic&economic_category=gdp,labor,prices,health,money,trade,government,business,consumer,housing,taxes"


def setup_driver():
    """Sets up the Chrome WebDriver with optimized settings in headless mode"""
    options = webdriver.ChromeOptions()

    # Platform-specific settings
    if platform.system() == 'Linux':
        options.binary_location = "/usr/bin/chromium-browser"
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--remote-debugging-port=9222")
    else:
        # macOS settings
        options.add_argument("--headless=new")

    # Common settings
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        if platform.system() == 'Linux':
            service = Service(executable_path='/usr/bin/chromedriver')
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)

        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logging.info("WebDriver initialized successfully")
        return driver
    except WebDriverException as e:
        logging.error(f"Failed to initialize WebDriver: {e}")
        sys.exit(1)


def load_cookies(driver):
    """Load cookies from cookies.json file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cookies_path = os.path.join(script_dir, COOKIES_FILE_NAME)

    if not os.path.exists(cookies_path):
        logging.warning(f"Cookies file not found at {cookies_path}")
        return False

    try:
        with open(cookies_path, 'r') as f:
            cookies_data = json.load(f)

        # Handle both formats: list of cookies or {"cookies": [...]}
        if isinstance(cookies_data, dict) and 'cookies' in cookies_data:
            cookies_list = cookies_data['cookies']
            logging.info("Extracted cookie list from 'cookies' key in cookies.json.")
        elif isinstance(cookies_data, list):
            cookies_list = cookies_data
            logging.info("Loaded cookie list directly from cookies.json.")
        else:
            logging.error("Unexpected cookies.json format.")
            return False

        # First navigate to base domain to set cookies
        driver.get("https://www.tradingview.com")
        time.sleep(2)

        added_count = 0
        for cookie in cookies_list:
            try:
                # Only include necessary cookie attributes
                cookie_to_add = {
                    'name': cookie['name'],
                    'value': cookie['value'],
                    'domain': cookie.get('domain', '.tradingview.com'),
                    'path': cookie.get('path', '/')
                }
                driver.add_cookie(cookie_to_add)
                added_count += 1
            except Exception as e:
                pass  # Skip problematic cookies silently

        logging.info(f"Added {added_count} cookies out of {len(cookies_list)} total cookies")
        return True
    except Exception as e:
        logging.error(f"Error loading cookies: {e}")
        return False


def scroll_to_load_more(driver, scroll_count=5, scroll_pause=2):
    """Scroll down to load more news items"""
    for i in range(scroll_count):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause)
        logging.info(f"Scroll {i+1}/{scroll_count} completed")


def extract_news_headlines(driver):
    """Extract news headlines from the page using JavaScript"""

    js_code = """
    const articles = document.querySelectorAll('article');
    const news = [];
    const providers = ['Reuters', 'Moneycontrol', 'CNBC TV18', 'Bloomberg', 'Economic Times', 'PTI', 'ANI'];

    articles.forEach(article => {
        try {
            // Get the parent link for the URL
            const link = article.closest('a');
            const url = link ? link.getAttribute('href') : '';

            // Get timestamp from title attribute
            const timeEl = article.querySelector('div[title*="GMT"]');
            const timestamp = timeEl ? timeEl.getAttribute('title') : '';

            // Find provider by looking for exact match divs
            let provider = '';
            const allDivs = article.querySelectorAll('div');
            for (const div of allDivs) {
                const text = div.textContent.trim();
                if (providers.includes(text)) {
                    provider = text;
                    break;
                }
            }

            // Check if it's a premium/locked article
            const isPremium = article.textContent.includes('Sign in to read exclusive');

            // Skip premium articles
            if (isPremium) {
                return;
            }

            // Get headline - find the div that contains only the headline text
            // The headline is typically the last substantial text div that's not provider/time
            let headline = '';
            for (const div of allDivs) {
                const text = div.textContent.trim();
                const children = div.children.length;

                // Look for leaf divs (no children or only text nodes) with substantial text
                if (children === 0 &&
                    text.length > 30 &&
                    !providers.includes(text) &&
                    !text.includes('GMT') &&
                    !text.includes('Sign in') &&
                    !text.includes('Less than')) {
                    headline = text;
                }
            }

            // Clean up headline - remove provider prefix if present
            for (const p of providers) {
                if (headline.startsWith(p)) {
                    headline = headline.substring(p.length).trim();
                }
            }

            if (headline && url) {
                news.push({
                    headline: headline,
                    provider: provider,
                    timestamp: timestamp,
                    url: 'https://in.tradingview.com' + url,
                    is_premium: false
                });
            }
        } catch (e) {
            console.error('Error extracting article:', e);
        }
    });

    return news;
    """

    try:
        news_data = driver.execute_script(js_code)
        logging.info(f"Extracted {len(news_data)} news items")
        return news_data
    except Exception as e:
        logging.error(f"Error extracting news: {e}")
        return []


def get_existing_urls():
    """Fetch existing article URLs from database to avoid duplicates (with pagination)"""
    try:
        all_urls = set()
        page_size = 1000
        offset = 0

        while True:
            response = supabase.table('twitter_posted_tweets') \
                .select('article_url') \
                .range(offset, offset + page_size - 1) \
                .execute()

            if not response.data:
                break

            for row in response.data:
                if row.get('article_url'):
                    all_urls.add(row['article_url'])

            if len(response.data) < page_size:
                break

            offset += page_size

        logging.info(f"Found {len(all_urls)} existing URLs in database")
        return all_urls
    except Exception as e:
        logging.error(f"Error fetching existing URLs: {e}")
        return set()


def save_to_supabase(news_data):
    """Save news headlines to Supabase with incremental loading (skip existing URLs)"""
    if not news_data:
        logging.warning("No news data to save")
        return False

    # Get existing URLs to avoid duplicates
    existing_urls = get_existing_urls()

    # Prepare records - only new ones
    records = []
    skipped = 0
    for item in news_data:
        url = item.get('url', '')

        # Skip if URL already exists in database
        if url in existing_urls:
            skipped += 1
            continue

        if item.get('headline'):
            # Generate unique tweet_id from URL hash
            import hashlib
            tweet_id = hashlib.md5(url.encode()).hexdigest()[:20]

            record = {
                'tweet_id': f"tv_{tweet_id}",
                'article_title': item['headline'][:500],
                'article_description': item['headline'][:500],
                'article_url': url,
                'username': item.get('provider', 'TradingView'),
                'posted_at': datetime.now().isoformat(),
                'is_critical': False
            }
            records.append(record)

    logging.info(f"Skipped {skipped} existing records, {len(records)} new records to insert")

    if not records:
        logging.info("No new records to insert - all headlines already exist")
        return True

    try:
        # Upsert into twitter_posted_tweets table (handles duplicates gracefully)
        response = supabase.table('twitter_posted_tweets').upsert(
            records,
            on_conflict='tweet_id'
        ).execute()
        logging.info(f"Successfully saved {len(records)} new news headlines to Supabase")
        return True
    except Exception as e:
        logging.error(f"Error saving to Supabase: {e}")
        return False


def main():
    """Main execution function"""
    logging.info("Starting TradingView News Flow extraction...")

    driver = setup_driver()

    try:
        # Load cookies first
        logging.info("Navigating to base domain to set cookies.")
        load_cookies(driver)

        # Navigate to news flow page
        logging.info(f"Navigating to target URL: {TARGET_URL}")
        driver.get(TARGET_URL)
        time.sleep(5)

        # Wait for page to load
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
            logging.info("Successfully loaded news flow page")
        except TimeoutException:
            logging.warning("Timeout waiting for articles, attempting to continue...")

        # Take initial screenshot
        screenshot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tradingview_newsflow.png')
        driver.save_screenshot(screenshot_path)
        logging.info(f"Screenshot saved to {screenshot_path}")

        # Scroll to load more news
        scroll_to_load_more(driver, scroll_count=3, scroll_pause=2)

        # Extract headlines
        news_data = extract_news_headlines(driver)

        if news_data:
            # Create DataFrame for display
            df = pd.DataFrame(news_data)
            logging.info(f"Extracted {len(df)} news headlines")

            # Display sample
            print("\n" + "="*80)
            print("EXTRACTED NEWS HEADLINES")
            print("="*80)
            for i, row in df.head(20).iterrows():
                print(f"\n[{row.get('provider', 'N/A')}] {row.get('timestamp', 'N/A')}")
                print(f"  {row.get('headline', 'N/A')[:100]}...")
            print("\n" + "="*80)

            # Save to Supabase
            save_to_supabase(news_data)

            # Also save to CSV for backup
            csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'news_headlines.csv')
            df.to_csv(csv_path, index=False)
            logging.info(f"Saved to CSV: {csv_path}")
        else:
            logging.error("No news headlines extracted")

    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise
    finally:
        driver.quit()
        logging.info("WebDriver closed")

    logging.info("News flow extraction completed")


if __name__ == "__main__":
    main()
