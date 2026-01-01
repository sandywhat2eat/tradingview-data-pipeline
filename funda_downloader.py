#!/usr/bin/env python3
"""
TradingView Fundamentals Data Downloader
Downloads fundamental data from funda screener
"""

import os
import time
import json
import logging
import subprocess
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# --- Configuration ---
TRADINGVIEW_URL = "https://www.tradingview.com/screener/0BmeuiW6/"  # funda screener
COOKIES_FILE_NAME = "cookies.json"
DOWNLOAD_DIR_NAME = "tradingview_downloads"
DOWNLOAD_TIMEOUT_SECONDS = 120

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'funda_downloader.log')),
        logging.StreamHandler()
    ]
)

def setup_virtual_display():
    """Setup virtual display for headless operation"""
    try:
        os.environ['DISPLAY'] = ':99'
        result = subprocess.run(['pgrep', 'Xvfb'], capture_output=True, text=True)
        if result.returncode != 0:
            subprocess.Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)
            logging.info("Virtual display started")
        else:
            logging.info("Virtual display already running")
        return True
    except Exception as e:
        logging.warning(f"Virtual display setup failed: {e}")
        return False

def setup_driver(download_abs_path):
    """Sets up the Chrome WebDriver"""
    setup_virtual_display()

    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_abs_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        logging.info(f"WebDriver initialized. Downloads: {download_abs_path}")
        return driver
    except WebDriverException as e:
        logging.error(f"Failed to initialize WebDriver: {e}")
        return None

def load_cookies_and_navigate(driver, url, base_script_path, cookies_file_name):
    """Load cookies and navigate to URL"""
    cookies_file_path = os.path.join(base_script_path, cookies_file_name)
    if not os.path.exists(cookies_file_path):
        logging.warning(f"Cookies file not found: {cookies_file_path}")
        driver.get(url)
        time.sleep(3)
        return

    try:
        base_url_domain = "https://www.tradingview.com"
        driver.get(base_url_domain)
        time.sleep(2)

        with open(cookies_file_path, 'r') as f:
            loaded_json_data = json.load(f)

        actual_cookies_list = loaded_json_data.get('cookies', []) if isinstance(loaded_json_data, dict) else loaded_json_data

        added_cookie_count = 0
        for cookie_data in actual_cookies_list:
            if not isinstance(cookie_data, dict):
                continue
            if 'name' not in cookie_data or 'value' not in cookie_data:
                continue

            cookie_to_add = {
                'name': cookie_data['name'],
                'value': cookie_data['value']
            }

            for key in ['path', 'domain', 'secure', 'httpOnly']:
                if key in cookie_data:
                    cookie_to_add[key] = cookie_data[key]

            expiry_value = cookie_data.get('expires') or cookie_data.get('expiry') or cookie_data.get('expirationDate')
            if expiry_value:
                cookie_to_add['expires'] = int(float(expiry_value))

            if 'sameSite' in cookie_data:
                cookie_to_add['sameSite'] = cookie_data['sameSite'] if cookie_data['sameSite'] in ['Strict', 'Lax', 'None'] else 'Lax'

            try:
                driver.add_cookie(cookie_to_add)
                added_cookie_count += 1
            except Exception as cookie_error:
                logging.warning(f"Could not add cookie: {cookie_to_add.get('name')}")

        logging.info(f"Added {added_cookie_count} cookies")
        driver.refresh()
        time.sleep(3)

        logging.info(f"Navigating to: {url}")
        driver.get(url)
        time.sleep(5)

    except Exception as e:
        logging.error(f"Error loading cookies: {e}")
        driver.get(url)
        time.sleep(3)

def click_export_button(driver):
    """Click export button to download CSV"""
    menu_trigger_xpath = "//*[@id='js-screener-container']/div[2]/div/div[1]/div[1]/div[1]/div/h2"

    try:
        logging.info("Clicking menu trigger...")
        menu_trigger = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, menu_trigger_xpath))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", menu_trigger)
        time.sleep(0.5)
        menu_trigger.click()
        logging.info("Menu triggered")
        time.sleep(2)

        # Find Download CSV button
        export_button = None
        for text_variant in ['Download results as CSV', 'Download results', 'CSV']:
            try:
                export_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{text_variant}')]"))
                )
                logging.info(f"Found export button: {text_variant}")
                break
            except TimeoutException:
                continue

        if export_button:
            driver.execute_script("arguments[0].scrollIntoView(true);", export_button)
            time.sleep(0.5)
            export_button.click()
            logging.info("Export button clicked")
            return True
        else:
            logging.error("Could not find export button")
            return False

    except Exception as e:
        logging.error(f"Error during export: {e}")
        return False

def wait_for_download_complete(download_path, timeout_seconds):
    """Wait for CSV download to complete"""
    start_time = time.time()
    initial_files = set(os.listdir(download_path))
    logging.info(f"Waiting for download in '{download_path}'...")

    while time.time() - start_time < timeout_seconds:
        current_files = set(os.listdir(download_path))
        new_files = current_files - initial_files

        csv_files = [f for f in new_files if f.lower().endswith(".csv") and not f.startswith('.')]

        if csv_files:
            latest_csv = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(download_path, f)))
            latest_csv_path = os.path.join(download_path, latest_csv)

            base_name = os.path.splitext(latest_csv)[0]
            is_crdownload = any(f.startswith(base_name) and f.endswith(".crdownload") for f in os.listdir(download_path))

            if not is_crdownload:
                logging.info(f"Detected CSV: {latest_csv}")
                last_size = -1
                stable_start = time.time()
                while time.time() - stable_start < 5:
                    if not os.path.exists(latest_csv_path):
                        break
                    current_size = os.path.getsize(latest_csv_path)
                    if current_size == last_size and current_size > 0:
                        logging.info(f"Download complete: {latest_csv} ({current_size} bytes)")
                        return latest_csv_path
                    last_size = current_size
                    time.sleep(1)
                if last_size > 0:
                    return latest_csv_path

        time.sleep(2)

    logging.error("Download timeout")
    return None

def delete_all_csv_files(download_path):
    """Delete all CSV files in directory"""
    if not os.path.isdir(download_path):
        return 0

    deleted = 0
    for filename in os.listdir(download_path):
        if filename.lower().endswith('.csv'):
            try:
                os.remove(os.path.join(download_path, filename))
                deleted += 1
                logging.info(f"Deleted: {filename}")
            except Exception as e:
                logging.error(f"Failed to delete {filename}: {e}")
    return deleted

def main():
    download_abs_path = os.path.join(SCRIPT_DIR, DOWNLOAD_DIR_NAME)

    if not os.path.exists(download_abs_path):
        os.makedirs(download_abs_path)
        logging.info(f"Created download directory: {download_abs_path}")

    logging.info("Deleting existing CSV files...")
    delete_all_csv_files(download_abs_path)

    driver = setup_driver(download_abs_path)
    if not driver:
        return

    try:
        load_cookies_and_navigate(driver, TRADINGVIEW_URL, SCRIPT_DIR, COOKIES_FILE_NAME)

        if not click_export_button(driver):
            logging.error("Failed to click export button")
            return

        time.sleep(5)
        downloaded_file = wait_for_download_complete(download_abs_path, DOWNLOAD_TIMEOUT_SECONDS)

        if downloaded_file:
            logging.info(f"✓ Downloaded: {os.path.basename(downloaded_file)}")
        else:
            logging.error("Download failed")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        if driver:
            driver.quit()
            logging.info("WebDriver closed")

if __name__ == "__main__":
    main()
