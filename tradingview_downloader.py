#!/usr/bin/env python3
"""
TradingView Data Downloader - Server Ready Version
- Headless Chrome operation  
- Server environment optimized
- Fixed paths for server deployment
- Improved error handling and logging
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
TRADINGVIEW_URL = "https://www.tradingview.com/screener/wgJk2W66/"
COOKIES_FILE_NAME = "cookies.json"  # In the same directory as the script
DOWNLOAD_DIR_NAME = "tradingview_downloads"
DOWNLOAD_TIMEOUT_SECONDS = 120  # Max time to wait for download

# Get current script directory for server deployment
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'tradingview_downloader.log')),
        logging.StreamHandler()
    ]
)

def setup_virtual_display():
    """Setup virtual display for headless operation"""
    try:
        # Set display environment variable
        os.environ['DISPLAY'] = ':99'
        
        # Start virtual display if not already running
        result = subprocess.run(['pgrep', 'Xvfb'], capture_output=True, text=True)
        if result.returncode != 0:
            subprocess.Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'], 
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)  # Wait for display to start
            logging.info("Virtual display started")
        else:
            logging.info("Virtual display already running")
        return True
    except Exception as e:
        logging.warning(f"Virtual display setup failed: {e}")
        return False

# --- Helper Functions ---
def setup_driver(download_abs_path):
    """Sets up the Chrome WebDriver with specified download preferences."""
    
    # Setup virtual display first
    setup_virtual_display()
    
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_abs_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    # Essential headless options for server
    options.add_argument("--headless=new")  # Use new headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")  # Speed up loading
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    
    # Memory and performance optimizations
    options.add_argument("--memory-pressure-off")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-background-networking")
    
    # Security options for server environment
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("--ignore-certificate-errors-spki-list")

    try:
        # Use webdriver-manager to automatically manage ChromeDriver
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)  # 60 second timeout
        logging.info(f"WebDriver initialized. Downloads will be saved to: {download_abs_path}")
        return driver
    except WebDriverException as e:
        logging.error(f"Failed to initialize WebDriver: {e}")
        return None

def load_cookies_and_navigate(driver, url, base_script_path, cookies_file_name):
    """Loads cookies from a file and navigates to the URL."""
    cookies_file_path = os.path.join(base_script_path, cookies_file_name)
    if not os.path.exists(cookies_file_path):
        logging.warning(f"Cookies file not found: {cookies_file_path}. Proceeding without loading cookies.")
        driver.get(url)
        time.sleep(3) # Wait for page to load
        return

    try:
        base_url_domain = "https://www.tradingview.com"
        logging.info(f"Navigating to base domain {base_url_domain} to set cookies.")
        driver.get(base_url_domain) 
        time.sleep(2) # Allow page to settle for cookie context

        with open(cookies_file_path, 'r') as f:
            loaded_json_data = json.load(f)
        
        actual_cookies_list = []
        if isinstance(loaded_json_data, list):
            actual_cookies_list = loaded_json_data
            logging.info("cookies.json appears to be a direct list of cookies.")
        elif isinstance(loaded_json_data, dict) and 'cookies' in loaded_json_data and isinstance(loaded_json_data['cookies'], list):
            actual_cookies_list = loaded_json_data['cookies']
            logging.info("Extracted cookie list from 'cookies' key in cookies.json.")
        else:
            logging.warning("Could not find a list of cookies in cookies.json. The file might be malformed or in an unexpected structure.")
            # Fallback: try to iterate over it directly if it's some other iterable, though unlikely to work
            if hasattr(loaded_json_data, '__iter__') and not isinstance(loaded_json_data, (str, bytes)):
                 actual_cookies_list = list(loaded_json_data) 
            else:
                actual_cookies_list = []

        added_cookie_count = 0
        skipped_cookie_count = 0

        for cookie_data in actual_cookies_list:
            # Ensure cookie_data is a dictionary before proceeding
            if not isinstance(cookie_data, dict):
                logging.info(f"Skipping non-dictionary item in cookies file: {cookie_data}")
                skipped_cookie_count += 1
                continue

            cookie_to_add = {}
            
            if 'name' not in cookie_data or 'value' not in cookie_data:
                logging.info(f"Skipping cookie due to missing name or value. Cookie data: {cookie_data}")
                skipped_cookie_count += 1
                continue
            
            cookie_to_add['name'] = cookie_data['name']
            cookie_to_add['value'] = cookie_data['value']

            # Optional but common keys
            if 'path' in cookie_data: cookie_to_add['path'] = cookie_data['path']
            if 'domain' in cookie_data: cookie_to_add['domain'] = cookie_data['domain']
            if 'secure' in cookie_data: cookie_to_add['secure'] = cookie_data['secure']
            if 'httpOnly' in cookie_data: cookie_to_add['httpOnly'] = cookie_data['httpOnly']
            
            # Handle expiry: Selenium expects 'expires' as an integer (Unix timestamp)
            # cookies.json might use 'expiry' or 'expirationDate' and it might be float
            expiry_value = None
            if 'expires' in cookie_data: # If 'expires' is already the key
                expiry_value = cookie_data['expires']
            elif 'expiry' in cookie_data: # Common alternative key
                expiry_value = cookie_data['expiry']
            elif 'expirationDate' in cookie_data: # Another common alternative
                expiry_value = cookie_data['expirationDate']
            
            if expiry_value is not None:
                if isinstance(expiry_value, float):
                    cookie_to_add['expires'] = int(expiry_value)
                elif isinstance(expiry_value, (int, str)):
                    try:
                        cookie_to_add['expires'] = int(expiry_value)
                    except ValueError:
                        logging.info(f"Could not convert expiry '{expiry_value}' to int for cookie {cookie_to_add['name']}. Cookie data: {cookie_data}")
                else:
                    logging.info(f"Unknown expiry type for cookie {cookie_to_add['name']}: {type(expiry_value)}. Cookie data: {cookie_data}")

            if 'sameSite' in cookie_data:
                if cookie_data['sameSite'] in ['Strict', 'Lax', 'None']:
                    cookie_to_add['sameSite'] = cookie_data['sameSite']
                else:
                    cookie_to_add['sameSite'] = 'Lax' # Default to Lax if invalid
            
            try:
                driver.add_cookie(cookie_to_add)
                added_cookie_count += 1
            except Exception as cookie_error:
                logging.warning(f"Could not add cookie: {cookie_to_add.get('name')}. Error: {cookie_error}. Details: {cookie_to_add}")
                skipped_cookie_count += 1

        if added_cookie_count > 0:
            logging.info(f"Successfully added {added_cookie_count} cookies. {skipped_cookie_count} cookies were skipped.")
        else:
            logging.warning(f"No cookies were successfully added from {cookies_file_path}. {skipped_cookie_count} cookies were skipped. Login may not be active.")
        
        logging.info(f"Refreshing page ({base_url_domain}) to apply cookies before navigating to target URL.")
        driver.refresh()
        time.sleep(3) # Wait for refresh and cookies to settle

        logging.info(f"Navigating to target URL: {url}")
        driver.get(url)
        time.sleep(5) # Wait for page to load fully with cookies
        logging.info(f"Navigation to {url} complete after attempting to load cookies.")

    except Exception as e:
        logging.error(f"Error loading cookies or navigating: {e}", exc_info=True)
        logging.info(f"Attempting to navigate to {url} without cookies as a fallback.")
        driver.get(url)
        time.sleep(3)

def click_export_button(driver):
    """Waits for and clicks the 'Export screen results' button using a two-step process."""
    
    # XPath for the element that opens the menu (e.g., a header or a menu button)
    menu_trigger_xpath = "//*[@id='js-screener-container']/div[2]/div/div[1]/div[1]/div[1]/div/h2"
    
    try:
        # Step 1: Click the element to open/reveal the export menu
        logging.info(f"Attempting to click the menu trigger element with XPath: {menu_trigger_xpath}")
        menu_trigger_element = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, menu_trigger_xpath))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", menu_trigger_element)
        time.sleep(0.5) # Brief pause after scroll
        menu_trigger_element.click()
        logging.info("Menu trigger element clicked successfully.")

        # Wait for the menu to appear and the export item to be clickable
        time.sleep(2) # Give the menu a couple of seconds to animate/load

        # Step 2: Find the 'Download results as CSV' item (previously called 'Export screen results')
        # TradingView changed the button text
        logging.info("Attempting to find and click 'Download results as CSV' button")

        # Try multiple approaches to find the download/export button
        export_button = None

        # Approach 1: Find by text 'Download results as CSV'
        try:
            export_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Download results as CSV')]"))
            )
            logging.info("Found export button using 'Download results as CSV' text")
        except TimeoutException:
            logging.info("Could not find using approach 1, trying approach 2")
            pass

        # Approach 2: Find by partial text 'Download results'
        if export_button is None:
            try:
                export_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Download results')]"))
                )
                logging.info("Found export button using 'Download results' text")
            except TimeoutException:
                logging.info("Could not find using approach 2, trying approach 3")
                pass

        # Approach 3: Find span with download text
        if export_button is None:
            try:
                export_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Download results')]"))
                )
                logging.info("Found export button using span approach")
            except TimeoutException:
                logging.info("Could not find using approach 3, trying approach 4")
                pass

        # Approach 4: Find any element with 'CSV' in menu context
        if export_button is None:
            try:
                export_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'CSV')]"))
                )
                logging.info("Found export button using CSV text")
            except TimeoutException:
                logging.info("Could not find using approach 4, trying approach 5")
                pass

        # Approach 5: Search all menu-like elements for download/CSV text
        if export_button is None:
            try:
                all_elements = driver.find_elements(By.XPATH, "//*")
                for el in all_elements:
                    try:
                        text = el.text.lower() if el.text else ""
                        if 'download' in text and 'csv' in text and el.is_displayed():
                            export_button = el
                            logging.info(f"Found export button via full search: {el.text[:50]}")
                            break
                    except:
                        pass
            except Exception as e:
                logging.info(f"Error in approach 5: {e}")
                pass
        
        # If we found a button, click it
        if export_button:
            # Scroll into view just in case it's needed
            driver.execute_script("arguments[0].scrollIntoView(true);", export_button)
            time.sleep(0.5) # Brief pause after scroll before click
            export_button.click()
            logging.info("'Export screen results' item clicked successfully.")
            return True
        else:
            logging.error("Could not find the 'Export screen results' button using any approach.")
            return False
            
    except TimeoutException as e_timeout:
        logging.error(f"Timeout: Could not find or click an element in the export process. Error: {e_timeout}")
        return False
    except Exception as e:
        logging.error(f"An error occurred during the export click process: {e}", exc_info=True)
        return False

def wait_for_download_complete(download_path, timeout_seconds):
    """Waits for a new CSV download to complete in the specified path."""
    start_time = time.time()
    initial_files = set(os.listdir(download_path))
    logging.info(f"Waiting for download to start in '{download_path}'...")

    while time.time() - start_time < timeout_seconds:
        current_files = set(os.listdir(download_path))
        new_files = current_files - initial_files
        
        # Filter for .csv files, ignore temp/hidden files like .DS_Store or .crdownload parts
        csv_files = [f for f in new_files if f.lower().endswith(".csv") and not f.startswith('.')]
        
        if csv_files:
            # Find the most recently modified CSV file among the new ones
            latest_csv_filename = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(download_path, f)))
            latest_csv_path = os.path.join(download_path, latest_csv_filename)
            
            # Check if a .crdownload file exists for this CSV (indicating it's still downloading)
            base_name_no_ext = os.path.splitext(latest_csv_filename)[0]
            is_crdownload_present = any(
                f.startswith(base_name_no_ext) and f.lower().endswith(".crdownload")
                for f in os.listdir(download_path)
            )

            if not is_crdownload_present:
                logging.info(f"Detected new CSV: {latest_csv_filename}. Checking for stability...")
                last_size = -1
                stable_check_start_time = time.time()
                # Check for size stability for a few seconds
                while time.time() - stable_check_start_time < 5:
                    try:
                        if not os.path.exists(latest_csv_path):
                             logging.warning(f"CSV file {latest_csv_filename} disappeared during size check.")
                             break # break stability check, re-evaluate new files
                        current_size = os.path.getsize(latest_csv_path)
                        if current_size == last_size and current_size > 0:
                            logging.info(f"Download of {latest_csv_filename} complete. Size: {current_size} bytes.")
                            return latest_csv_path
                        last_size = current_size
                    except FileNotFoundError:
                        logging.warning(f"CSV file {latest_csv_filename} disappeared during size check.")
                        break 
                    time.sleep(1) 
                # If stability check finishes and size was positive, assume complete
                if last_size > 0:
                    logging.info(f"Download of {latest_csv_filename} assumed complete by stability check. Size: {last_size} bytes.")
                    return latest_csv_path
            else:
                logging.info(f"CSV {latest_csv_filename} found, but .crdownload associated file is still present. Waiting...")
        
        # Check generally for any .crdownload files if no specific new CSV is yet stable
        # crdownload_files_in_dir = [f for f in os.listdir(download_path) if f.lower().endswith(".crdownload")]
        # if crdownload_files_in_dir and not csv_files:
        #     logging.info(f".crdownload file(s) present: {crdownload_files_in_dir}. Download in progress...")

        time.sleep(2) # Wait before checking directory again

    logging.error("Download timeout or failed to confirm completion.")
    return None

def get_latest_downloaded_file(download_path, extension=".csv"):
    """Gets the most recently modified file with the given extension."""
    if not os.path.isdir(download_path):
        return None
    files = [os.path.join(download_path, f) for f in os.listdir(download_path) if f.lower().endswith(extension)]
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def delete_all_csv_files(download_path):
    """Deletes all CSV files in the specified directory."""
    if not os.path.isdir(download_path):
        logging.warning(f"Cannot delete CSV files: Directory {download_path} does not exist")
        return 0
    
    deleted_count = 0
    for filename in os.listdir(download_path):
        if filename.lower().endswith('.csv'):
            file_path = os.path.join(download_path, filename)
            try:
                os.remove(file_path)
                deleted_count += 1
                logging.info(f"Deleted CSV file: {filename}")
            except Exception as e:
                logging.error(f"Failed to delete {filename}: {e}")
    
    if deleted_count > 0:
        logging.info(f"Successfully deleted {deleted_count} CSV file(s) from {download_path}")
    else:
        logging.info(f"No CSV files found to delete in {download_path}")
    
    return deleted_count

# --- Main Script Logic ---
def main():
    script_dir = SCRIPT_DIR
    download_abs_path = os.path.join(script_dir, DOWNLOAD_DIR_NAME)

    if not os.path.exists(download_abs_path):
        try:
            os.makedirs(download_abs_path)
            logging.info(f"Created download directory: {download_abs_path}")
        except OSError as e:
            logging.error(f"Failed to create download directory {download_abs_path}: {e}")
            return
    
    # Delete all existing CSV files in the download directory before proceeding
    logging.info("Checking for existing CSV files in download directory...")
    delete_all_csv_files(download_abs_path)

    driver = setup_driver(download_abs_path)
    if not driver:
        logging.error("Failed to setup driver")
        return

    try:
        load_cookies_and_navigate(driver, TRADINGVIEW_URL, script_dir, COOKIES_FILE_NAME)
        
        if not click_export_button(driver):
            logging.error("Failed to click the export button. Exiting.")
            return # Exit if click fails

        # Give a brief moment for the download process to initiate fully
        time.sleep(5) 

        downloaded_file_path = wait_for_download_complete(download_abs_path, DOWNLOAD_TIMEOUT_SECONDS)

        if downloaded_file_path:
            logging.info(f"Successfully downloaded: {os.path.basename(downloaded_file_path)}")
            logging.info(f"Full path: {downloaded_file_path}")
        else:
            logging.error("Download failed or could not be confirmed.")
            # As a fallback, try to find the latest CSV if wait_for_download_complete failed but a file might exist
            latest_file = get_latest_downloaded_file(download_abs_path)
            if latest_file:
                logging.info(f"Found latest CSV in download folder (fallback): {os.path.basename(latest_file)}")
            else:
                logging.info("No CSV file found in download folder (fallback).")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if driver:
            try:
                driver.quit()
                logging.info("WebDriver closed successfully")
            except:
                logging.warning("Failed to close WebDriver gracefully")

def run_subsequent_scripts():
    """Run uploadtodb.py and calcompositescore.py in sequence, regardless of success/failure."""
    
    scripts_to_run = [
        'uploadtodb.py',
        'calcompositescore.py'
    ]
    
    for script in scripts_to_run:
        script_path = os.path.join(SCRIPT_DIR, script)
        try:
            logging.info(f"\n{'='*50}")
            logging.info(f"Starting {script}...")
            logging.info(f"{'='*50}")
            
            # Run the script and capture output
            result = subprocess.run(
                [sys.executable, script_path],
                check=False,  # Don't raise exception on non-zero exit code
                capture_output=True,
                text=True,
                cwd=SCRIPT_DIR  # Set working directory
            )
            
            # Log the output
            if result.stdout:
                logging.info(f"{script} output:\n{result.stdout}")
            if result.stderr:
                logging.error(f"{script} errors:\n{result.stderr}")
                
            logging.info(f"{script} completed with return code: {result.returncode}")
            
        except Exception as e:
            logging.error(f"Error running {script}: {str(e)}")
        
        # Add a small delay between script executions
        time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    finally:
        # Always run the subsequent scripts, even if main() fails
        run_subsequent_scripts()
