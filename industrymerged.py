#!/usr/bin/env python3
"""
TradingView Industry Data Extraction Script - Server Ready Version
- Headless Chrome operation
- Fixed paths for server deployment
- Environment variables from .env file
- Improved error handling and logging
- Server environment optimized
"""
#!/usr/bin/env python3
"""
TradingView Industry Data Extraction Script
Extracts industry data from TradingView, processes it, and stores it in Supabase.
"""

import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import subprocess
import sys
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import json
from datetime import datetime
import re
import platform

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('industrymerged.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables based on platform
if platform.system() == 'Darwin':  # macOS
    load_dotenv('/Users/jaykrish/Documents/digitalocean/.env')
else:  # Server (Linux)
    load_dotenv('/root/.env')

# Supabase configuration from environment variables
supabase_url = "https://aisqbjjpdztnuerniefl.supabase.co"
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')

if not supabase_key:
    logging.error("SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY not found in environment variables")
    sys.exit(1)

supabase: Client = create_client(supabase_url, supabase_key)

print(f"Using Supabase database: {supabase_url}")

# Configuration
COOKIES_FILE_NAME = "cookies.json"
TARGET_URL = "https://in.tradingview.com/markets/stocks-india/sectorandindustry-industry/"

def setup_driver():
    """Sets up the Chrome WebDriver with optimized settings in headless mode"""
    options = webdriver.ChromeOptions()
    # Use Chromium browser instead of Chrome
    options.binary_location = "/usr/bin/chromium-browser"
    options.add_argument("--headless=new")  # Run in headless mode
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--remote-debugging-port=9222")  # Required for headless mode
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Add user agent to appear more like a real browser
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        # Use Service class to specify chromedriver path
        from selenium.webdriver.chrome.service import Service
        service = Service(executable_path='/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logging.info("WebDriver initialized successfully")
        return driver
    except WebDriverException as e:
        logging.error(f"Failed to initialize WebDriver: {e}")
        return None

def clean_data(text):
    """Clean and normalize text data"""
    if not text:
        return ""
    
    # Remove unicode characters and normalize
    replacements = {
        '\u202f': '',
        '\u2013': '-',
        '‚àí': '-',
        '‚Äì': '-',
        '‚Ä¶': '...',
        'â€™': "'",
        'â€œ': '"',
        'â€': '"',
        'â€"': '-',
        'â€': '-',
        'â€…': '...',
        '−': '-',  # Unicode minus sign
        '+': '+',
        '%': '%'
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Remove extra whitespace and normalize
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Remove commas from numbers (but keep decimal points)
    if re.match(r'^[\d,]+\.?\d*\s*[KMBT]?\s*(INR|%)?$', text):
        text = text.replace(',', '')
    
    return text

def load_cookies_and_navigate(driver, url, script_dir, cookies_file_name):
    """Loads cookies from a file and navigates to the URL"""
    cookies_file_path = os.path.join(script_dir, cookies_file_name)
    
    if not os.path.exists(cookies_file_path):
        logging.warning(f"Cookies file not found: {cookies_file_path}. Proceeding without loading cookies.")
        driver.get(url)
        time.sleep(5)
        return

    try:
        # First navigate to the TradingView domain to set cookies
        base_url_domain = "https://www.tradingview.com"
        logging.info(f"Navigating to base domain {base_url_domain} to set cookies.")
        driver.get(base_url_domain) 
        time.sleep(3)

        # Load cookies from file
        with open(cookies_file_path, 'r') as f:
            loaded_json_data = json.load(f)
        
        # Determine the format of the cookies file
        actual_cookies_list = []
        if isinstance(loaded_json_data, list):
            actual_cookies_list = loaded_json_data
            logging.info("cookies.json appears to be a direct list of cookies.")
        elif isinstance(loaded_json_data, dict) and 'cookies' in loaded_json_data:
            actual_cookies_list = loaded_json_data['cookies']
            logging.info("Extracted cookie list from 'cookies' key in cookies.json.")
        else:
            logging.warning("Could not find a list of cookies in cookies.json.")
            
        # Add each cookie to the browser
        cookies_added = 0
        for cookie in actual_cookies_list:
            try:
                if 'name' in cookie and 'value' in cookie:
                    # Clean up cookie data
                    if 'expiry' in cookie and isinstance(cookie['expiry'], float):
                        cookie['expiry'] = int(cookie['expiry'])
                    
                    if 'domain' not in cookie or not cookie['domain']:
                        cookie['domain'] = '.tradingview.com'
                    
                    # Remove problematic fields
                    for field in ['sameSite', 'storeId', 'id']:
                        if field in cookie:
                            del cookie[field]
                    
                    driver.add_cookie(cookie)
                    cookies_added += 1
                    
            except Exception as cookie_error:
                logging.warning(f"Error adding cookie {cookie.get('name', 'unknown')}: {cookie_error}")
        
        logging.info(f"Added {cookies_added} cookies out of {len(actual_cookies_list)} total cookies")
        
        # Navigate to the actual URL after setting cookies
        logging.info(f"Navigating to target URL: {url}")
        driver.get(url)
        time.sleep(8)
        
        # Check if we're on the correct page
        if "industry" in driver.current_url.lower():
            logging.info("Successfully navigated to the industry page")
        else:
            logging.warning(f"Current URL does not appear to be the industry page: {driver.current_url}")
            
    except Exception as e:
        logging.error(f"Error loading cookies: {e}")
        driver.get(url)
        time.sleep(5)

def close_popups(driver):
    """Close any popups that might appear on the page"""
    try:
        # Common popup selectors
        popup_selectors = [
            "[data-dialog-name='popup']",
            ".tv-dialog__close",
            ".js-dialog-close",
            "[aria-label='Close']",
            ".close-button"
        ]
        
        for selector in popup_selectors:
            try:
                popups = driver.find_elements(By.CSS_SELECTOR, selector)
                for popup in popups:
                    if popup.is_displayed():
                        popup.click()
                        logging.info(f"Closed popup with selector: {selector}")
                        time.sleep(1)
            except Exception:
                continue
                
    except Exception as e:
        logging.warning(f"Error handling popups: {e}")

def extract_table_data_with_js(driver, tab_name="overview"):
    """Extract table data using JavaScript for better reliability"""
    
    js_script = """
    function extractTableData() {
        const tables = document.querySelectorAll('table');
        if (tables.length === 0) {
            return {headers: [], data: [], error: 'No tables found'};
        }
        
        // Get the main data table (usually the largest one)
        let targetTable = null;
        let maxRows = 0;
        
        for (let table of tables) {
            const rows = table.querySelectorAll('tbody tr');
            if (rows.length > maxRows) {
                maxRows = rows.length;
                targetTable = table;
            }
        }
        
        if (!targetTable) {
            return {headers: [], data: [], error: 'No suitable table found'};
        }
        
        const headers = [];
        const data = [];
        
        // Extract headers
        const headerCells = targetTable.querySelectorAll('thead tr th');
        for (let cell of headerCells) {
            headers.push(cell.textContent.trim());
        }
        
        // Extract data rows
        const dataRows = targetTable.querySelectorAll('tbody tr');
        for (let row of dataRows) {
            const rowData = {};
            const cells = row.querySelectorAll('td');
            
            // Skip rows that don't have enough cells
            if (cells.length < 2) continue;
            
            let validRow = true;
            for (let i = 0; i < cells.length && i < headers.length; i++) {
                const cellText = cells[i].textContent.trim();
                rowData[headers[i]] = cellText;
                
                // Check if this is a header row or invalid data
                if (i === 0) { // First column should be industry name
                    if (cellText === 'Industry' || 
                        cellText === '' || 
                        cellText.includes('0001-01-01') ||
                        cellText.length > 100 ||
                        cellText.includes('Strengths:') ||
                        cellText.includes('Neutral') ||
                        /^\\d{4}-\\d{2}-\\d{2}/.test(cellText)) {
                        validRow = false;
                        break;
                    }
                }
            }
            
            if (validRow && Object.keys(rowData).length > 0) {
                data.push(rowData);
            }
        }
        
        return {
            headers: headers,
            data: data,
            rowCount: data.length,
            error: null
        };
    }
    
    return extractTableData();
    """
    
    try:
        result = driver.execute_script(js_script)
        logging.info(f"JavaScript extraction for {tab_name}: Found {result.get('rowCount', 0)} rows")
        
        if result.get('error'):
            logging.warning(f"JavaScript extraction error for {tab_name}: {result['error']}")
            return pd.DataFrame()
        
        if result.get('data'):
            df = pd.DataFrame(result['data'])
            logging.info(f"{tab_name.capitalize()} columns: {df.columns.tolist()}")
            
            # Additional validation in Python
            if 'Industry' in df.columns:
                initial_count = len(df)
                # Filter out invalid industry names
                df = df[
                    (df['Industry'].str.len() <= 100) &  # Reasonable industry name length
                    (~df['Industry'].str.contains('Industry', na=False)) &  # Not header
                    (~df['Industry'].str.contains('0001-01-01', na=False)) &  # Not date
                    (~df['Industry'].str.contains('Strengths:', na=False)) &  # Not analysis
                    (~df['Industry'].str.contains('Neutral', na=False)) &  # Not analysis
                    (df['Industry'].str.strip() != '') &  # Not empty
                    (~df['Industry'].str.match(r'^\d{4}-\d{2}-\d{2}', na=False))  # Not date format
                ]
                filtered_count = len(df)
                if filtered_count != initial_count:
                    logging.info(f"Filtered out {initial_count - filtered_count} invalid rows from {tab_name}")
            
            return df
        else:
            logging.warning(f"No data extracted for {tab_name}")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error in JavaScript extraction for {tab_name}: {e}")
        return pd.DataFrame()

def click_load_more_button(driver, max_clicks=10):
    """Click the Load More button to load all available records"""
    clicks_count = 0
    
    # Multiple selectors to try for the Load More button
    load_more_selectors = [
        '//*[@id="js-category-content"]/div[2]/div/div[4]/div[3]/button/span',  # Provided xpath
        '//*[@id="js-category-content"]/div[2]/div/div[4]/div[3]/button',      # Button element
        '//button[contains(text(), "Load More")]',                             # Button with text
        '//span[contains(text(), "Load More")]',                               # Span with text
        '.loadMoreWrapper button',                                             # CSS selector
        '[data-overflow-tooltip-text="Load More"]'                            # Data attribute
    ]
    
    while clicks_count < max_clicks:
        try:
            # Wait a bit for the page to load
            time.sleep(2)
            
            load_more_button = None
            
            # Try different selectors to find the Load More button
            for selector in load_more_selectors:
                try:
                    if selector.startswith('//') or selector.startswith('//*'):
                        # XPath selector
                        load_more_button = driver.find_element(By.XPATH, selector)
                    else:
                        # CSS selector
                        load_more_button = driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if load_more_button and load_more_button.is_displayed():
                        logging.info(f"Found Load More button using selector: {selector}")
                        break
                except:
                    continue
            
            if not load_more_button:
                logging.info("Load More button not found with any selector, assuming all data is loaded")
                break
            
            # Check if the button is visible and clickable
            if load_more_button.is_displayed() and load_more_button.is_enabled():
                # Scroll to the button to ensure it's in view
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", load_more_button)
                time.sleep(1)
                
                # Get current row count before clicking
                current_rows = len(driver.find_elements(By.CSS_SELECTOR, 'table tbody tr'))
                
                # Try clicking the button (try both the button and its parent)
                try:
                    load_more_button.click()
                except:
                    # If clicking the span fails, try clicking the parent button
                    parent_button = load_more_button.find_element(By.XPATH, '..')
                    parent_button.click()
                
                clicks_count += 1
                logging.info(f"Clicked Load More button (click #{clicks_count})")
                
                # Wait for new content to load
                time.sleep(4)
                
                # Check if new rows were added
                new_rows = len(driver.find_elements(By.CSS_SELECTOR, 'table tbody tr'))
                if new_rows <= current_rows:
                    logging.info(f"No new rows added after click #{clicks_count}, stopping")
                    break
                else:
                    logging.info(f"Added {new_rows - current_rows} new rows (total: {new_rows})")
                
            else:
                logging.info("Load More button is not clickable, assuming all data is loaded")
                break
                
        except NoSuchElementException:
            logging.info("Load More button not found, assuming all data is loaded")
            break
        except Exception as e:
            logging.warning(f"Error clicking Load More button: {e}")
            break
    
    if clicks_count >= max_clicks:
        logging.warning(f"Reached maximum Load More clicks ({max_clicks}), stopping")
    
    # Get final row count
    try:
        final_rows = len(driver.find_elements(By.CSS_SELECTOR, 'table tbody tr'))
        logging.info(f"Finished clicking Load More button. Total clicks: {clicks_count}, Final rows: {final_rows}")
    except:
        logging.info(f"Finished clicking Load More button. Total clicks: {clicks_count}")
    
    return clicks_count

def extract_industry_data():
    """Main function to extract industry data from both Overview and Performance tabs"""
    
    driver = setup_driver()
    if not driver:
        logging.error("Failed to initialize WebDriver. Exiting.")
        return None, None
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        # Load cookies and navigate to the page
        load_cookies_and_navigate(driver, TARGET_URL, script_dir, COOKIES_FILE_NAME)
        
        # Take initial screenshot
        screenshot_path = os.path.join(script_dir, 'tradingview_industry_initial.png')
        driver.save_screenshot(screenshot_path)
        logging.info(f"Initial screenshot saved to {screenshot_path}")
        
        # Close any popups
        close_popups(driver)
        
        # Wait for page to load completely
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        # Click Load More button to load all records in Overview tab
        logging.info("Clicking Load More button to load all Overview records...")
        load_more_clicks = click_load_more_button(driver)
        
        # Extract Overview data (after loading all records)
        logging.info("Extracting Overview data...")
        overview_df = extract_table_data_with_js(driver, "overview")
        
        # Click on Performance tab using the provided xpath
        logging.info("Clicking on Performance tab...")
        try:
            # Use the xpath provided: //*[@id="industry"]/span
            performance_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="performance"]'))
            )
            performance_tab.click()
            time.sleep(5)  # Wait for tab content to load
            
            # Click Load More button to load all records in Performance tab
            logging.info("Clicking Load More button to load all Performance records...")
            perf_load_more_clicks = click_load_more_button(driver)
            
            # Take screenshot of performance tab
            perf_screenshot_path = os.path.join(script_dir, 'tradingview_industry_performance.png')
            driver.save_screenshot(perf_screenshot_path)
            logging.info(f"Performance tab screenshot saved to {perf_screenshot_path}")
            
            # Extract Performance data
            logging.info("Extracting Performance data...")
            performance_df = extract_table_data_with_js(driver, "performance")
            
        except TimeoutException:
            logging.error("Could not find or click Performance tab")
            performance_df = pd.DataFrame()
        except Exception as e:
            logging.error(f"Error extracting performance data: {e}")
            performance_df = pd.DataFrame()
        
        return overview_df, performance_df
        
    except Exception as e:
        logging.error(f"Error in extract_industry_data: {e}")
        return None, None
        
    finally:
        driver.quit()
        logging.info("WebDriver closed")

def merge_dataframes(overview_df, performance_df):
    """Merge overview and performance dataframes"""
    
    if overview_df.empty and performance_df.empty:
        logging.error("Both dataframes are empty")
        return pd.DataFrame()
    
    if overview_df.empty:
        logging.warning("Overview dataframe is empty, using only performance data")
        return performance_df
    
    if performance_df.empty:
        logging.warning("Performance dataframe is empty, using only overview data")
        return overview_df
    
    # Clean industry names for merging
    overview_df['Industry_Clean'] = overview_df['Industry'].apply(lambda x: clean_data(x) if pd.notna(x) else '')
    performance_df['Industry_Clean'] = performance_df['Industry'].apply(lambda x: clean_data(x) if pd.notna(x) else '')
    
    # Merge on cleaned industry names
    merged_df = pd.merge(
        overview_df, 
        performance_df, 
        on='Industry_Clean', 
        how='outer',
        suffixes=('_overview', '_performance')
    )
    
    # Use the overview industry name as primary, fallback to performance
    merged_df['Industry'] = merged_df['Industry_overview'].fillna(merged_df['Industry_performance'])
    
    # Drop the temporary columns
    merged_df = merged_df.drop(['Industry_Clean', 'Industry_overview', 'Industry_performance'], axis=1)
    
    logging.info(f"Merged dataframe has {len(merged_df)} rows and {len(merged_df.columns)} columns")
    return merged_df

def clean_dataframe(df):
    """Clean and standardize the dataframe"""
    
    if df.empty:
        return df
    
    # Clean all text data
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: clean_data(str(x)) if pd.notna(x) else '')
    
    # Clean column names first (remove unicode characters and normalize)
    cleaned_columns = {}
    for col in df.columns:
        cleaned_col = clean_data(str(col))
        # Additional cleaning for column names
        cleaned_col = cleaned_col.replace('\xa0', ' ')  # Replace non-breaking space
        cleaned_col = cleaned_col.replace('  ', ' ')    # Replace double spaces
        cleaned_col = cleaned_col.strip()               # Remove leading/trailing spaces
        cleaned_columns[col] = cleaned_col
    
    df = df.rename(columns=cleaned_columns)
    
    # Log the cleaned column names for debugging
    logging.info(f"Cleaned column names: {list(df.columns)}")
    
    # Standardize column names (handle various possible formats)
    column_mapping = {
        'Market cap': 'market_cap',
        'Market Cap': 'market_cap',
        'Div yield % (indicated)': 'div_yield_indicated',
        'Div Yield % (indicated)': 'div_yield_indicated',
        'Div yield % (Indicated)': 'div_yield_indicated',
        'Change %': 'change_pct',
        'Change%': 'change_pct',
        'Volume': 'volume',
        'Industries': 'industries',
        'Stocks': 'stocks',
        'Perf % 1W': 'perf_1w',
        'Perf% 1W': 'perf_1w',
        'Perf %1W': 'perf_1w',
        'Perf % 1M': 'perf_1m',
        'Perf% 1M': 'perf_1m',
        'Perf %1M': 'perf_1m',
        'Perf % 3M': 'perf_3m',
        'Perf% 3M': 'perf_3m',
        'Perf %3M': 'perf_3m',
        'Perf % 6M': 'perf_6m',
        'Perf% 6M': 'perf_6m',
        'Perf %6M': 'perf_6m',
        'Perf % YTD': 'perf_ytd',
        'Perf% YTD': 'perf_ytd',
        'Perf %YTD': 'perf_ytd',
        'Perf % 1Y': 'perf_1y',
        'Perf% 1Y': 'perf_1y',
        'Perf %1Y': 'perf_1y',
        'Perf % 5Y': 'perf_5y',
        'Perf% 5Y': 'perf_5y',
        'Perf %5Y': 'perf_5y',
        'Perf % 10Y': 'perf_10y',
        'Perf% 10Y': 'perf_10y',
        'Perf %10Y': 'perf_10y',
        'Perf % All Time': 'perf_all_time',
        'Perf% All Time': 'perf_all_time',
        'Perf %All Time': 'perf_all_time'
    }
    
    # Log current columns before mapping
    logging.info(f"Columns before mapping: {list(df.columns)}")
    
    # Apply column mapping
    df = df.rename(columns=column_mapping)
    
    # Log columns after mapping
    logging.info(f"Columns after mapping: {list(df.columns)}")
    
    # Handle merged change_pct columns (note: industry table has change_x and change_y)
    if 'Change %_overview' in df.columns and 'Change %_performance' in df.columns:
        # Use overview change % as primary, fallback to performance
        df['change_x'] = df['Change %_overview'].fillna(df['Change %_performance'])
        df = df.drop(['Change %_overview', 'Change %_performance'], axis=1)
    elif 'Change %_overview' in df.columns:
        df['change_x'] = df['Change %_overview']
        df = df.drop('Change %_overview', axis=1)
    elif 'Change %_performance' in df.columns:
        df['change_y'] = df['Change %_performance']
        df = df.drop('Change %_performance', axis=1)
    
    # Ensure Industry column exists and is clean
    if 'Industry' in df.columns:
        df['industry'] = df['Industry'].apply(lambda x: clean_data(str(x)) if pd.notna(x) else '')
        df = df.drop('Industry', axis=1)
    
    # Add timestamp
    df['updated_at'] = datetime.now()
    
    return df

def save_to_supabase(df):
    """Save dataframe directly to Supabase database"""
    
    if df.empty:
        logging.error("DataFrame is empty, nothing to save")
        return False
    
    try:
        # Get Supabase schema
        supabase_columns = check_supabase_schema()
        
        # Prepare data for Supabase
        supabase_data = []
        records_processed = 0
        
        for _, row in df.iterrows():
            industry_name = row.get('industry', '')
            
            # Validate the record
            if (not industry_name or 
                len(str(industry_name)) > 100 or 
                'Strengths:' in str(industry_name) or 
                'Neutral' in str(industry_name) or
                '0001-01-01' in str(industry_name)):
                logging.warning(f"Skipping invalid record: {industry_name}")
                continue
                
            supabase_record = {}
            
            # Map DataFrame columns to Supabase columns
            for col in supabase_columns:
                if col in row.index and pd.notna(row[col]):
                    value = row[col]
                    # Convert datetime objects to strings for JSON serialization
                    if col == 'updated_at' and value:
                        supabase_record[col] = value.isoformat() if hasattr(value, 'isoformat') else str(value)
                    else:
                        supabase_record[col] = str(value) if value is not None else None
                else:
                    # Set default values for columns not in DataFrame
                    if col == 'updated_at':
                        supabase_record[col] = datetime.now().isoformat()
                    elif col == 'industry_atm_iv':
                        supabase_record[col] = 0
                    else:
                        supabase_record[col] = None
            
            # Final validation
            if supabase_record.get('industry'):
                supabase_data.append(supabase_record)
                records_processed += 1
            else:
                logging.warning(f"Skipping record with empty industry after mapping")
        
        if not supabase_data:
            logging.error("No valid records to save to Supabase")
            return False
        
        logging.info(f"Prepared {len(supabase_data)} valid records for Supabase")
        
        # Clear existing data and insert new data
        try:
            # Delete all existing records
            supabase.table('industry_data').delete().neq('industry', '').execute()
            logging.info("Cleared existing Supabase industry data")
            
            # Insert new records in batches
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(supabase_data), batch_size):
                batch = supabase_data[i:i + batch_size]
                result = supabase.table('industry_data').insert(batch).execute()
                total_inserted += len(batch)
                logging.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
            
            logging.info(f"Successfully saved {total_inserted} records to Supabase")
            return True
            
        except Exception as supabase_error:
            logging.error(f"Supabase save error: {supabase_error}")
            return False
        
    except Exception as e:
        logging.error(f"Error preparing data for Supabase: {e}")
        return False

def check_supabase_schema():
    """Check and log the Supabase table schema"""
    try:
        # Try to get existing data to understand the schema
        result = supabase.table('industry_data').select('*').limit(1).execute()
        if result.data:
            columns = list(result.data[0].keys()) if result.data else []
            logging.info(f"Supabase industry table columns: {columns}")
            return columns
        else:
            logging.info("Supabase industry table exists but is empty")
            # Return known columns from schema
            return [
                'industry', 'market_cap', 'div_yield_indicated', 'change_x', 'volume', 
                'industries', 'stocks', 'change_y', 'perf_1w', 'perf_1m', 'perf_3m', 'perf_6m', 
                'perf_ytd', 'perf_1y', 'perf_5y', 'perf_10y', 'perf_all_time',
                'normalized_score_3m', 'normalized_score_6m', 'normalized_score_1y',
                'old_macro_call', 'macro_summary', 'short_term_classification', 'short_term_performance',
                'short_term_catalysts', 'long_term_classification', 'long_term_performance',
                'long_term_catalysts', 'upside_risks', 'downside_risks', 
                'quantitative_assessment', 'updated_at', 'macro_rank',
                'overall_rating', 'rationale', 'industry_atm_iv'
            ]
    except Exception as e:
        logging.error(f"Error checking Supabase schema: {e}")
        return []

def main():
    """Main execution function"""
    
    logging.info("Starting TradingView industry data extraction...")
    
    try:
        # Extract data from TradingView
        overview_df, performance_df = extract_industry_data()
        
        if overview_df is None and performance_df is None:
            logging.error("Failed to extract any data")
            return False
        
        # Merge the dataframes
        merged_df = merge_dataframes(overview_df, performance_df)
        
        if merged_df.empty:
            logging.error("No data to process")
            return False
        
        # Clean the dataframe
        cleaned_df = clean_dataframe(merged_df)
        
        # Save directly to Supabase
        if save_to_supabase(cleaned_df):
            logging.info("Data successfully saved to Supabase")
        else:
            logging.error("Failed to save data to Supabase")
            return False
        
        logging.info("Industry data extraction completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 