import os
import sys
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
import requests
import fitz # PyMuPDF library for PDF conversion

# Import Azure storage utility from a parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from controllers.azure_storage import create_azure_storage_client

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("am730_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
START_DATE = datetime(2015, 1, 1) # Changed back to 2015,1,1 to demonstrate quarterly check
END_DATE = datetime(2025, 7, 18)
PUBLISHER_NAME = "am730"
TEMP_DIR = "temp_downloads"

# Create temp directory
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

def is_weekday(date):
    """Checks if a date is a weekday (Monday=0, Sunday=6)."""
    return date.weekday() < 5

def get_date_range(start_date, end_date):
    """Generates a list of weekday dates to scrape."""
    current_date = start_date
    while current_date <= end_date:
        if is_weekday(current_date):
            yield current_date
        current_date += timedelta(days=1)

def upload_to_azure(azure_client, filepath, date, page_num, extension):
    """
    Uploads a file to Azure Blob Storage and handles any errors.
    """
    try:
        logger.info(f"Uploading to Azure: {filepath}")
        with open(filepath, 'rb') as f:
            image_data = f.read()
        
        blob_url = azure_client.upload_image(
            publisher_name=PUBLISHER_NAME,
            date=date,
            page_num=page_num,
            image_data=image_data,
            file_extension=extension
        )
        if blob_url:
            logger.info(f"Successfully uploaded to Azure: {blob_url}")
            return True
        else:
            logger.error("Failed to upload to Azure")
            return False
    except Exception as e:
        logger.error(f"Azure upload error: {e}")
        return False

def download_and_convert_pdf(date, azure_client):
    """
    Downloads each page as a PDF, converts it to a high-quality JPG, and uploads it to Azure.
    Includes page-level existence check for resumption and 429 error handling.
    """
    pages_converted = 0
    date_str = date.strftime("%Y-%m-%d")
    base_pdf_url = f"https://flippingbook.am730.com.hk/daily-news/{date_str}/files/assets/common/downloads/page"
    
    for page_num in range(1, 201): # Assuming max 200 pages per issue
        # Check if the page already exists in Azure Blob Storage
        if azure_client.blob_exists(PUBLISHER_NAME, date, page_num, "jpeg"):
            logger.info(f"Page {page_num:03d} for {date_str} already exists in Azure. Skipping download and conversion.")
            pages_converted += 1 # Count as processed even if skipped
            continue # Move to the next page

        time.sleep(0.1) # Adjusted from 0.5s. Adjust if rate limits hit.
        formatted_page_num = f"{page_num:04d}"
        pdf_url = f"{base_pdf_url}{formatted_page_num}.pdf" # Defined here
        
        temp_pdf_name = f"page_{formatted_page_num}.pdf"
        temp_pdf_path = Path(TEMP_DIR) / temp_pdf_name
        
        temp_jpg_name = f"{page_num}.jpeg"
        temp_jpg_path = Path(TEMP_DIR) / temp_jpg_name
        
        logger.info(f"Attempting to download {pdf_url}")
        
        try:
            response = requests.get(pdf_url, stream=True, timeout=10)
            
            # Handle 429 Too Many Requests
            if response.status_code == 429:
                logger.warning(f"Received 429 Too Many Requests for {pdf_url}. Stopping for this issue to avoid further rate limiting.")
                break # Stop processing this date

            if response.status_code == 200:
                with open(temp_pdf_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Successfully downloaded PDF for page {page_num}.")
                
                try:
                    doc = fitz.open(temp_pdf_path)
                    page = doc.load_page(0)
                    pix = page.get_pixmap(matrix=fitz.Matrix(1, 1)) # Changed from 2,2 to 1,1 for speed
                    pix.save(temp_jpg_path, "jpeg")
                    logger.info(f"Successfully converted page {page_num} to JPG.")
                    
                    # Upload to Azure and clean up local file
                    if upload_to_azure(azure_client, temp_jpg_path, date, page_num, "jpeg"):
                        pages_converted += 1
                    
                except Exception as convert_e:
                    logger.error(f"Error converting page {page_num} to JPG: {convert_e}")
                finally:
                    if temp_pdf_path.exists():
                        os.remove(temp_pdf_path)
                    if temp_jpg_path.exists():
                        os.remove(temp_jpg_path)
                    logger.info(f"Removed temporary files for page {page_num}")

            elif response.status_code in [403, 404]:
                logger.info(f"Page {page_num} not found (Status Code {response.status_code}). Assuming end of issue.")
                break # No more pages for this date
            else:
                logger.warning(f"Failed to download {pdf_url} with status code {response.status_code}. Stopping for this issue.")
                break # Stop processing this date on unexpected error
                
            response.close()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading {pdf_url}: {e}. Stopping for this issue.")
            break # Stop processing this date on network error
            
    return pages_converted

def download_jpg_pages(date, date_format, azure_client):
    """
    Downloads JPG pages directly and uploads them to Azure.
    Includes page-level existence check for resumption and 429 error handling.
    """
    pages_downloaded = 0
    date_str = date.strftime(date_format)
    base_jpg_url = f"https://flippingbook.am730.com.hk/daily-news/{date_str}/files/assets/common/page-html5-substrates/page"
    
    for page_num in range(1, 201): # Assuming max 200 pages per issue
        # Check if the page already exists in Azure Blob Storage
        if azure_client.blob_exists(PUBLISHER_NAME, date, page_num, "jpeg"):
            logger.info(f"Page {page_num:03d} for {date_str} already exists in Azure. Skipping download.")
            pages_downloaded += 1 # Count as processed even if skipped
            continue # Move to the next page

        time.sleep(0.1) # Adjusted from 0.5s. Adjust if rate limits hit.
        formatted_page_num = f"{page_num:04d}"
        jpg_url = f"{base_jpg_url}{formatted_page_num}_3.jpg" # Defined here
        
        temp_jpg_name = f"{page_num}.jpeg"
        temp_jpg_path = Path(TEMP_DIR) / temp_jpg_name
        
        logger.info(f"Attempting to download {jpg_url}")

        try:
            response = requests.get(jpg_url, stream=True, timeout=10)
            
            # Handle 429 Too Many Requests
            if response.status_code == 429:
                logger.warning(f"Received 429 Too Many Requests for {jpg_url}. Stopping for this issue to avoid further rate limiting.")
                break # Stop processing this date

            if response.status_code == 200:
                with open(temp_jpg_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Successfully downloaded page {page_num} as JPEG.")
                
                # Upload to Azure and clean up local file
                if upload_to_azure(azure_client, temp_jpg_path, date, page_num, "jpeg"):
                    pages_downloaded += 1
                
                os.remove(temp_jpg_path)
                logger.info(f"Removed temporary file: {temp_jpg_path}")

            elif response.status_code in [403, 404]:
                logger.info(f"Page {page_num} not found. Assuming end of issue.")
                break # No more pages for this date
            else:
                logger.warning(f"Failed to download {jpg_url} with status code {response.status_code}. Stopping for this issue.")
                break # Stop processing this date on unexpected error

            response.close()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error during download for page {page_num}: {e}. Stopping for this issue.")
            break # Stop processing this date on network error
            
    return pages_downloaded

def scrape_issues_main():
    """Main function to orchestrate the scraping and uploading process."""
    logger.info("=== Starting am730 E-Paper Scraper (Azure Version) ===")
    
    # Initialize Azure storage client
    try:
        azure_client = create_azure_storage_client()
        logger.info("Azure storage client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Azure storage: {e}")
        return

    # --- NEW BLOCK: Find the actual start date using quarterly checks ---
    actual_start_date = START_DATE
    current_quarter_check_date = datetime(START_DATE.year, ((START_DATE.month - 1) // 3) * 3 + 1, 1)

    logger.info(f"Starting quarterly check from {current_quarter_check_date.strftime('%Y-%m-%d')}...")

    while current_quarter_check_date <= END_DATE:
        # Ensure it's a weekday for the check
        check_date = current_quarter_check_date
        while not is_weekday(check_date) and check_date <= END_DATE:
            check_date += timedelta(days=1)
        
        if check_date > END_DATE: # If we've gone past END_DATE looking for a weekday
            break

        # Check if page 1 of this quarter's first weekday exists in Azure
        if azure_client.blob_exists(PUBLISHER_NAME, check_date, 1, "jpeg"):
            logger.info(f"Page 1 for {check_date.strftime('%Y-%m-%d')} (quarterly check) exists. Assuming this quarter is processed.")
            actual_start_date = check_date # Update actual_start_date to this point
            
            # Move to the first day of the next quarter
            if current_quarter_check_date.month == 10:
                current_quarter_check_date = datetime(current_quarter_check_date.year + 1, 1, 1)
            else:
                current_quarter_check_date = datetime(current_quarter_check_date.year, current_quarter_check_date.month + 3, 1)
        else:
            logger.info(f"Page 1 for {check_date.strftime('%Y-%m-%d')} (quarterly check) does NOT exist. Starting detailed scan from the previous known point.")
            break # Found the gap, break out of quarterly loop

    # Adjust actual_start_date to be the day after the last found full date,
    # or the original START_DATE if no quarter was found fully processed.
    # If actual_start_date was updated, we need to move it to the next day to start scraping.
    # If it's still the original START_DATE, we start from there.
    if actual_start_date != START_DATE:
        # Find the first weekday after the last found processed date
        next_day = actual_start_date + timedelta(days=1)
        while not is_weekday(next_day) and next_day <= END_DATE:
            next_day += timedelta(days=1)
        if next_day <= END_DATE:
            actual_start_date = next_day
        else: # All dates processed or no more weekdays
            logger.info("All relevant dates appear to be processed based on quarterly check.")
            return # Exit if all done

    logger.info(f"Actual scraping will start from: {actual_start_date.strftime('%Y-%m-%d')}")
    # --- END NEW BLOCK ---

    # --- MODIFIED LINE: Generate dates from the actual_start_date ---
    dates_to_process = list(get_date_range(actual_start_date, END_DATE))
    logger.info(f"Found {len(dates_to_process)} weekdays to process from {actual_start_date.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    
    # --- MODIFIED LOOP: Iterate over dates_to_process ---
    for i, date in enumerate(dates_to_process):
        date_str = date.strftime('%Y-%m-%d')
        logger.info(f"Processing date {i+1}/{len(dates_to_process)}: {date_str}")
        
        pages_found = 0
        issue_found = False
        
        # Define the URL formats to check, in order of priority (highest quality first)
        formats_to_check = [
            {'type': 'pdf', 'url_format': 'https://flippingbook.am730.com.hk/daily-news/{date}/files/assets/common/downloads/page0001.pdf', 'date_format': '%Y-%m-%d'},
            {'type': 'jpg', 'url_format': 'https://flippingbook.am730.com.hk/daily-news/{date}/files/assets/common/page-html5-substrates/page0001_3.jpg', 'date_format': '%Y-%m-%d'},
            {'type': 'jpg', 'url_format': 'https://flippingbook.am730.com.hk/daily-news/{date}/files/assets/common/page-html5-substrates/page0001_3.jpg', 'date_format': '%d_%m_%Y'},
        ]
        
        # Iterate through formats to find a working one for the current date
        for format_info in formats_to_check:
            date_str_formatted = date.strftime(format_info['date_format'])
            check_url = format_info['url_format'].replace('{date}', date_str_formatted)
            
            logger.info(f"Checking for issue at: {check_url}")
            
            try:
                response = requests.head(check_url, timeout=10)

                # Handle 429 Too Many Requests using check_url
                if response.status_code == 429:
                    logger.warning(f"Received 429 Too Many Requests for {check_url}. Stopping for this issue to avoid further rate limiting.")
                    issue_found = False # Important: Set this to False to prevent attempting download
                    break # Stop trying formats for this date and move to next date

                if response.status_code == 200:
                    logger.info(f"Issue found using {format_info['type']} method.")
                    issue_found = True
                    if format_info['type'] == 'pdf':
                        pages_found = download_and_convert_pdf(date, azure_client)
                    else:
                        pages_found = download_jpg_pages(date, format_info['date_format'], azure_client)
                    break # Found a format and processed, move to next date
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error checking {format_info['type']} URL for {date_str}: {e}")
        
        if not issue_found:
            logger.info(f"No issue found for {date.strftime('%Y-%m-%d')} after checking all formats.")
            pages_found = 0 # No pages processed for this date
            
        logger.info(f"Completed for {date.strftime('%Y-%m-%d')} e-paper: {pages_found} pages processed (including skips).")
        
        if i < len(dates_to_process) - 1: # MODIFIED: Use dates_to_process here
            logger.info(f"Waiting 3 seconds before next issue...")
            time.sleep(3)
            
    logger.info("=== am730 E-Paper Scraper Completed ===")

if __name__ == "__main__":
    scrape_issues_main()
