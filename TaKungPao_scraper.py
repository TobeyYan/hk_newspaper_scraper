#!/usr/bin/env python
# coding: utf-8

import os
import sys
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from typing import Union
import fitz

# Import Azure storage utility (this file remains untouched)
# Ensure your sys.path is correctly configured if 'controllers' is not in the same directory or Python path
# For example, if 'controllers' is a sibling directory to the one containing this script:
# sys.path.append(str(Path(__file__).parent.parent)) 
from controllers.azure_storage import create_azure_storage_client, AzureBlobStorage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("TaKungPao_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL_FORMAT = "http://www.takungpao.com.hk/paper/{date_str}.html"
START_DATE = datetime(2018, 6, 10) 
END_DATE = datetime(2018, 6, 10) 
PUBLISHER_NAME = "TaKungPao"
TEMP_DIR = "temp_downloads"
CHECKPOINT_FILE = "takungpao_checkpoint.txt"

# Create necessary temporary directory
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)


def get_download_urls(date_str: str) -> list[str]:
    """
    Fetches the webpage for a given date from takungpao.com.hk and extracts
    all 'downloadurl' attributes from <img> tags.

    Returns:
        A list of strings, where each string is a download URL (PDF link) found on the page.
        Returns an empty list if no URLs are found or if there's an error fetching the page
        (e.g., 404 for a holiday).
    """
    url = BASE_URL_FORMAT.format(date_str=date_str)
    logger.info(f"Attempting to fetch URL: {url}")

    download_urls = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status() # This will raise HTTPError for 4xx/5xx responses

        soup = BeautifulSoup(response.text, 'html.parser')
        img_tags = soup.find_all('img', downloadurl=True)

        for img_tag in img_tags:
            download_url = img_tag.get('downloadurl')
            if download_url:
                download_urls.append(download_url)

    except requests.exceptions.RequestException as e:
        # Check for 404 specifically to treat as a non-fatal "holiday" scenario
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
            logger.warning(f"Page not found (404) for {date_str}. This might be a holiday. Skipping.")
        else:
            logger.error(f"Error fetching the page {url}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {url}: {e}")

    return download_urls


def download_pdf(pdf_url: str, date_str: str, page_index: int) -> Union[Path, None]:
    """
    Downloads a PDF file from the given URL and saves it to a temporary directory.

    Returns:
        The Path object to the downloaded PDF file, or None if download fails.
    """
    # Renamed temp PDF filename to distinguish from potential output JPG page numbers
    temp_pdf_path = Path(TEMP_DIR) / f"{date_str}_pdf_part_{page_index}.pdf" 
    logger.info(f"Downloading PDF from: {pdf_url} to {temp_pdf_path}")
    try:
        response = requests.get(pdf_url, stream=True, timeout=30)
        response.raise_for_status()

        with open(temp_pdf_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Successfully downloaded PDF: {temp_pdf_path}")
        return temp_pdf_path
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading PDF from {pdf_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during PDF download: {e}")
        return None


def convert_pdf_and_upload(pdf_path: Path, azure_client: AzureBlobStorage, date: datetime, actual_start_page_num: int) -> list[str]:
    """
    Converts a PDF file's pages to JPG images and uploads them to Azure Blob Storage.
    The actual_start_page_num ensures correct sequential numbering.

    Args:
        pdf_path: Path to the downloaded PDF file.
        azure_client: The initialized AzureBlobStorage client.
        date: The date for which the PDF is being processed.
        actual_start_page_num: The 1-based overall page number that the first page of *this* PDF corresponds to.

    Returns:
        A list of URLs of the successfully uploaded JPG images.
    """
    uploaded_urls = []
    try:
        with fitz.open(pdf_path) as doc:
            logger.info(f"Opened PDF {pdf_path.name} with {doc.page_count} pages.")
            if doc.page_count == 0:
                logger.warning(f"PDF {pdf_path.name} contains no pages. Skipping conversion and upload.")
                return uploaded_urls # Return empty list if PDF has no pages

            for i in range(doc.page_count): # i is internal_page_index within this PDF (0, 1, 2...)
                temp_jpg_name = f"{pdf_path.stem}_page_{i+1}.jpeg"
                temp_jpg_path = Path(TEMP_DIR) / temp_jpg_name

                try:
                    page = doc.load_page(i)
                    zoom = 2.0
                    mat = fitz.Matrix(zoom, zoom)
                    pix = page.get_pixmap(matrix=mat)

                    pix.save(temp_jpg_path, "jpeg")
                    logger.info(f"Successfully converted page {i+1} of {pdf_path.name} to JPG: {temp_jpg_path.name}")

                    with open(temp_jpg_path, 'rb') as f:
                        image_data = f.read()

                    # Calculate the overall 1-based page number for upload
                    page_num_for_upload = actual_start_page_num + i 
                    file_extension = "jpg"

                    uploaded_url = azure_client.upload_image(
                        publisher_name=PUBLISHER_NAME,
                        date=date,
                        page_num=page_num_for_upload,
                        image_data=image_data,
                        file_extension=file_extension
                    )
                    if uploaded_url:
                        uploaded_urls.append(uploaded_url)
                        logger.info(f"Uploaded page {page_num_for_upload} to Azure: {uploaded_url}")
                    else:
                        logger.error(f"Failed to upload page {page_num_for_upload} to Azure.")
                        # Even if one page fails to upload, we continue within this PDF,
                        # but the smaller len(uploaded_urls) will be caught by scrape_date.
                        
                except Exception as convert_e:
                    logger.error(f"Failed to convert or upload page {actual_start_page_num + i} of {pdf_path.name}: {convert_e}")
                    continue # Continue to next page in this PDF
                finally:
                    if temp_jpg_path.exists():
                        os.remove(temp_jpg_path)
                        logger.info(f"Cleaned up temporary JPG: {temp_jpg_path.name}")

        logger.info(f"Finished processing pages from {pdf_path.name}. Uploaded {len(uploaded_urls)} pages.")

    except Exception as e:
        logger.error(f"Error processing PDF {pdf_path.name}: {e}")
    finally:
        if pdf_path.exists():
            os.remove(pdf_path)
            logger.info(f"Cleaned up temporary PDF: {pdf_path.name}")
    return uploaded_urls


def save_checkpoint(date: datetime):
    """Save checkpoint with the current date."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            f.write(date.strftime("%Y-%m-%d"))
        logger.info(f"Checkpoint saved: {date.strftime('%Y-%m-%d')}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")

def load_checkpoint() -> Union[datetime, None]:
    """Load checkpoint date if exists."""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r') as f:
                date_str = f.read().strip()
            # Add 1 day to the loaded checkpoint date to start from the *next* day
            # This is because the checkpoint indicates the LAST SUCCESSFULLY PROCESSED date
            return datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
        return None
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
        return None


def scrape_date(date: datetime, azure_client: AzureBlobStorage) -> bool:
    """
    Scrapes the Ta Kung Pao e-paper for a specific date, downloads PDFs,
    converts them to JPGs, and uploads them to Azure Blob Storage.
    Stops immediately and returns False if any non-holiday related error occurs.

    Args:
        date: The datetime object for the date to scrape.
        azure_client: The initialized AzureBlobStorage client.
    Returns:
        True if all PDFs for the date were successfully processed and uploaded, False otherwise.
    """
    date_str = date.strftime('%Y%m%d')
    logger.info(f"\n--- Processing date: {date_str} ---")

    pdf_urls = get_download_urls(date_str)

    if not pdf_urls:
        logger.info(f"No PDF URLs found for {date_str}. This might be a holiday or page not existing. Skipping this date.")
        return True # Considered successful if no PDFs (e.g., holiday)

    logger.info(f"Found {len(pdf_urls)} PDF URLs for {date_str}.")
    
    for i, pdf_url in enumerate(pdf_urls):
        logger.info(f"Processing PDF {i+1}/{len(pdf_urls)} for {date_str}: {pdf_url}")
        
        temp_pdf_path = download_pdf(pdf_url, date_str, i)
        if temp_pdf_path:
            # Pass the 1-based index (i+1) as the actual starting page number for this PDF
            uploaded_urls = convert_pdf_and_upload(temp_pdf_path, azure_client, date, actual_start_page_num=i + 1)
            
            if uploaded_urls:
                logger.info(f"Successfully processed and uploaded {len(uploaded_urls)} pages from {pdf_url}.")
            else:
                logger.error(f"Failed to convert or upload any images from PDF: {pdf_url}. This indicates a content or conversion issue. Stopping processing for this date.")
                return False # Immediate stop if conversion/upload yields no results from a downloaded PDF
        else:
            logger.error(f"Failed to download PDF from {pdf_url}. This indicates a network or server issue. Stopping processing for this date.")
            return False # Immediate stop if download fails for any PDF

        time.sleep(0.1) # Polite scraping: Wait a bit after each PDF download/conversion/upload
    
    logger.info(f"All PDFs for {date_str} processed successfully.")
    return True # All PDFs for this date were successfully processed


def main():
    logger.info("=== Starting Ta Kung Pao E-Paper Scraper ===")

    azure_client = create_azure_storage_client()
    if not azure_client:
        logger.critical("Failed to initialize Azure Blob Storage client. Cannot proceed. Exiting.")
        sys.exit(1) # Exit with an error code, indicating a critical setup failure

    start_from_date = load_checkpoint()
    if start_from_date:
        logger.info(f"Resuming from checkpoint: {start_from_date.strftime('%Y-%m-%d')}")
    else:
        start_from_date = START_DATE
        logger.info(f"Starting from beginning: {start_from_date.strftime('%Y-%m-%d')}")

    total_dates_to_scrape = (END_DATE - start_from_date).days + 1
    logger.info(f"Will attempt to scrape {total_dates_to_scrape} dates from {start_from_date.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}.")

    current_date = start_from_date
    processed_count = 0
    while current_date <= END_DATE:
        try:
            success = scrape_date(current_date, azure_client)
            if success:
                save_checkpoint(current_date)
            else:
                logger.critical(f"Critical error encountered while processing {current_date.strftime('%Y-%m-%d')}. Stopping program.")
                sys.exit(1) # Hard stop the program on critical failure for a date

            processed_count += 1
            if processed_count % 10 == 0: # Checkpoint and longer break every 10 processed dates
                logger.info(f"Processed {processed_count} dates. Taking a longer break.")
                time.sleep(5)
            else:
                time.sleep(1) # Shorter break between individual dates

        except Exception as e:
            logger.critical(f"An unexpected critical error occurred during scraping for {current_date.strftime('%Y-%m-%d')}: {e}. Stopping program.")
            sys.exit(1) # Hard stop on any unhandled exception during the main loop

        current_date += timedelta(days=1)

    logger.info(f"Scraping completed for dates from {start_from_date.strftime('%Y-%m-%d')} to {current_date.strftime
