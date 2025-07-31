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
import fitz # PyMuPDF
import io # For handling in-memory PDF data

# Import Azure storage utility
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
START_DATE = datetime(2022, 7, 8) # This is your desired start date
END_DATE = datetime(2023, 12, 31)
PUBLISHER_NAME = "TaKungPao"
TEMP_PDF_DIR = "temp_downloads"
MISSING_PAGES_LOG = "missing_pages.log" # New file for missing pages

# Create necessary temporary directory
Path(TEMP_PDF_DIR).mkdir(parents=True, exist_ok=True)


def log_missing_page(date: datetime, original_pdf_url: str, expected_azure_page_num: int, reason: str):
    """Logs details of a missing page to a dedicated file."""
    message = f"DATE: {date.strftime('%Y-%m-%d')}, URL: {original_pdf_url}, Expected Azure Page: {expected_azure_page_num}, Reason: {reason}\n"
    with open(MISSING_PAGES_LOG, 'a') as f:
        f.write(message)
    logger.warning(f"Logged missing page: {message.strip()}")


def get_download_urls(date_str: str) -> list[str]:
    """
    Fetches the webpage for a given date from takungpao.com.hk and extracts
    all 'downloadurl' attributes from <img> tags.
    """
    url = BASE_URL_FORMAT.format(date_str=date_str)
    logger.info(f"Attempting to fetch URL: {url}")

    download_urls = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        img_tags = soup.find_all('img', downloadurl=True)

        for img_tag in img_tags:
            download_url = img_tag.get('downloadurl')
            if download_url:
                download_urls.append(download_url)

    except requests.exceptions.RequestException as e:
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
            logger.warning(f"Page not found (404) for {date_str}. This might be a holiday. Skipping.")
        else:
            logger.error(f"Error fetching the page {url}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {url}: {e}")

    return download_urls


# Removed get_pdf_page_count_from_url as all PDFs are assumed to be 1 page.
# def get_pdf_page_count_from_url(pdf_url: str) -> Union[int, None]:
#     """
#     (Removed)
#     """
#     pass


def download_pdf(pdf_url: str, temp_pdf_path: Path) -> Union[Path, None]:
    """
    Downloads a PDF file from the given URL and saves it to a temporary directory.
    Increased timeout to 60 seconds.
    """
    logger.info(f"Downloading PDF from: {pdf_url} to {temp_pdf_path}")
    try:
        response = requests.get(pdf_url, stream=True, timeout=60) # Increased timeout
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


def convert_pdf_and_upload(pdf_path: Path, azure_client: AzureBlobStorage, date: datetime, starting_azure_page_num: int, original_pdf_url: str) -> int:
    """
    Converts a single-page PDF to JPG, uploads it to Azure, and handles cleanup.
    Only uploads if the blob does not already exist.
    Returns 1 if the page was successfully processed (uploaded or already existed), 0 otherwise.
    """
    pages_processed_count = 0
    
    if not pdf_path or not pdf_path.exists():
        logger.error(f"PDF file not found for conversion: {pdf_path}. This should ideally be caught earlier.")
        log_missing_page(date, original_pdf_url, starting_azure_page_num, "PDF file not found locally for conversion.")
        return 0

    try:
        with fitz.open(pdf_path) as doc:
            # Assuming all PDFs have only one page
            if doc.page_count != 1:
                logger.warning(f"PDF {pdf_path.name} was expected to have 1 page but has {doc.page_count}. Processing only the first page as intended.")

            page_num_for_azure_upload = starting_azure_page_num
            file_extension = "jpg" # Output format for Azure

            # This check here is a secondary, page-level check, mostly for robustness
            # in case the pre-check was imperfect or if a blob was deleted manually.
            if azure_client.blob_exists(PUBLISHER_NAME, date, page_num_for_azure_upload, file_extension):
                logger.info(f"Page {page_num_for_azure_upload} for {date.strftime('%Y-%m-%d')} already exists in Azure. Skipping upload.")
                pages_processed_count = 1 # Mark as processed if it exists
            else:
                temp_jpg_name = f"{pdf_path.stem}_page_1.jpeg" # Always page 1
                temp_jpg_path = Path(TEMP_PDF_DIR) / temp_jpg_name 

                try:
                    page = doc.load_page(0) # Load the first (and only) page
                    zoom = 2.0
                    mat = fitz.Matrix(zoom, zoom)
                    pix = page.get_pixmap(matrix=mat)

                    pix.save(temp_jpg_path, "jpeg")
                    logger.info(f"Successfully converted page 1 to JPG: {temp_jpg_path.name}")

                    with open(temp_jpg_path, 'rb') as f:
                        image_data = f.read()

                    uploaded_url = azure_client.upload_image(
                        publisher_name=PUBLISHER_NAME,
                        date=date,
                        page_num=page_num_for_azure_upload,
                        image_data=image_data,
                        file_extension=file_extension
                    )
                    if uploaded_url:
                        logger.info(f"Uploaded page {page_num_for_azure_upload} to Azure: {uploaded_url}")
                        pages_processed_count = 1
                    else:
                        logger.error(f"Failed to upload page {page_num_for_azure_upload} to Azure.")
                        log_missing_page(date, original_pdf_url, page_num_for_azure_upload, f"Failed to upload JPG from PDF page 1")
                except Exception as convert_e:
                    logger.error(f"Failed to convert or upload page 1 (expected Azure page {page_num_for_azure_upload}) of {pdf_path.name}: {convert_e}")
                    log_missing_page(date, original_pdf_url, page_num_for_azure_upload, f"Failed to convert or upload PDF page 1")
                finally:
                    if temp_jpg_path.exists():
                        os.remove(temp_jpg_path)
                        logger.info(f"Cleaned up temporary JPG: {temp_jpg_path.name}")

            logger.info(f"Finished attempting to process page from {pdf_path.name}. Successfully processed {pages_processed_count} page(s).")

    except Exception as e:
        logger.error(f"Error opening or processing PDF {pdf_path.name}: {e}")
        log_missing_page(date, original_pdf_url, starting_azure_page_num, f"Failed to open/process entire PDF. Page {starting_azure_page_num} likely missing.")
        return 0 

    return pages_processed_count


def scrape_date(date: datetime, azure_client: AzureBlobStorage) -> bool:
    """
    Scrapes the e-paper for a specific date, downloads PDFs,
    converts them to JPGs, and uploads them to Azure Blob Storage, checking for existing blobs.
    Assumes all PDFs have only one page.
    """
    date_str = date.strftime('%Y%m%d')
    logger.info(f"\n--- Processing date: {date_str} ---")

    pdf_urls = get_download_urls(date_str)

    if not pdf_urls:
        logger.info(f"No PDF URLs found for {date_str}. Skipping this date.")
        return True # Considered successful if no content for this date

    logger.info(f"Found {len(pdf_urls)} PDF URLs for {date_str}.")

    current_output_page_num = 1    
    date_has_any_failures = False 

    for i, pdf_url in enumerate(pdf_urls):
        logger.info(f"Evaluating PDF {i+1}/{len(pdf_urls)} for {date_str}: {pdf_url}")

        # Assuming all PDFs are 1 page
        expected_pages_from_this_pdf = 1 

        # IMPORTANT NEW LOGIC: Check if the expected output JPG blob for this PDF is already in Azure BEFORE downloading
        expected_azure_page_num = current_output_page_num
        if azure_client.blob_exists(PUBLISHER_NAME, date, expected_azure_page_num, "jpg"):
            logger.info(f"Page {expected_azure_page_num} for {date.strftime('%Y-%m-%d')} already exists in Azure. Skipping download and processing this PDF.")
            current_output_page_num += expected_pages_from_this_pdf # Advance page number correctly
            time.sleep(0.1) # Small delay even on skip for politeness
            continue # Skip to the next PDF URL in the list, avoiding download

        # If we reach here, we need to download and process the PDF because the blob does not exist
        temp_pdf_path = Path(TEMP_PDF_DIR) / f"{date_str}_pdf_{i}.pdf"
        downloaded_pdf_path = download_pdf(pdf_url, temp_pdf_path)

        if downloaded_pdf_path:
            pages_successfully_processed_from_this_pdf = convert_pdf_and_upload(
                downloaded_pdf_path,    
                azure_client,    
                date,    
                starting_azure_page_num=current_output_page_num,
                original_pdf_url=pdf_url
            )
            
            # Since we assume 1 page, actual_pages_in_downloaded_pdf is always 1 unless there's a critical error
            # in opening the PDF.
            # We explicitly check for 1 page here, logging a warning if it's not.
            actual_pages_in_downloaded_pdf = 1
            try:
                with fitz.open(downloaded_pdf_path) as doc_check:
                    if doc_check.page_count != 1:
                        logger.warning(f"Downloaded PDF {downloaded_pdf_path.name} was expected to have 1 page but actually has {doc_check.page_count}.")
                        # Even if it has more, we only process the first one in convert_pdf_and_upload,
                        # but for numbering continuity, we still advance by 1 for this source PDF.
            except Exception as e:
                logger.error(f"Could not open downloaded PDF {downloaded_pdf_path} to verify page count, assuming 1 page for numbering: {e}")
                log_missing_page(date, pdf_url, current_output_page_num, "Could not open downloaded PDF to verify page count. Page assumed missing.")
                date_has_any_failures = True # Mark date as having issues
            finally: # PDF cleanup
                if downloaded_pdf_path.exists():
                    os.remove(downloaded_pdf_path)
                    logger.info(f"Cleaned up temporary PDF: {downloaded_pdf_path.name}")

            current_output_page_num += actual_pages_in_downloaded_pdf # Always advance by 1
            logger.info(f"Advanced output page number by {actual_pages_in_downloaded_pdf} page(s). Next PDF will start at Azure page {current_output_page_num}.")

            if pages_successfully_processed_from_this_pdf == 0: # If convert_pdf_and_upload failed
                date_has_any_failures = True
        else:
            logger.warning(f"Failed to download PDF from {pdf_url}. Skipping conversion and upload for this PDF.")
            log_missing_page(date, pdf_url, current_output_page_num, "PDF download failed. Page likely missing.")
            
            current_output_page_num += expected_pages_from_this_pdf # Advance page number even if PDF download failed
            date_has_any_failures = True # Mark date as having issues

        time.sleep(0.1) # Polite scraping delay between PDFs

    return not date_has_any_failures


def main():
    logger.info("=== Starting Ta Kung Pao E-Paper Scraper ===")

    # Initialize missing pages log file (clear it if it exists from a previous run, or just create it)
    if os.path.exists(MISSING_PAGES_LOG):
        os.remove(MISSING_PAGES_LOG)
    logger.info(f"Created/Cleared missing pages log: {MISSING_PAGES_LOG}")

    azure_client = create_azure_storage_client()
    if not azure_client:
        logger.error("Failed to initialize Azure Blob Storage client. Exiting.")
        return

    # Always start from START_DATE as checkpoint logic has been removed.
    start_from_date = START_DATE
    logger.info(f"Starting from configured START_DATE: {start_from_date.strftime('%Y-%m-%d')}")

    # Ensure END_DATE is not before start_from_date, and not in the future.
    effective_end_date = min(END_DATE, datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
    if start_from_date > effective_end_date:
        logger.info(f"Start date {start_from_date.strftime('%Y-%m-%d')} is after current effective end date {effective_end_date.strftime('%Y-%m-%d')}. No new dates to scrape.")
        return

    total_dates_to_scrape = (effective_end_date - start_from_date).days + 1
    logger.info(f"Will attempt to scrape {total_dates_to_scrape} dates from {start_from_date.strftime('%Y-%m-%d')} to {effective_end_date.strftime('%Y-%m-%d')}.")

    current_date = start_from_date
    processed_count = 0
    while current_date <= effective_end_date:
        try:
            # Call scrape_date for each date. It handles internal errors and continues.
            scrape_date(current_date, azure_client)
            
            processed_count += 1
            if processed_count % 10 == 0:
                logger.info(f"Processed {processed_count} dates. Taking a longer break.")
                time.sleep(5) # Long break after 10 dates
            else:
                time.sleep(1) # Short break between dates

        except Exception as e:
            logger.error(f"An unexpected error occurred during scraping for {current_date.strftime('%Y-%m-%d')}: {e}")
            # If a date-level error occurs, we still break to prevent uncontrolled execution.
            break    

        current_date += timedelta(days=1)

    final_processed_date = current_date - timedelta(days=1) if current_date > start_from_date else start_from_date
    logger.info(f"Scraping session finished. Last attempted date: {final_processed_date.strftime('%Y-%m-%d')}.")
    logger.info("=== Ta Kung Pao E-Paper Scraper Finished ===")


if __name__ == "__main__":
    main()
