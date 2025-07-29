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
START_DATE = datetime(2019, 9, 15) # This is your desired start date
END_DATE = datetime(2020, 6, 30)
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


def get_pdf_page_count_from_url(pdf_url: str) -> Union[int, None]:
    """
    Attempts to get the page count of a remote PDF without fully downloading it.
    This reads a small part of the PDF to find the page count.
    Returns the page count or None if unsuccessful.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Range': 'bytes=0-4096' # Request first 4KB, usually enough for PDF header to try to parse
        }
        response = requests.get(pdf_url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()

        initial_bytes = response.raw.read(4096)

        if not initial_bytes.startswith(b'%PDF'):
            logger.warning(f"URL {pdf_url} does not seem to be a valid PDF (missing %PDF header).")
            return None

        # Use BytesIO to simulate a file for PyMuPDF
        with fitz.open(stream=io.BytesIO(initial_bytes), filetype="pdf") as doc:
            if doc.page_count > 0:
                return doc.page_count
            else:
                logger.warning(f"Could not reliably determine page count from partial download for {pdf_url}.")
                return None

    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to get partial PDF for page count from {pdf_url}: {e}")
        return None
    except fitz.EmptyInputError:
        logger.warning(f"Partial PDF content from {pdf_url} is empty or unreadable by PyMuPDF.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while getting PDF page count from {pdf_url}: {e}")
        return None


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
    Converts pages of a PDF to JPGs, uploads them to Azure, and handles cleanup.
    Only uploads if the blob does not already exist.
    Returns the number of pages successfully processed (uploaded or already existed) from this PDF.
    If an error occurs for a page, it's logged as missing, but processing continues for subsequent pages.
    """
    pages_processed_count = 0
    
    if not pdf_path or not pdf_path.exists():
        logger.error(f"PDF file not found for conversion: {pdf_path}")
        # Log all expected pages from this PDF as missing if the entire PDF cannot be processed
        # We need the page count from the URL pre-check, or assume 1 if not available
        # This part of the logic needs to be careful not to double log.
        # If the PDF wasn't downloaded, the error is caught earlier in scrape_date.
        # This branch implies the path was provided but doesn't exist.
        log_missing_page(date, original_pdf_url, starting_azure_page_num, "PDF file not found locally after download attempt.")
        return 0

    try:
        with fitz.open(pdf_path) as doc:
            logger.info(f"Opened PDF {pdf_path.name} with {doc.page_count} pages.")
            for i in range(doc.page_count):
                page_num_for_azure_upload = starting_azure_page_num + i
                file_extension = "jpg" # Output format for Azure

                # Check if this specific page (JPG blob) already exists in Azure
                if azure_client.blob_exists(PUBLISHER_NAME, date, page_num_for_azure_upload, file_extension):
                    logger.info(f"Page {page_num_for_azure_upload} for {date.strftime('%Y-%m-%d')} already exists in Azure. Skipping upload.")
                    pages_processed_count += 1
                    continue # Skip to next page if it exists

                # If blob doesn't exist, proceed with conversion and upload
                temp_jpg_name = f"{pdf_path.stem}_page_{i+1}.jpeg"
                temp_jpg_path = Path(TEMP_PDF_DIR) / temp_jpg_name # Use TEMP_PDF_DIR for all temps

                try:
                    page = doc.load_page(i)
                    zoom = 2.0
                    mat = fitz.Matrix(zoom, zoom)
                    pix = page.get_pixmap(matrix=mat)

                    # Save to temp file for upload, or use in-memory bytes if your azure client supports it better
                    pix.save(temp_jpg_path, "jpeg")
                    logger.info(f"Successfully converted page {i+1} to JPG: {temp_jpg_path.name}")

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
                        pages_processed_count += 1
                    else:
                        logger.error(f"Failed to upload page {page_num_for_azure_upload} to Azure.")
                        log_missing_page(date, original_pdf_url, page_num_for_azure_upload, f"Failed to upload JPG from PDF page {i+1}")
                except Exception as convert_e:
                    logger.error(f"Failed to convert or upload page {i+1} (expected Azure page {page_num_for_azure_upload}) of {pdf_path.name}: {convert_e}")
                    log_missing_page(date, original_pdf_url, page_num_for_azure_upload, f"Failed to convert or upload PDF page {i+1}")
                finally:
                    if temp_jpg_path.exists():
                        os.remove(temp_jpg_path)
                        logger.info(f"Cleaned up temporary JPG: {temp_jpg_path.name}")

            logger.info(f"Finished attempting to process pages from {pdf_path.name}. Successfully processed {pages_processed_count} pages.")

    except Exception as e:
        logger.error(f"Error opening or processing PDF {pdf_path.name}: {e}")
        # If the entire PDF cannot be opened/processed, log all its expected pages as missing.
        # We need the actual page count of the PDF that *was* downloaded.
        actual_pages_in_pdf = 0
        try:
            with fitz.open(pdf_path) as doc_actual_pages:
                actual_pages_in_pdf = doc_actual_pages.page_count
        except Exception:
            pass # Ignore errors here, we just need a best guess if the file is truly unopenable.
        
        # If we couldn't get a count, assume at least one page was expected for logging purposes.
        if actual_pages_in_pdf == 0:
            # This is a fallback. Ideally, the pre-check would have given an estimate.
            # If a PDF downloads but is completely unreadable, this might be hit.
            logger.warning(f"Could not get page count from downloaded PDF {pdf_path.name}. Assuming 1 page for logging missing.")
            actual_pages_in_pdf = 1 

        for i in range(actual_pages_in_pdf):
            log_missing_page(date, original_pdf_url, starting_azure_page_num + i, f"Failed to open/process entire PDF. Page {i+1} likely missing.")
        
        return 0 # Indicate 0 pages successfully processed from this PDF

    return pages_processed_count


def scrape_date(date: datetime, azure_client: AzureBlobStorage) -> bool:
    """
    Scrapes the e-paper for a specific date, downloads PDFs,
    converts them to JPGs, and uploads them to Azure Blob Storage, checking for existing blobs.
    This version continues processing even if some PDFs or pages fail.
    """
    date_str = date.strftime('%Y%m%d')
    logger.info(f"\n--- Processing date: {date_str} ---")

    pdf_urls = get_download_urls(date_str)

    if not pdf_urls:
        logger.info(f"No PDF URLs found for {date_str}. Skipping this date.")
        return True # Considered successful if no content for this date

    logger.info(f"Found {len(pdf_urls)} PDF URLs for {date_str}.")

    # Tracks the 1-based output page number across all PDFs for this date in Azure
    # This is crucial for sequential numbering.
    current_output_page_num = 1    
    
    # Track overall success for the date. If any page fails (even if others succeed), this becomes False.
    date_has_any_failures = False 

    for i, pdf_url in enumerate(pdf_urls):
        logger.info(f"Evaluating PDF {i+1}/{len(pdf_urls)} for {date_str}: {pdf_url}")

        pre_checked_pdf_page_count = get_pdf_page_count_from_url(pdf_url)

        # Determine how many pages this PDF is *expected* to contribute to the overall sequence
        # We use pre_checked_pdf_page_count if available, otherwise default to 1 for calculation.
        expected_pages_from_this_pdf = pre_checked_pdf_page_count if pre_checked_pdf_page_count is not None else 1

        # Check if ALL expected output JPGs for this PDF are already in Azure
        all_expected_pages_exist_in_azure = True
        if pre_checked_pdf_page_count is not None:
            for page_idx_in_pdf in range(pre_checked_pdf_page_count):
                expected_azure_page_num = current_output_page_num + page_idx_in_pdf
                if not azure_client.blob_exists(PUBLISHER_NAME, date, expected_azure_page_num, "jpg"):
                    all_expected_pages_exist_in_azure = False
                    break # Found a missing page, so we cannot skip the download for this PDF
        else:
            # If we can't pre-check page count, we can't reliably say all exist.
            # We must proceed with download and rely on page-level checks within convert_pdf_and_upload.
            all_expected_pages_exist_in_azure = False    
            logger.warning(f"Could not reliably determine page count for PDF {i+1} ({pdf_url}). Will attempt download and processing regardless.")

        if all_expected_pages_exist_in_azure:
            logger.info(f"All {expected_pages_from_this_pdf} pages from PDF {i+1} ({pdf_url}) for {date_str} already exist in Azure. Skipping download and processing.")
            current_output_page_num += expected_pages_from_this_pdf # Advance page number correctly
            time.sleep(0.1) # Small delay even on skip for politeness
            continue # Skip to the next PDF URL in the list

        # If we reach here, we need to download and process the PDF
        temp_pdf_path = Path(TEMP_PDF_DIR) / f"{date_str}_pdf_{i}.pdf"
        downloaded_pdf_path = download_pdf(pdf_url, temp_pdf_path)

        if downloaded_pdf_path:
            # Pass the current_output_page_num to the conversion function
            pages_successfully_processed_from_this_pdf = convert_pdf_and_upload(
                downloaded_pdf_path,    
                azure_client,    
                date,    
                starting_azure_page_num=current_output_page_num,
                original_pdf_url=pdf_url
            )
            
            # The actual page count of the downloaded PDF. This is critical for correct page numbering.
            actual_pages_in_downloaded_pdf = 0
            try:
                with fitz.open(downloaded_pdf_path) as doc_actual_pages:
                    actual_pages_in_downloaded_pdf = doc_actual_pages.page_count
            except Exception as e:
                logger.error(f"Could not determine actual page count for downloaded PDF {downloaded_pdf_path}: {e}. This may affect subsequent page numbering.")
                # If we can't even open the downloaded PDF, we assume the 'expected' number of pages for numbering consistency
                # and mark each of those as missing in the log.
                for page_idx in range(expected_pages_from_this_pdf):
                    log_missing_page(date, pdf_url, current_output_page_num + page_idx, "Could not open downloaded PDF to get actual page count. Page assumed missing.")
                actual_pages_in_downloaded_pdf = expected_pages_from_this_pdf # Use expected for advancing page number
                date_has_any_failures = True # Mark date as having issues
            finally: # PDF cleanup happens here, AFTER its page count has been used
                if downloaded_pdf_path.exists():
                    os.remove(downloaded_pdf_path)
                    logger.info(f"Cleaned up temporary PDF: {downloaded_pdf_path.name}")

            # Advance current_output_page_num based on the *actual* pages found in the PDF.
            # If the PDF was corrupt or empty and we couldn't get a page count, actual_pages_in_downloaded_pdf will be 0 or 1.
            current_output_page_num += actual_pages_in_downloaded_pdf
            logger.info(f"Advanced output page number by {actual_pages_in_downloaded_pdf} pages. Next PDF will start at Azure page {current_output_page_num}.")

            if pages_successfully_processed_from_this_pdf < actual_pages_in_downloaded_pdf:
                # If not all pages expected from the PDF were processed, mark as failure for the date.
                date_has_any_failures = True
        else:
            logger.warning(f"Failed to download PDF from {pdf_url}. Skipping conversion and upload for this PDF.")
            # If a PDF fails to download, we need to account for its expected pages in the numbering.
            # We use the number of pages we tried to pre-check, or 1 if pre-check failed.
            for page_idx in range(expected_pages_from_this_pdf):
                log_missing_page(date, pdf_url, current_output_page_num + page_idx, "PDF download failed. Page likely missing.")
            
            current_output_page_num += expected_pages_from_this_pdf # Advance page number even if PDF download failed
            date_has_any_failures = True # Mark date as having issues

        time.sleep(0.1) # Polite scraping delay between PDFs

    # Return True if no errors were encountered for *any* page/PDF for this date, False otherwise.
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
