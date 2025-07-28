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
START_DATE = datetime(2018, 7, 28) # This is your desired start date
END_DATE = datetime(2018, 12, 31)
PUBLISHER_NAME = "TaKungPao"
TEMP_PDF_DIR = "temp_downloads"
CHECKPOINT_FILE = "takungpao_checkpoint.txt"

# Create necessary temporary directory
Path(TEMP_PDF_DIR).mkdir(parents=True, exist_ok=True)


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
        if response.status_code == 404:
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

        with fitz.open(stream=initial_bytes, filetype="pdf") as doc:
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
    """
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


def convert_pdf_and_upload(pdf_path: Path, azure_client: AzureBlobStorage, date: datetime, page_number_offset: int = 0) -> bool:
    """
    Converts pages of a PDF to JPGs, uploads them to Azure, and handles cleanup.
    Only uploads if the blob does not already exist.
    Returns True if all pages were either uploaded or already existed, False if any error occurred.
    """
    all_pages_processed_ok = True
    if not pdf_path or not pdf_path.exists():
        logger.error(f"PDF file not found for conversion: {pdf_path}")
        return False

    try:
        with fitz.open(pdf_path) as doc:
            logger.info(f"Opened PDF {pdf_path.name} with {doc.page_count} pages.")
            for i in range(doc.page_count):
                page_num_for_upload = i + 1 + page_number_offset
                file_extension = "jpg" # Output format for Azure

                # Check if this specific page (JPG blob) already exists in Azure
                if azure_client.blob_exists(PUBLISHER_NAME, date, page_num_for_upload, file_extension):
                    logger.info(f"Page {page_num_for_upload} for {date.strftime('%Y-%m-%d')} already exists in Azure. Skipping upload.")
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
                        page_num=page_num_for_upload,
                        image_data=image_data,
                        file_extension=file_extension
                    )
                    if uploaded_url:
                        logger.info(f"Uploaded page {page_num_for_upload} to Azure: {uploaded_url}")
                    else:
                        logger.error(f"Failed to upload page {page_num_for_upload} to Azure.")
                        all_pages_processed_ok = False # Mark overall status as failed
                except Exception as convert_e:
                    logger.error(f"Failed to convert or upload page {i+1} of {pdf_path.name}: {convert_e}")
                    all_pages_processed_ok = False # Mark overall status as failed
                    continue # Try next page in PDF if one fails
                finally:
                    if temp_jpg_path.exists():
                        os.remove(temp_jpg_path)
                        logger.info(f"Cleaned up temporary JPG: {temp_jpg_path.name}")

        logger.info(f"Finished attempting to process pages from {pdf_path.name}.")

    except Exception as e:
        logger.error(f"Error opening or processing PDF {pdf_path.name}: {e}")
        all_pages_processed_ok = False

    # PDF cleanup is now handled in scrape_date AFTER page count is determined for offset
    return all_pages_processed_ok


def save_checkpoint(date: datetime):
    """Saves the given date as the last successfully processed date."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            f.write(date.strftime("%Y-%m-%d"))
        logger.info(f"Checkpoint saved: {date.strftime('%Y-%m-%d')}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")

def load_checkpoint() -> Union[datetime, None]:
    """Loads the last successfully processed date and returns the *next* date to start from."""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r') as f:
                date_str = f.read().strip()
            last_processed_date = datetime.strptime(date_str, "%Y-%m-%d")
            return last_processed_date + timedelta(days=1)
        return None
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
        return None


def scrape_date(date: datetime, azure_client: AzureBlobStorage) -> bool:
    """
    Scrapes the e-paper for a specific date, downloads PDFs,
    converts them to JPGs, and uploads them to Azure Blob Storage, checking for existing blobs.
    """
    date_str = date.strftime('%Y%m%d')
    logger.info(f"\n--- Processing date: {date_str} ---")

    pdf_urls = get_download_urls(date_str)

    if not pdf_urls:
        logger.info(f"No PDF URLs found for {date_str}. Skipping this date.")
        return True # Considered successful if no content for this date

    logger.info(f"Found {len(pdf_urls)} PDF URLs for {date_str}.")

    current_output_page_num = 1  # Tracks the 1-based output page number across all PDFs for this date
    all_pdfs_for_date_processed_successfully = True

    for i, pdf_url in enumerate(pdf_urls):
        logger.info(f"Evaluating PDF {i+1}/{len(pdf_urls)} for {date_str}: {pdf_url}")

        pdf_page_count = get_pdf_page_count_from_url(pdf_url)

        # --- NEW & IMPROVED PAGE CHECKING LOGIC ---
        can_skip_download = False
        if pdf_page_count is not None and pdf_page_count > 0:
            # Check if ALL expected output JPGs for this PDF are already in Azure
            all_expected_pages_exist = True
            for page_idx_in_pdf in range(pdf_page_count):
                expected_azure_page_num = current_output_page_num + page_idx_in_pdf
                if not azure_client.blob_exists(PUBLISHER_NAME, date, expected_azure_page_num, "jpg"):
                    all_expected_pages_exist = False
                    break # Found a missing page, so we cannot skip the download for this PDF

            if all_expected_pages_exist:
                logger.info(f"All {pdf_page_count} pages from PDF {i+1} ({pdf_url}) for {date_str} already exist in Azure. Skipping download and processing.")
                current_output_page_num += pdf_page_count # IMPORTANT: Advance page number correctly even if skipped
                can_skip_download = True
        else:
            # If we can't get a reliable page count, we can't reliably pre-check.
            # We must proceed with download and rely on page-level checks within convert_pdf_and_upload.
            logger.warning(f"Could not reliably determine page count for PDF {i+1} ({pdf_url}). Will attempt download and processing regardless.")


        if can_skip_download:
            time.sleep(0.1) # Small delay even on skip for politeness
            continue # Skip to the next PDF URL in the list


        # If we reach here, we need to download and process the PDF
        temp_pdf_path = Path(TEMP_PDF_DIR) / f"{date_str}_pdf_{i}.pdf"
        downloaded_pdf_path = download_pdf(pdf_url, temp_pdf_path)

        if downloaded_pdf_path:
            pages_processed_ok_for_this_pdf = convert_pdf_and_upload(downloaded_pdf_path, azure_client, date, page_number_offset=current_output_page_num - 1)

            # After processing (or attempting to process), get actual page count from the downloaded file and advance.
            actual_pages_in_pdf = 0
            try:
                with fitz.open(downloaded_pdf_path) as doc_actual_pages:
                    actual_pages_in_pdf = doc_actual_pages.page_count
            except Exception as e:
                logger.error(f"Could not determine actual page count for downloaded PDF {downloaded_pdf_path}: {e}. This may affect subsequent page numbering.")
                all_pdfs_for_date_processed_successfully = False # Consider this a failure for the date if page count tracking breaks
            finally: # PDF cleanup happens here, AFTER its page count has been used
                if downloaded_pdf_path.exists():
                    os.remove(downloaded_pdf_path)
                    logger.info(f"Cleaned up temporary PDF: {downloaded_pdf_path.name}")

            # Advance current_output_page_num based on the actual pages found in the PDF
            current_output_page_num += actual_pages_in_pdf
            logger.info(f"Advanced output page number by {actual_pages_in_pdf} pages. Next PDF will start at Azure page {current_output_page_num}.")


            if not pages_processed_ok_for_this_pdf:
                all_pdfs_for_date_processed_successfully = False # A page failed to convert/upload for this PDF

        else:
            logger.warning(f"Failed to download PDF from {pdf_url}. Skipping conversion and upload.")
            all_pdfs_for_date_processed_successfully = False # Mark as failure for this date

        time.sleep(0.1) # Polite scraping delay between PDFs

    return all_pdfs_for_date_processed_successfully


def main():
    logger.info("=== Starting Ta Kung Pao E-Paper Scraper ===")

    azure_client = create_azure_storage_client()
    if not azure_client:
        logger.error("Failed to initialize Azure Blob Storage client. Exiting.")
        return

    # --- MODIFIED CHECKPOINT LOADING LOGIC ---
    loaded_checkpoint_date = load_checkpoint()

    # Define the specific problematic checkpoint date we want to ignore.
    # Since load_checkpoint() returns the *next* date to start from,
    # if the checkpoint file contained '2018-06-09', load_checkpoint() would return '2018-06-10'.
    # So, PROBLEM_CHECKPOINT_DATE should be 2018-06-10.
    PROBLEM_CHECKPOINT_DATE = datetime(2018, 6, 10)

    if loaded_checkpoint_date == PROBLEM_CHECKPOINT_DATE:
        start_from_date = START_DATE # Use the script's defined START_DATE (2018-07-03 in your case)
        logger.info(f"Checkpoint date found is {PROBLEM_CHECKPOINT_DATE.strftime('%Y-%m-%d')}, which is problematic. Overriding to start from: {start_from_date.strftime('%Y-%m-%d')}")
    elif loaded_checkpoint_date:
        start_from_date = loaded_checkpoint_date
        logger.info(f"Resuming from checkpoint: {start_from_date.strftime('%Y-%m-%d')}")
    else:
        start_from_date = START_DATE
        logger.info(f"No valid checkpoint found or checkpoint is problematic. Starting from beginning: {start_from_date.strftime('%Y-%m-%d')}")
    # --- END MODIFIED CHECKPOINT LOADING LOGIC ---

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
            success = scrape_date(current_date, azure_client)
            if success:
                save_checkpoint(current_date) # Save checkpoint only on full success of the date
            else:
                logger.error(f"Processing failed for {current_date.strftime('%Y-%m-%d')}. Stopping to address the issue.")
                break # Stop the loop on first failure to allow investigation

            processed_count += 1
            if processed_count % 10 == 0:
                logger.info(f"Processed {processed_count} dates. Taking a longer break.")
                time.sleep(5) # Long break after 10 dates
            else:
                time.sleep(1) # Short break between dates

        except Exception as e:
            logger.error(f"An unexpected error occurred during scraping for {current_date.strftime('%Y-%m-%d')}: {e}")
            break # Stop the loop on unexpected error

        current_date += timedelta(days=1)

    final_processed_date = current_date - timedelta(days=1) if current_date > start_from_date else start_from_date
    logger.info(f"Scraping session finished. Last attempted date: {final_processed_date.strftime('%Y-%m-%d')}.")
    logger.info("=== Ta Kung Pao E-Paper Scraper Finished ===")


if __name__ == "__main__":
    main()
