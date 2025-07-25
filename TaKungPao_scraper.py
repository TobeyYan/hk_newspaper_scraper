#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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

# Import Azure storage utility
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'controllers'))
from azure_storage import create_azure_storage_client


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
START_DATE = datetime(2018, 6, 8) # Your original start date
END_DATE = datetime.now() # Dynamically set end date to current date
PUBLISHER_NAME = "TaKungPao"
TEMP_PDF_DIR = "temp_downloads"
CHECKPOINT_FILE = "takungpao_checkpoint.txt" # New checkpoint file

# Create necessary temporary directory
Path(TEMP_PDF_DIR).mkdir(parents=True, exist_ok=True)


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
        # Send a GET request to the URL
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


def download_pdf(pdf_url: str, date_str: str, page_index: int) -> Union[Path, None]:
    """
    Downloads a PDF file from the given URL and saves it to a temporary directory.

    Returns:
        The Path object to the downloaded PDF file, or None if download fails.
    """
    
    temp_pdf_path = Path(TEMP_PDF_DIR) / f"{date_str}_page_{page_index}.pdf"
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


def convert_pdf_and_upload(pdf_path: Path, azure_client: AzureBlobStorage, date: datetime, page_number_offset: int = 0) -> list[str]:
    """
    Converts a PDF file into multiple JPG images, one for each page,
    and uploads them to Azure Blob Storage using the provided azure_client.

    Args:
        pdf_path: The path to the PDF file.
        azure_client: The initialized AzureBlobStorage client.
        date: The datetime object for the current date.
        page_number_offset: An offset to apply to page numbers.

    Returns:
        A list of URLs of the uploaded JPG images.
    """
    uploaded_image_urls = []
    if not pdf_path or not pdf_path.exists():
        logger.error(f"PDF file not found for conversion: {pdf_path}")
        return []

    try:
        with fitz.open(pdf_path) as doc:
            logger.info(f"Opened PDF {pdf_path.name} with {doc.page_count} pages.")

            for i in range(doc.page_count):
                page = doc.load_page(i)
                zoom = 2.0
                mat = fitz.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat)

                # Get image data as bytes (PyMuPDF's pil_save for in-memory conversion)
                try:
                    img_bytes = pix.pil_save(None, format="jpeg", optimize=True)
                except Exception as e:
                    logger.error(f"Failed to convert pixmap to bytes for page {i+1} of {pdf_path.name}: {e}. Ensure Pillow is installed.")
                    continue
                
                # Construct the 1-based page number for the output file name
                page_num_for_upload = i + 1 + page_number_offset 
                file_extension = "jpg" # Output format is JPG

                # Upload to Azure using the utility function
                uploaded_url = azure_client.upload_image(
                    publisher_name=PUBLISHER_NAME,
                    date=date,
                    page_num=page_num_for_upload,
                    image_data=img_bytes,
                    file_extension=file_extension
                )
                
                if uploaded_url:
                    uploaded_image_urls.append(uploaded_url)
                else:
                    logger.warning(f"Failed to upload image for {PUBLISHER_NAME}/{date.strftime('%Y/%m/%d')}/{page_num_for_upload}.{file_extension}")
            
            logger.info(f"Finished processing pages from {pdf_path.name}.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during PDF conversion or upload for {pdf_path}: {e}")
    finally:
        if pdf_path.exists():
            os.remove(pdf_path)
            logger.info(f"Cleaned up temporary PDF: {pdf_path}")

    return uploaded_image_urls


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
        logger.info(f"No PDF URLs found for {date_str}. Skipping this date.")
        # If no URLs, it's considered successfully "processed" for this date (e.g., holiday)
        return True 

    logger.info(f"Found {len(pdf_urls)} PDF URLs for {date_str}.")
    
    current_output_page_num = 1 # Initialize page number for the output filename
    all_pages_uploaded_for_date = True

    for i, pdf_url in enumerate(pdf_urls):
        logger.info(f"Processing PDF {i+1}/{len(pdf_urls)} for {date_str}: {pdf_url}")
        
        temp_pdf_path = download_pdf(pdf_url, date_str, i)
        if temp_pdf_path:
            # Convert PDF to JPGs and upload to Azure
            uploaded_urls = convert_pdf_and_upload(temp_pdf_path, azure_client, date, page_number_offset=current_output_page_num - 1)
            
            if uploaded_urls:
                current_output_page_num += len(uploaded_urls)
                logger.info(f"Successfully processed and uploaded {len(uploaded_urls)} pages from {pdf_url}.")
            else:
                logger.warning(f"No images converted or uploaded from PDF: {pdf_url}")
                all_pages_uploaded_for_date = False # Mark as failure for this date
        else:
            logger.warning(f"Failed to download PDF from {pdf_url}. Skipping conversion and upload.")
            all_pages_uploaded_for_date = False # Mark as failure for this date

        # Polite scraping: Wait a bit after each PDF download/conversion/upload
        time.sleep(0.1)
    
    return all_pages_uploaded_for_date


def main():
    logger.info("=== Starting Ta Kung Pao E-Paper Scraper ===")

    # Initialize Azure Blob Storage client
    azure_client = create_azure_storage_client()
    if not azure_client:
        logger.error("Failed to initialize Azure Blob Storage client. Exiting.")
        return

    # Load checkpoint or start from START_DATE
    start_from_date = load_checkpoint()
    if start_from_date:
        logger.info(f"Resuming from checkpoint: {start_from_date.strftime('%Y-%m-%d')}")
    else:
        start_from_date = START_DATE
        logger.info(f"Starting from beginning: {start_from_date.strftime('%Y-%m-%d')}")

    current_date = start_from_date
    processed_count = 0
    while current_date <= END_DATE:
        try:
            success = scrape_date(current_date, azure_client)
            if success:
                save_checkpoint(current_date) # Save checkpoint only on successful completion of a date
            else:
                # If a date fails, we stop and let the next run pick up from the last successful checkpoint.
                # Or, you could decide to attempt the failed date again on the next run.
                # For now, we'll stop to ensure we don't skip dates.
                logger.error(f"Processing failed for {current_date.strftime('%Y-%m-%d')}. Stopping.")
                break 

            processed_count += 1
            # Polite scraping: Wait longer between different dates, especially if many are processed
            # Adjust sleep time based on typical run duration and rate limits
            if processed_count % 10 == 0: # Save checkpoint more frequently, e.g., every 10 dates
                logger.info(f"Processed {processed_count} dates. Taking a longer break.")
                time.sleep(5) # Longer break after a batch of dates
            else:
                time.sleep(1) # Shorter break between individual dates

        except Exception as e:
            logger.error(f"An unexpected error occurred during scraping for {current_date.strftime('%Y-%m-%d')}: {e}")
            # If an error occurs, save the last successfully processed date (or the date *before* the error)
            # and break to prevent continuous failure.
            # The checkpoint will ensure it tries from the next day on the next run.
            break # Exit the loop on error to prevent cascading issues

        current_date += timedelta(days=1)

    logger.info(f"Scraping completed for dates from {start_from_date.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}.")
    logger.info(f"All downloaded images were uploaded to Azure Blob Storage in container '{os.environ.get('AZURE_CONTAINER_NAME', 'epaper-images')}'.")
    logger.info(f"Temporary PDF files were stored in '{TEMP_PDF_DIR}' and should have been cleaned up.")
    logger.info("=== Ta Kung Pao E-Paper Scraper Finished ===")


if __name__ == "__main__":
    main()

