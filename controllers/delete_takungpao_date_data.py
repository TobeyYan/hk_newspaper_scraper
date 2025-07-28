#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Import BlobServiceClient directly as we cannot modify controllers/azure_storage.py
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError # Import for better error handling

# Setup logging for this utility script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PUBLISHER_NAME = "TaKungPao"

def delete_blobs_by_prefix_independent(connection_string: str, container_name: str, prefix: str) -> int:
    """
    Deletes all blobs within the specified container that start with the given prefix.
    This function is independent and does not rely on the AzureBlobStorage class in controllers.
    Returns the number of blobs deleted.
    """
    deleted_count = 0
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Check if container exists before proceeding
        try:
            container_client.get_container_properties()
        except ResourceNotFoundError:
            logger.error(f"Azure container '{container_name}' not found. Cannot proceed with deletion.")
            return 0
        except Exception as e:
            logger.error(f"Error checking container '{container_name}' existence: {e}")
            return 0

        blob_list = container_client.list_blobs(name_starts_with=prefix)
        blobs_to_delete = [blob.name for blob in blob_list]

        if not blobs_to_delete:
            logger.info(f"No blobs found with prefix '{prefix}' for deletion in container '{container_name}'.")
            return 0

        logger.info(f"Found {len(blobs_to_delete)} blobs to delete with prefix '{prefix}' in container '{container_name}'.")
        
        for blob_name in blobs_to_delete:
            try:
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.delete_blob()
                logger.info(f"Successfully deleted blob: {blob_name}")
                deleted_count += 1
            except Exception as e:
                logger.error(f"Failed to delete blob '{blob_name}': {e}")
        
    except Exception as e:
        logger.error(f"Error initializing BlobServiceClient or listing blobs for prefix '{prefix}': {e}")
    
    return deleted_count

def delete_date_data_independent(target_date: datetime):
    """
    Connects to Azure Blob Storage and deletes all blobs for a specific date.
    This version is independent and does not rely on the AzureBlobStorage class in controllers.
    Includes a confirmation prompt.
    """
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.environ.get("AZURE_CONTAINER_NAME", "epaper-images") # Default container name

    if not connection_string:
        logger.error("AZURE_STORAGE_CONNECTION_STRING environment variable not set. Cannot proceed.")
        sys.exit(1)

    # Construct the prefix for blobs related to the target_date
    date_path_prefix = f"{PUBLISHER_NAME}/{target_date.strftime('%Y')}/{target_date.strftime('%m')}/{target_date.strftime('%d')}/"
    
    logger.info(f"Preparing to delete all data for {target_date.strftime('%Y-%m-%d')} from Azure Blob Storage.")
    logger.info(f"This will delete all blobs with the prefix: '{date_path_prefix}' in container '{container_name}'.")
    
    confirm = input("Are you sure you want to proceed with deletion (y/N)? ").strip().lower()
    
    if confirm == 'y':
        logger.info("Starting deletion...")
        deleted_count = delete_blobs_by_prefix_independent(connection_string, container_name, date_path_prefix)
        logger.info(f"Deletion complete for {target_date.strftime('%Y-%m-%d')}. Deleted {deleted_count} blobs.")
    else:
        logger.info("Deletion cancelled by user.")

if __name__ == "__main__":
    # --- IMPORTANT: Specify the date you want to delete here ---
    date_to_delete = datetime(2018, 7, 27) 
    # -----------------------------------------------------------

    logger.info(f"Running deletion utility script for {PUBLISHER_NAME} on {date_to_delete.strftime('%Y-%m-%d')}")
    delete_date_data_independent(date_to_delete)
