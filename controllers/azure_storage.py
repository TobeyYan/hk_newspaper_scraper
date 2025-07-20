#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Azure Blob Storage utility for HK Newspaper project
- Handles image upload/download with hierarchical organization
- Supports publisher/YYYY/MM/DD/pageNum.extension structure
- Automatic container creation and error handling
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, ClientAuthenticationError

logger = logging.getLogger(__name__)

class AzureBlobStorage:
    """
    Azure Blob Storage utility for managing newspaper images
    with hierarchical namespace organization.
    """
    
    def __init__(self, connection_string: str, container_name: str = "epaper"):
        """
        Initialize Azure Blob Storage client.
        
        Args:
            connection_string: Azure storage account connection string
            container_name: Name of the blob container (default: newspaper-images)
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self._ensure_container_exists()
    
    def _ensure_container_exists(self):
        """Ensure the container exists, create if it doesn't"""
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            container_client.get_container_properties()
            logger.info(f"Using existing container: {self.container_name}")
        except ResourceNotFoundError:
            try:
                container_client = self.blob_service_client.create_container(self.container_name)
                logger.info(f"Created new container: {self.container_name}")
            except Exception as e:
                logger.error(f"Failed to create container {self.container_name}: {e}")
                raise
        except ClientAuthenticationError:
            logger.error("Authentication failed. Check your connection string.")
            raise
    
    def upload_image(self, 
                    publisher_name: str, 
                    date: datetime, 
                    page_num: int, 
                    image_data: bytes, 
                    file_extension: str = "jpg",
                    overwrite: bool = True) -> Optional[str]:
        """
        Upload an image to Azure Blob Storage with hierarchical organization.
        
        Args:
            publisher_name: Name of the newspaper publisher (e.g., 'am730', 'singtao')
            date: Publication date
            page_num: Page number (will be zero-padded to 3 digits)
            image_data: Binary image data
            file_extension: File extension (jpg, pdf, png, etc.)
            overwrite: Whether to overwrite existing files (default: True)
        
        Returns:
            str: The blob URL if successful, None if failed
        """
        try:
            # Create hierarchical path: publisher/YYYY/MM/DD/pageNum.extension
            year = date.strftime("%Y")
            month = date.strftime("%m")
            day = date.strftime("%d")
            
            blob_name = f"{publisher_name}/{year}/{month}/{day}/{page_num:03d}.{file_extension}"
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            # Check if blob exists
            blob_exists = blob_client.exists()
            
            if blob_exists and not overwrite:
                logger.warning(f"Blob already exists and overwrite=False: {blob_name}")
                return blob_client.url
            
            # Upload the blob
            blob_client.upload_blob(image_data, overwrite=overwrite)
            
            logger.info(f"Successfully uploaded: {blob_name}")
            return blob_client.url
            
        except Exception as e:
            logger.error(f"Failed to upload image {publisher_name}/{date.strftime('%Y/%m/%d')}/{page_num:03d}.{file_extension}: {e}")
            return None
    
    def download_image(self, 
                      publisher_name: str, 
                      date: datetime, 
                      page_num: int, 
                      file_extension: str = "jpg") -> Optional[bytes]:
        """
        Download an image from Azure Blob Storage.
        
        Args:
            publisher_name: Name of the newspaper publisher
            date: Publication date
            page_num: Page number
            file_extension: File extension
        
        Returns:
            bytes: Image data if found, None if not found
        """
        try:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            day = date.strftime("%d")
            
            blob_name = f"{publisher_name}/{year}/{month}/{day}/{page_num:03d}.{file_extension}"
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            if not blob_client.exists():
                logger.warning(f"Blob not found: {blob_name}")
                return None
            
            blob_data = blob_client.download_blob().readall()
            logger.info(f"Successfully downloaded: {blob_name}")
            return blob_data
            
        except Exception as e:
            logger.error(f"Failed to download image {publisher_name}/{date.strftime('%Y/%m/%d')}/{page_num:03d}.{file_extension}: {e}")
            return None
    
    def list_images(self, 
                   publisher_name: Optional[str] = None,
                   year: Optional[str] = None,
                   month: Optional[str] = None,
                   day: Optional[str] = None,
                   max_results: int = 1000) -> List[Dict[str, Any]]:
        """
        List images based on hierarchical filters.
        
        Args:
            publisher_name: Filter by publisher
            year: Filter by year (YYYY)
            month: Filter by month (MM)
            day: Filter by day (DD)
            max_results: Maximum number of results to return
        
        Returns:
            List of dictionaries with blob information
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            # Build prefix for hierarchical filtering
            prefix = ""
            if publisher_name:
                prefix += f"{publisher_name}/"
            if year:
                prefix += f"{year}/"
            if month:
                prefix += f"{month}/"
            if day:
                prefix += f"{day}/"
            
            blobs = container_client.list_blobs(
                name_starts_with=prefix
            )
            
            results = []
            for blob in blobs:
                blob_info = {
                    'name': blob.name,
                    'url': f"https://hknews.blob.core.windows.net/{self.container_name}/{blob.name}",
                    'size': blob.size,
                    'last_modified': blob.last_modified,
                    'content_type': blob.content_settings.content_type if blob.content_settings else None
                }
                results.append(blob_info)
            
            logger.info(f"Found {len(results)} blobs with prefix: {prefix}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to list images: {e}")
            return []
    
    def delete_image(self, 
                    publisher_name: str, 
                    date: datetime, 
                    page_num: int, 
                    file_extension: str = "jpg") -> bool:
        """
        Delete an image from Azure Blob Storage.
        
        Args:
            publisher_name: Name of the newspaper publisher
            date: Publication date
            page_num: Page number
            file_extension: File extension
        
        Returns:
            bool: True if successful, False if failed or not found
        """
        try:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            day = date.strftime("%d")
            
            blob_name = f"{publisher_name}/{year}/{month}/{day}/{page_num:03d}.{file_extension}"
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            if not blob_client.exists():
                logger.warning(f"Blob not found for deletion: {blob_name}")
                return False
            
            blob_client.delete_blob()
            logger.info(f"Successfully deleted: {blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete image {publisher_name}/{date.strftime('%Y/%m/%d')}/{page_num:03d}.{file_extension}: {e}")
            return False
    
    def get_blob_url(self, 
                     publisher_name: str, 
                     date: datetime, 
                     page_num: int, 
                     file_extension: str = "jpg") -> str:
        """
        Get the URL for a blob without checking if it exists.
        
        Args:
            publisher_name: Name of the newspaper publisher
            date: Publication date
            page_num: Page number
            file_extension: File extension
        
        Returns:
            str: The blob URL
        """
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")
        
        blob_name = f"{publisher_name}/{year}/{month}/{day}/{page_num:03d}.{file_extension}"
        
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, 
            blob=blob_name
        )
        
        return blob_client.url
    
    def blob_exists(self, 
                   publisher_name: str, 
                   date: datetime, 
                   page_num: int, 
                   file_extension: str = "jpg") -> bool:
        """
        Check if a blob exists.
        
        Args:
            publisher_name: Name of the newspaper publisher
            date: Publication date
            page_num: Page number
            file_extension: File extension
        
        Returns:
            bool: True if blob exists, False otherwise
        """
        try:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            day = date.strftime("%d")
            
            blob_name = f"{publisher_name}/{year}/{month}/{day}/{page_num:03d}.{file_extension}"
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            return blob_client.exists()
            
        except Exception as e:
            logger.error(f"Failed to check blob existence: {e}")
            return False


def create_azure_storage_client(container_name: str = "epaper") -> AzureBlobStorage:
    """
    Factory function to create Azure Blob Storage client using environment variables.
    
    Args:
        container_name: Name of the blob container
    
    Returns:
        AzureBlobStorage: Configured storage client
    
    Raises:
        ValueError: If AZURE_STORAGE_CONNECTION_STRING is not set
    """
    connection_string = os.getenv("BLOB_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required")
    
    return AzureBlobStorage(connection_string, container_name) 