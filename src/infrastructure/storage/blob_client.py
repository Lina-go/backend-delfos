"""Azure Blob Storage client."""

import logging
from typing import Optional

from azure.storage.blob.aio import BlobServiceClient
from azure.identity.aio import DefaultAzureCredential
from azure.core.exceptions import AzureError

from src.config.settings import Settings

logger = logging.getLogger(__name__)


class BlobStorageClient:
    """Azure Blob Storage client for storing files."""

    def __init__(self, settings: Settings):
        """Initialize blob storage client.
        
        Args:
            settings: Application settings containing Azure Storage configuration
        """
        self.settings = settings
        self.credential = DefaultAzureCredential()
        self._client: Optional[BlobServiceClient] = None

    def _get_client(self, account_url: Optional[str] = None) -> BlobServiceClient:
        """
        Get or create BlobServiceClient instance.
        
        Args:
            account_url: Optional storage account URL (overrides settings)
            
        Returns:
            BlobServiceClient instance
            
        Raises:
            ValueError: If neither connection string nor account URL is configured
        """
        # Use provided URL or fall back to settings
        storage_url = account_url or self.settings.azure_storage_account_url
        
        # Prefer connection string if available
        if self.settings.azure_storage_connection_string:
            logger.debug("Using connection string for blob storage")
            return BlobServiceClient.from_connection_string(
                self.settings.azure_storage_connection_string
            )
        
        # Use account URL with credential
        if storage_url:
            logger.debug(f"Using account URL with credential: {storage_url}")
            return BlobServiceClient(
                account_url=storage_url,
                credential=self.credential
            )
        
        raise ValueError(
            "Azure Storage configuration required. "
            "Set either 'azure_storage_connection_string' or 'azure_storage_account_url' in settings."
        )

    async def upload_blob(
        self,
        container_name: str,
        blob_name: str,
        data: bytes,
        account_url: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Upload blob to Azure Storage.
        
        Args:
            container_name: Container name (defaults to settings.azure_storage_container_name)
            blob_name: Blob name
            data: Blob data as bytes
            account_url: Storage account URL (optional, overrides settings)
            content_type: Content type (e.g., 'image/png', 'text/html')
            
        Returns:
            URL to the uploaded blob
            
        Raises:
            ValueError: If storage configuration is missing
            AzureError: If upload fails
        """
        if not data:
            raise ValueError("Blob data cannot be empty")
        
        # Use default container from settings if not provided
        if not container_name:
            container_name = self.settings.azure_storage_container_name or "charts"
        
        try:
            client = self._get_client(account_url)
               
            # Get blob client and upload
            blob_client = client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            # Upload with content type if provided
            upload_kwargs = {"data": data}
            if content_type:
                upload_kwargs["content_settings"] = {"content_type": content_type}
            
            logger.debug(f"Uploading blob '{blob_name}' to container '{container_name}'")
            await blob_client.upload_blob(**upload_kwargs, overwrite=True)
            
            # Generate and return URL
            blob_url = blob_client.url
            logger.info(f"Blob uploaded successfully: {blob_url}")
            return blob_url
            
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            raise
        except AzureError as e:
            logger.error(f"Azure Storage upload error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading blob: {e}", exc_info=True)
            raise

    async def get_blob_url(
        self,
        container_name: str,
        blob_name: str,
        account_url: Optional[str] = None,
    ) -> str:
        """
        Get URL for a blob (without checking if it exists).
        
        Args:
            container_name: Container name (defaults to settings.azure_storage_container_name)
            blob_name: Blob name
            account_url: Storage account URL (optional, overrides settings)
            
        Returns:
            Blob URL
            
        Raises:
            ValueError: If storage configuration is missing
        """
        # Use default container from settings if not provided
        if not container_name:
            container_name = self.settings.azure_storage_container_name or "charts"
        
        try:
            client = self._get_client(account_url)
            blob_client = client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            return blob_client.url
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error generating blob URL: {e}", exc_info=True)
            raise

    async def blob_exists(
        self,
        container_name: str,
        blob_name: str,
        account_url: Optional[str] = None,
    ) -> bool:
        """
        Check if a blob exists.
        
        Args:
            container_name: Container name
            blob_name: Blob name
            account_url: Storage account URL (optional)
            
        Returns:
            True if blob exists, False otherwise
        """
        try:
            client = self._get_client(account_url)
            blob_client = client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            await blob_client.get_blob_properties()
            return True
        except AzureError:
            return False
        except Exception as e:
            logger.error(f"Error checking blob existence: {e}", exc_info=True)
            return False

    async def delete_blob(
        self,
        container_name: str,
        blob_name: str,
        account_url: Optional[str] = None,
    ) -> bool:
        """
        Delete a blob.
        
        Args:
            container_name: Container name
            blob_name: Blob name
            account_url: Storage account URL (optional)
            
        Returns:
            True if deleted, False if blob doesn't exist
        """
        try:
            client = self._get_client(account_url)
            blob_client = client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            await blob_client.delete_blob()
            logger.info(f"Blob '{blob_name}' deleted from container '{container_name}'")
            return True
        except AzureError as e:
            if "BlobNotFound" in str(e):
                logger.warning(f"Blob '{blob_name}' not found in container '{container_name}'")
                return False
            logger.error(f"Error deleting blob: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting blob: {e}", exc_info=True)
            raise
    
    async def close(self):
        """Close the blob storage client."""
        if self._client:
            await self._client.close()
            self._client = None
        if self.credential:
            await self.credential.close()
            self.credential = None
