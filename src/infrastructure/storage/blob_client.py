"""Azure Blob Storage client."""

import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from azure.core.exceptions import AzureError
from azure.storage.blob import BlobSasPermissions, ContentSettings, generate_blob_sas
from azure.storage.blob.aio import BlobServiceClient

from src.config.settings import Settings
from src.infrastructure.llm.factory import get_shared_credential

logger = logging.getLogger(__name__)


class BlobStorageClient:
    """Azure Blob Storage client for storing files."""

    def __init__(self, settings: Settings):
        """Initialize blob storage client.

        Args:
            settings: Application settings containing Azure Storage configuration
        """
        self.settings = settings
        self.credential = get_shared_credential()
        self._client: BlobServiceClient | None = None

    def _get_client(self, account_url: str | None = None) -> BlobServiceClient:
        """
        Get or create BlobServiceClient instance (reuses existing client if available).

        Args:
            account_url: Optional storage account URL (overrides settings)

        Returns:
            BlobServiceClient instance

        Raises:
            ValueError: If neither connection string nor account URL is configured
        """
        # Reuse existing client if available
        if self._client is not None:
            return self._client

        # Use provided URL or fall back to settings
        storage_url = account_url or self.settings.azure_storage_account_url

        # Prefer connection string if available
        if self.settings.azure_storage_connection_string:
            logger.debug("Using connection string for blob storage")
            self._client = BlobServiceClient.from_connection_string(
                self.settings.azure_storage_connection_string
            )
            return self._client

        # Use account URL with credential
        if storage_url:
            logger.debug(f"Using account URL with credential: {storage_url}")
            self._client = BlobServiceClient(account_url=storage_url, credential=self.credential)
            return self._client

        raise ValueError(
            "Azure Storage configuration required. "
            "Set either 'azure_storage_connection_string' or 'azure_storage_account_url' in settings."
        )

    async def upload_blob(
        self,
        container_name: str,
        blob_name: str,
        data: bytes,
        account_url: str | None = None,
        content_type: str | None = None,
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
            blob_client = client.get_blob_client(container=container_name, blob=blob_name)

            logger.debug(f"Uploading blob '{blob_name}' to container '{container_name}'")

            if content_type:
                content_settings = ContentSettings(content_type=content_type)
                await blob_client.upload_blob(
                    data=data,
                    overwrite=True,
                    content_settings=content_settings,
                )
            else:
                await blob_client.upload_blob(
                    data=data,
                    overwrite=True,
                )

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
        account_url: str | None = None,
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
            blob_client = client.get_blob_client(container=container_name, blob=blob_name)
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
        account_url: str | None = None,
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
            blob_client = client.get_blob_client(container=container_name, blob=blob_name)
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
        account_url: str | None = None,
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
            blob_client = client.get_blob_client(container=container_name, blob=blob_name)
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

    async def get_blob_sas_url(
        self,
        container_name: str,
        blob_name: str,
        expiry_minutes: int = 60,
        account_url: str | None = None,
    ) -> str:
        """
        Generate a signed (SAS) URL for a blob that is valid temporarily.

        Args:
            container_name: Container name (defaults to settings.azure_storage_container_name)
            blob_name: Blob name
            expiry_minutes: Number of minutes until the SAS token expires (default: 60)
            account_url: Storage account URL (optional, overrides settings)

        Returns:
            Signed blob URL with SAS token

        Raises:
            ValueError: If storage configuration is missing
        """
        if not container_name:
            container_name = self.settings.azure_storage_container_name or "charts"

        try:
            client = self._get_client(account_url)
            blob_client = client.get_blob_client(container=container_name, blob=blob_name)
            account_name = client.account_name

            now = datetime.now(timezone.utc)
            expiry = now + timedelta(minutes=expiry_minutes)

            # Option A: Using Connection String
            if self.settings.azure_storage_connection_string:
                # Parse connection string to extract account key
                conn_str = self.settings.azure_storage_connection_string
                account_key = None
                for part in conn_str.split(";"):
                    if part.startswith("AccountKey="):
                        account_key = part.split("AccountKey=", 1)[1]
                        break

                if not account_key:
                    error_msg = "Could not extract account key from connection string. Cannot generate SAS token."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                sas_token = generate_blob_sas(
                    account_name=account_name,
                    container_name=container_name,
                    blob_name=blob_name,
                    account_key=account_key,
                    permission=BlobSasPermissions(read=True),
                    expiry=expiry,
                )
                return f"{blob_client.url}?{sas_token}"

            # Option B: Using Managed Identity (Production recommended)
            else:
                # Get user delegation key for Managed Identity
                user_delegation_key = await client.get_user_delegation_key(
                    key_start_time=now,
                    key_expiry_time=expiry,
                )

                sas_token = generate_blob_sas(
                    account_name=account_name,
                    container_name=container_name,
                    blob_name=blob_name,
                    user_delegation_key=user_delegation_key,
                    permission=BlobSasPermissions(read=True),
                    expiry=expiry,
                )
                return f"{blob_client.url}?{sas_token}"

        except Exception as e:
            logger.error(
                f"Error generating SAS token for blob {blob_name} in container {container_name}: {e}",
                exc_info=True,
            )
            # Do not return unsigned URL as it will fail with anonymous access disabled
            # Re-raise the exception so callers can handle it appropriately
            raise

    async def close(self) -> None:
        """Close the blob storage client."""
        if self._client:
            await self._client.close()
            self._client = None
        # Note: Do NOT close self.credential - it's a shared credential
        # The shared credential is managed by get_shared_credential() and
        # should only be closed during application shutdown
