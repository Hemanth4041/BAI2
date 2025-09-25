"""
Provides functionality for interacting with Google Cloud Storage (GCS).
"""

import logging
from google.cloud import storage as gcs
from google.api_core.exceptions import NotFound
from bai_pipeline import settings

logger = logging.getLogger(__name__)


def read_file_from_gcs(gcs_path: str) -> str:
    """
    Downloads a file from GCS and returns its content as a string.

    Args:
        gcs_path: The GCS path in the format 'bucket_name/blob_name'.

    Returns:
        The text content of the file.
    """
    if "/" not in gcs_path:
        raise ValueError("Invalid GCS path format. Expected 'bucket_name/blob_name'.")
    
    bucket_name, blob_name = gcs_path.split("/", 1)
    logger.info(f"Reading file from GCS: gs://{bucket_name}/{blob_name}")

    try:
        client = gcs.Client(project=settings.PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text()
    except NotFound:
        logger.error(f"File not found at GCS path: gs://{gcs_path}")
        raise FileNotFoundError(f"File not found at GCS path: gs://{gcs_path}")
    except Exception as e:
        logger.error(f"Failed to read from GCS path gs://{gcs_path}: {e}", exc_info=True)
        raise
