"""
Handles data encryption using Google Cloud Key Management Service (KMS).
Provides an optimized KmsEncryptor class that caches customer-specific keys.
"""

import base64
import logging
from typing import Dict, List, Optional
from google.cloud import kms
from bai_pipeline import settings

logger = logging.getLogger(__name__)


class KmsEncryptor:
    """Handles encryption using KMS with key caching for performance."""

    def __init__(self):
        self.kms_client = kms.KeyManagementServiceClient()
        self._key_cache: Dict[str, str] = {}  # {customer_id: key_name}

    def _find_and_cache_key(self, customer_id: str) -> str:
        """Finds and caches the CMEK key for a customer to reduce API calls."""
        if customer_id in self._key_cache:
            return self._key_cache[customer_id]

        logger.info(f"Looking up KMS key for customer_id: {customer_id}")
        parent = f"projects/{settings.PROJECT_ID}/locations/{settings.LOCATION}/keyRings/{settings.KEY_RING}"

        try:
            keys = self.kms_client.list_crypto_keys(request={"parent": parent})
            for key in keys:
                if key.labels and key.labels.get("customer_id") == customer_id:
                    self._key_cache[customer_id] = key.name
                    logger.info(f"Found and cached key '{key.name}' for customer '{customer_id}'")
                    return key.name
        except Exception as e:
            logger.error(f"Failed to list KMS keys in '{parent}': {e}", exc_info=True)
            raise

        raise ValueError(f"No CMEK found with label customer_id={customer_id} in key ring {settings.KEY_RING}")

    def _encrypt_value(self, customer_id: str, plaintext: str) -> Optional[str]:
        """Encrypts a single plaintext value for a customer using KMS."""
        if plaintext is None:
            return None
        key_name = self._find_and_cache_key(customer_id)
        response = self.kms_client.encrypt(
            request={"name": key_name, "plaintext": str(plaintext).encode("utf-8")}
        )
        return base64.b64encode(response.ciphertext).decode("utf-8")

    def encrypt_row(self, row: Dict, sensitive_fields: List[str]) -> Dict:
        """Encrypts all sensitive fields within a row."""
        customer_id = row.get("customer_id")
        if not customer_id:
            raise ValueError("Row is missing 'customer_id' for encryption key lookup.")

        encrypted_row = row.copy()
        for field in sensitive_fields:
            if field in row and row[field] is not None:
                try:
                    encrypted_row[field] = self._encrypt_value(customer_id, row[field])
                except Exception as e:
                    logger.error(f"Encryption failed for field '{field}' for customer '{customer_id}'", exc_info=True)
                    raise

        # Remove the customer_id from the row before loading to BigQuery
        encrypted_row.pop("customer_id", None)
        return encrypted_row
