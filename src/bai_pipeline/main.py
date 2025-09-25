"""
Main orchestrator for the BAI2 to BigQuery pipeline.
Configures logging and defines the main execution flow.
"""

import os
import io
import logging
import sys
import argparse
from bai_parser import parse_from_file
from bai_pipeline import settings, config_loader, transformer, validator
from gcp import storage, kms, bigquery

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def _get_identifiers_from_filename(filename: str) -> tuple[str, str]:
    """Extracts bank_id and customer_id from a filename like 'BANK_CUSTOMER.txt'."""
    base = os.path.basename(filename).split(".")[0]
    parts = base.split("_")
    if len(parts) < 2:
        raise ValueError(f"Filename '{filename}' does not match 'BANKID_CUSTOMERID' format.")
    return parts[0], parts[1]


def run(input_gcs_path: str):
    """Executes the full BAI2 processing pipeline from start to finish."""
    logger.info(f"--- Starting BAI2 pipeline for file: {input_gcs_path} ---")
    try:
        # 1. Load Config
        config = config_loader.load_config(settings.MAPPING_CONFIG_PATH)
        bank_id, customer_id = _get_identifiers_from_filename(input_gcs_path)
        logger.info(f"Identified Bank ID: '{bank_id}', Customer ID: '{customer_id}'")

        # 2. Read and Parse
        bai_text = storage.read_file_from_gcs(input_gcs_path)
        bai_file = parse_from_file(io.StringIO(bai_text), check_integrity=True)

        # 3. Transform
        bank_mappings = config_loader.get_bank_mappings(config, bank_id)
        transformed_rows = transformer.transform_bai_to_rows(bai_file, customer_id, config, bank_mappings)
        if not transformed_rows:
            logger.warning("Transformation resulted in zero rows. Pipeline finished successfully.")
            return

        # 4. Validate
        validator.validate_rows(transformed_rows, config)

        # 5. Encrypt
        sensitive_fields = config_loader.get_all_sensitive_fields(config)
        encryptor = kms.KmsEncryptor()
        logger.info(f"Encrypting {len(sensitive_fields)} sensitive fields: {sensitive_fields}")
        encrypted_rows = [encryptor.encrypt_row(row, sensitive_fields) for row in transformed_rows]

        # 6. Load to BigQuery
        bigquery.load_rows_to_bq(encrypted_rows)
        logger.info("--- BAI2 processing pipeline completed successfully. ---")

    except Exception as e:
        logger.error(f"--- Pipeline failed with a critical error: {e} ---", exc_info=True)
        raise


def run_cli():
    """Parses command-line arguments and triggers the pipeline."""
    parser = argparse.ArgumentParser(description="Run the BAI2 to BigQuery processing pipeline.")
    parser.add_argument(
        "input_file",
        type=str,
        help="The full GCS path to the input BAI file (e.g., 'my-bucket/data/CITI_customer.bai')."
    )
    args = parser.parse_args()
    try:
        run(args.input_file)
    except Exception:
        sys.exit(1)
