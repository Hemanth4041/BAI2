"""
Centralized configuration management for the BAI pipeline.
Loads settings from environment variables with sensible defaults.
"""

import os

# GCP Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "developmentenv-464809")
LOCATION = os.environ.get("GCP_LOCATION", "global")

# BigQuery Configuration
DATASET_ID = os.environ.get("BQ_DATASET_ID", "Transactions")
BALANCE_TABLE_ID = os.environ.get("BQ_BALANCE_TABLE_ID", "balance")
TRANSACTIONS_TABLE_ID = os.environ.get("BQ_TRANSACTIONS_TABLE_ID", "transactions")

# KMS Configuration
KEY_RING = os.environ.get("KMS_KEY_RING", "anz_encrypt")

# File Paths
MAPPING_CONFIG_PATH = os.environ.get("MAPPING_CONFIG_PATH", "config/bq_mappings.json")
