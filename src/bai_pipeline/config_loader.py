"""
Configuration loader for the BAI pipeline.
Reads bq_mappings.json and provides structured access to schemas and mappings.
"""

import json
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """Loads the JSON mapping configuration from a file."""
    logger.info(f"Loading mapping configuration from: {config_path}")
    with open(config_path, "r") as f:
        return json.load(f)


def get_table_schema(config: Dict, table_name: str) -> List[Dict]:
    """Constructs the full schema for a table by combining common and specific fields."""
    common_fields = config.get("common_fields_schema", [])
    table_specific_fields = config.get(f"{table_name}_table_schema", [])
    if not table_specific_fields:
        raise ValueError(f"Schema definition not found for table: {table_name}")
    return common_fields + table_specific_fields


def get_all_sensitive_fields(config: Dict) -> List[str]:
    """Gets a unique list of all sensitive fields across all schemas."""
    fields = set()
    for schema_key in ["common_fields_schema", "balance_table_schema", "transactions_table_schema"]:
        schema = config.get(schema_key, [])
        fields.update([col["name"] for col in schema if col.get("sensitive", False)])
    return list(fields)


def get_bank_mappings(config: Dict, bank_id: str) -> Dict[str, Dict]:
    """
    Retrieves BAI code mappings for a bank, falling back to defaults if not found.
    """
    bank_config = next((m for m in config.get("mappings", []) if m.get("bank_id") == bank_id), None)
    if bank_config:
        logger.info(f"Found specific mappings for bank_id: {bank_id}")
        mappings_list = bank_config.get("mappings", [])
    else:
        logger.warning(f"No specific mappings found for bank_id: {bank_id}. Using defaults.")
        mappings_list = config.get("bank_id_default_typecodes", [])
    if not mappings_list:
        raise ValueError(f"No mappings found for bank_id='{bank_id}' and no defaults defined.")
    return {m["bai_code"]: m for m in mappings_list}
