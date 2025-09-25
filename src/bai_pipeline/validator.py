"""
Validates transformed data rows against their target BigQuery schema.
"""

import logging
from typing import List, Dict
from bai_pipeline import config_loader

logger = logging.getLogger(__name__)


def validate_rows(rows: List[Dict], config: Dict):
    """Validates rows against schema requirements, checking for required fields."""
    logger.info(f"Validating {len(rows)} transformed rows...")
    for idx, row in enumerate(rows):
        table_name = row.get("_target_table")
        if not table_name:
            raise ValueError(f"Row {idx} is missing the '_target_table' metadata key.")
        schema = config_loader.get_table_schema(config, table_name)
        required_fields = {col["name"] for col in schema if col.get("required", False)}
        missing_fields = [f for f in required_fields if row.get(f) in (None, "")]
        if missing_fields:
            raise ValueError(
                f"Validation failed for row {idx} (target: {table_name}). "
                f"Missing required fields: {', '.join(missing_fields)}"
            )
    logger.info("All rows passed validation.")
