"""
Transforms parsed BAI2 data into structured rows for BigQuery.
"""

import logging
from typing import Dict, List, Any
from bai_parser.models import Bai2File
from bai_pipeline import settings, config_loader

logger = logging.getLogger(__name__)


def _apply_default_values(row: Dict, schema: List[Dict]):
    """Applies default values to a row based on its schema."""
    for col in schema:
        if "default_value" in col and row.get(col["name"]) is None:
            row[col["name"]] = col["default_value"]


def _create_base_row(customer_id: str, account_header: Any, group_date: Any, table_type: str) -> Dict:
    """Creates a base row with common fields."""
    base_row = {
        "organisation_biz_id": customer_id,
        "division_biz_id": customer_id,
        "account_number": account_header.customer_account_number,
        "customer_id": customer_id,  # For encryption key lookup
    }

    if table_type == "balance":
        base_row.update({
            "balance_date": group_date.isoformat(),
            "currency": account_header.currency or " ",
            "bsb": " ",
            "financial_institute": "",
            "_target_table": settings.BALANCE_TABLE_ID,
        })
    elif table_type == "transactions":
        base_row.update({
            "currency_code": account_header.currency or " ",
            "_target_table": settings.TRANSACTIONS_TABLE_ID,
        })

    return base_row


def transform_bai_to_rows(bai_file: Bai2File, customer_id: str, config: Dict, code_map: Dict) -> List[Dict]:
    all_rows = []
    balance_schema = config_loader.get_table_schema(config, "balance")
    tx_schema = config_loader.get_table_schema(config, "transactions")

    for group in bai_file.children:
        group_date = group.header.as_of_date
        if not group_date:
            logger.warning("Skipping a group because it is missing 'as_of_date'.")
            continue

        for account in group.children:
            # Balance Row
    
            balance_row = _create_base_row(customer_id, account.header, group_date, "balance")
            _apply_default_values(balance_row, balance_schema)
            for summary in account.header.summary_items or []:
                code = summary.type_code.code if summary.type_code else None
                if code and code in code_map and code_map[code]["table"] == "balance":
                    rule = code_map[code]
                    balance_row[rule["bq_column"]] = getattr(summary, rule["bai_field"], None)
            all_rows.append(balance_row)

            # Transaction Rows
            for tx in getattr(account, "children", []):
                tx_row = _create_base_row(customer_id, account.header, group_date, "transactions")
                _apply_default_values(tx_row, tx_schema)

                for code, rule in code_map.items():
                    if rule["table"] != "transactions":
                        continue
                    value = getattr(tx, rule["bai_field"], None)
                    if value is not None:
                        tx_row[rule["bq_column"]] = value
                        if rule["bq_column"] == "transaction_amount":
                            tx_row["debit_credit_indicator"] = "D" if tx.type_code.transaction.value == "debit" else "C"

                # Ensure required fields are present
                tx_row["transaction_posting_date"] = getattr(tx, "posting_date", group_date).isoformat()
                tx_row["transaction_value_date"] = getattr(tx, "value_date", group_date).isoformat()
                if "transaction_amount" not in tx_row or tx_row["transaction_amount"] is None:
                    tx_row["transaction_amount"] = 0
                    tx_row["debit_credit_indicator"] = "D"
                all_rows.append(tx_row)

    balance_count = sum(1 for r in all_rows if r["_target_table"] == settings.BALANCE_TABLE_ID)
    tx_count = sum(1 for r in all_rows if r["_target_table"] == settings.TRANSACTIONS_TABLE_ID)
    logger.info(f"Prepared {len(all_rows)} rows: {balance_count} balances, {tx_count} transactions")
    return all_rows
   
