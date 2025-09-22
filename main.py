import os
import json
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from bai_lib.bai2 import parse_from_file
from encrypt import encrypt_row


PROJECT_ID = "developmentenv-464809"
DATASET_ID = "Transactions"
INPUT_BAI_FILE = "CITI_hemanthkiran.bai" #bucket_name
MAPPING_CONFIG_FILE = "bq_mapping.json"
LOCATION = "global"
KEY_RING = "anz_encrypt"

SENSITIVE_FIELDS = ["account_number","closing_balance","opening_balance","transaction_amount"] 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_bank_and_customer_from_filename(filename: str):
    base = os.path.basename(filename).split(".")[0]
    parts = base.split("_")
    return parts[0], parts[1]

def load_config(config_file: str):
    with open(config_file, "r") as f:
        return json.load(f)


def apply_default_values(row, default_values):
    for key, val in default_values.items():
        if key not in row or row[key] is None:
            row[key] = val


def run_parser(input_file: str, config_file: str):
    bank_id, customer_id = get_bank_and_customer_from_filename(input_file)
    logger.info(f"Bank ID: {bank_id}")
    config = load_config(config_file)
    mappings = [m for m in config.get("mappings", []) if m.get("bank_id") == bank_id]
    if not mappings:
        logger.warning(f"No mappings found for bank_id={bank_id}, using default_typecodes.")
        mappings = config.get("default_typecodes", [])

    if not mappings:
        raise ValueError(f"No mappings found for bank_id={bank_id} and no default_typecodes provided.")

    code_map = {m["bai_code"]: m for m in mappings}
    default_values = config.get("default_values", {})
    # Parsing the file
    with open(input_file, "r") as f:
        bai_file = parse_from_file(f, check_integrity=True)

    rows = []
    for group_idx, group in enumerate(bai_file.children):
        group_date = group.header.as_of_date
        if not group_date:
            raise ValueError(f"Group {group_idx} missing as_of_date.")

        for account_idx, account in enumerate(group.children):
            account_header = account.header
            base_row = {
                "account_number": account_header.customer_account_number,
                "currency_code": account_header.currency or group.header.currency,
                "balance_date": group_date.isoformat(),
                "customer_id": customer_id,
            }
            apply_default_values(base_row, default_values)

            if account_header.summary_items:
                for summary in account_header.summary_items:
                    code = summary.type_code.code if summary.type_code else None
                    if code and code in code_map:
                        rule = code_map[code]
                        value = getattr(summary, rule["bai_field"], None)
                        row = base_row.copy()
                        row[rule["bq_column"]] = value
                        row["_target_table"] = rule["table"]
                        rows.append(row)

            
            if account.children:
                for tx in account.children:
                    for code, rule in code_map.items():
                        value = getattr(tx, rule["bai_field"], None)
                        if value is not None:
                            row = base_row.copy()
                            row[rule["bq_column"]] = value
                            row["_target_table"] = rule["table"]
                            rows.append(row)

    logger.info(f"Prepared {len(rows)} rows for BQ insert")
    return rows


def validate_rows(rows, config):
    required_map = config.get("validation_rules", {}).get("required_fields", [])

    for idx, row in enumerate(rows):
        missing = [f for f in required_map if f not in row or row[f] in (None, "")]
        if missing:
            raise ValueError(
                f"Row {idx} is missing required fields: {missing}"
            )
    return True


def load_rows_to_bq(client, dataset_ref, rows):
    if not rows:
        logger.warning("No rows to load.")
        return
    tables = {}
    for row in rows:
        table_name = row.pop("_target_table", "org")
        tables.setdefault(table_name, []).append(row)

    for table_name, table_rows in tables.items():
        table_ref = dataset_ref.table(table_name)
        try:
            client.get_table(table_ref)
        except NotFound:
            raise RuntimeError(f"Table {table_name} not found in dataset {dataset_ref.dataset_id}")

        logger.info(f"Loading {len(table_rows)} rows into {table_name}")
        errors = client.insert_rows_json(table_ref, table_rows)
        if errors:
            for err in errors:
                logger.error(f"Row insert error: {err}")
            raise RuntimeError("BigQuery load failed.")


def main():
    rows = run_parser(INPUT_BAI_FILE, MAPPING_CONFIG_FILE)
    config = load_config(MAPPING_CONFIG_FILE)
    validate_rows(rows, config)
    encrypted_rows = [
        encrypt_row(PROJECT_ID, LOCATION, KEY_RING, row, SENSITIVE_FIELDS)
        for row in rows
    ]

    bq_client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bq_client.dataset(DATASET_ID)
    load_rows_to_bq(bq_client, dataset_ref, encrypted_rows)
    logger.info("BAI2 processing completed.")


if __name__ == "__main__":
    main()

