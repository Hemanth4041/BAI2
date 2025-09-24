import os
import io
import json
import logging
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from bai_lib.bai2 import parse_from_file
from encrypt import encrypt_row


PROJECT_ID = "developmentenv-464809"
DATASET_ID = "Transactions"
INPUT_BAI_FILE = "bai_data/bai/CITI_hemanth.bai"
MAPPING_CONFIG_FILE = "bq_mapping.json"
LOCATION = "global"
KEY_RING = "anz_encrypt"
BALANCE_TABLE = "balance"
TRANSACTIONS_TABLE = "transactions"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_bank_and_customer_from_filename(filename: str):
    base = os.path.basename(filename).split(".")[0]
    parts = base.split("_")
    return parts[0], parts[1]

def load_config(config_file: str):
    with open(config_file, "r") as f:
        return json.load(f)

def apply_default_values(row, schema):
    for col in schema:
        if "default_value" in col and row.get(col["name"]) is None:
            row[col["name"]] = col["default_value"]

def get_sensitive_fields(schema):
    return [col["name"] for col in schema if col.get("sensitive")]

def get_schema_for_table(config, table_name):
    common_fields = config.get("common_fields_schema", [])
    table_fields = []
    if table_name == "balance":
        table_fields = config.get("balance_table_schema", [])
    elif table_name == "transactions":
        table_fields = config.get("transactions_table_schema", [])
    return common_fields + table_fields

def read_file_from_gcs(gcs_path: str) -> str:
    if "/" not in gcs_path:
        raise ValueError("Invalid GCS path format")
    bucket_name, blob_name = gcs_path.split("/", 1)
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text()


def create_base_row(customer_id, account_header, group_date, table_type):
    base_row = {
        "organisation_biz_id": customer_id,
        "customer_id": customer_id,
        "division_biz_id": customer_id,
        "account_number": account_header.customer_account_number,
        "balance_date": group_date.isoformat(),
        "account_name": None,
    }
    if table_type == "balance":
        base_row.update({
            "currency": account_header.currency or " ",
            "bsb": "",
            "financial_institute": "",
        })
    elif table_type == "transactions":
        base_row.update({
            "currency_code": account_header.currency or " ",
        })
    return base_row

def parse_bai_file(input_file, config_file):
    bank_id, customer_id = get_bank_and_customer_from_filename(input_file)
    logger.info(f"Bank ID: {bank_id}, Customer ID: {customer_id}")
    config = load_config(config_file)

    bank_config = next((m for m in config.get("mappings", []) if m.get("bank_id") == bank_id), None)
    mappings = bank_config.get("mappings", []) if bank_config else config.get("bank_id_default_typecodes", [])
    code_map = {m["bai_code"]: m for m in mappings}


    bai_text = read_file_from_gcs(input_file)
    bai_file = parse_from_file(io.StringIO(bai_text), check_integrity=True)

    balance_rows = []
    transaction_rows = []

    for group in bai_file.children:
        group_date = group.header.as_of_date

        for account in group.children:
            account_header = account.header
            balance_row = create_base_row(customer_id, account_header, group_date, "balance")
            balance_schema = get_schema_for_table(config, "balance")
            apply_default_values(balance_row, balance_schema)

            if account_header.summary_items:
                for summary in account_header.summary_items:
                    code = summary.type_code.code if summary.type_code else None
                    if code and code in code_map and code_map[code]["table"] == "balance":
                        bq_col = code_map[code]["bq_column"]
                        value = getattr(summary, code_map[code]["bai_field"], None)
                        balance_row[bq_col] = value

            balance_row["_target_table"] = BALANCE_TABLE
            balance_rows.append(balance_row)
            for tx in getattr(account, "children", []):
                tx_row = create_base_row(customer_id, account_header, group_date, "transactions")
                tx_schema = get_schema_for_table(config, "transactions")
                apply_default_values(tx_row, tx_schema)
                for code, rule in code_map.items():
                    if rule["table"] == "transactions":
                        value = getattr(tx, rule["bai_field"], None)
                        if value is not None:
                            tx_row[rule["bq_column"]] = value
                            if rule["bq_column"] == "transaction_amount":
                                tx_row["debit_credit_indicator"] = "D" if tx.type_code.transaction.value == "debit" else "C"

                tx_row["transaction_posting_date"] = getattr(tx, "posting_date", group_date).isoformat()
                tx_row["transaction_value_date"] = getattr(tx, "value_date", group_date).isoformat()
                tx_row["_target_table"] = TRANSACTIONS_TABLE
                transaction_rows.append(tx_row)

    return balance_rows, transaction_rows, config


def validate_rows(rows, config):
    for idx, row in enumerate(rows):
        table_name = row.get("_target_table", BALANCE_TABLE)
        schema = get_schema_for_table(config, table_name)
        required_fields = [col["name"] for col in schema if col.get("required")]
        missing = [f for f in required_fields if f not in row or row[f] in (None, "")]
        if missing:
            raise ValueError(f"Row {idx} missing required fields: {missing}")
    return True

def get_all_sensitive_fields(config):
    sensitive_fields = set()
    for table in ["common_fields_schema", "balance_table_schema", "transactions_table_schema"]:
        sensitive_fields.update(get_sensitive_fields(config.get(table, [])))
    return list(sensitive_fields)

def load_rows_to_bq(client, dataset_ref, rows):
    if not rows:
        logger.warning("No rows to load.")
        return

    tables = {}
    for row in rows:
        table_name = row.pop("_target_table", BALANCE_TABLE)
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
    balance_rows, transaction_rows, config = parse_bai_file(INPUT_BAI_FILE, MAPPING_CONFIG_FILE)

    all_rows = balance_rows + transaction_rows

    validate_rows(all_rows, config)

    sensitive_fields = get_all_sensitive_fields(config)
    encrypted_rows = [encrypt_row(PROJECT_ID, LOCATION, KEY_RING, row, sensitive_fields) for row in all_rows]

    bq_client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bq_client.dataset(DATASET_ID)
    load_rows_to_bq(bq_client, dataset_ref, encrypted_rows)

    logger.info("BAI2 processing completed.")

if __name__ == "__main__":
    main()
