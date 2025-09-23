import base64
from google.cloud import kms

kms_client = kms.KeyManagementServiceClient()

def find_key_for_customer(project_id: str, location: str, key_ring: str, customer_id: str) -> str:
    parent = f"projects/{project_id}/locations/{location}/keyRings/{key_ring}"
    keys = kms_client.list_crypto_keys(request={"parent": parent})
    for key in keys:
        labels = key.labels or {}
        if labels.get("customer_id") == customer_id:
            return key.name
    raise ValueError(f"No CMEK found with label customer_id={customer_id}")


def encrypt_value(project_id: str, location: str, key_ring: str, customer_id: str, plaintext: str) -> dict:
    if plaintext is None:
        return {"ciphertext": None, "key_name": None}
    key_name = find_key_for_customer(project_id, location, key_ring, customer_id)
    response = kms_client.encrypt(
        request={
            "name": key_name,
            "plaintext": plaintext.encode("utf-8")
        }
    )
    return {
        "ciphertext": base64.b64encode(response.ciphertext).decode("utf-8")
    }


def encrypt_row(project_id: str, location: str, key_ring: str, row: dict, sensitive_fields: list) -> dict:
    customer_id = row.get("customer_id")
    if not customer_id:
        raise ValueError("Row missing 'customer_id' for encryption.")
    encrypted_row = row.copy()
    encrypted_row.pop("customer_id", None)
    for field in sensitive_fields:
        if field in row and row[field]:
            encrypted_value = encrypt_value(
                project_id, location, key_ring, customer_id, str(row[field])
            )
            encrypted_row[field] = encrypted_value["ciphertext"]
    return encrypted_row
