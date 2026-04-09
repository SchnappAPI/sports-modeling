"""
storage_pandas.py - shared helpers for writing Parquet to Azure Blob Storage.
For use with pandas DataFrames (MLB backfill scripts).
Container is set via AZURE_STORAGE_CONTAINER env var.
"""
import io, json, logging, os
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

ACCOUNT   = os.environ["AZURE_STORAGE_ACCOUNT"]
KEY       = os.environ["AZURE_STORAGE_KEY"]
CONTAINER = os.environ["AZURE_STORAGE_CONTAINER"]

def _client():
    return BlobServiceClient(account_url=f"https://{ACCOUNT}.blob.core.windows.net", credential=KEY)

def upload_parquet(df: pd.DataFrame, blob_path: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    _client().get_blob_client(container=CONTAINER, blob=blob_path).upload_blob(buf, overwrite=True)
    logging.info(f"Uploaded {len(df)} rows -> {blob_path}")

def checkpoint_exists(key: str) -> bool:
    try:
        _client().get_blob_client(container=CONTAINER, blob=f"_checkpoints/{key}.done").get_blob_properties()
        return True
    except Exception:
        return False

def mark_checkpoint(key: str) -> None:
    _client().get_blob_client(container=CONTAINER, blob=f"_checkpoints/{key}.done").upload_blob(
        json.dumps({"completed_at": datetime.utcnow().isoformat()}), overwrite=True
    )

def log_error(source: str, key: str, error: str) -> None:
    blob_path = f"_errors/{source}_errors.jsonl"
    record = json.dumps({"key": key, "error": str(error), "logged_at": datetime.utcnow().isoformat()}) + "\n"
    blob = _client().get_blob_client(container=CONTAINER, blob=blob_path)
    try:
        existing = blob.download_blob().readall().decode()
    except Exception:
        existing = ""
    blob.upload_blob(existing + record, overwrite=True)
    logging.warning(f"SKIPPED {key}: {error}")
