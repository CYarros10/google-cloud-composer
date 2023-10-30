import yaml
from typing import Any
from typing import Dict
from google.cloud import storage

def local_config_from_local(path: str) -> Dict[str, Any]:
    """Loads and returns a local configuration file as a dictionary"""
    with open(path) as f:
        config = yaml.safe_load(f)
    return config

def load_config_from_gcs(bucket_name: str, source_blob_name: str) -> Dict[str, Any]:
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename("config.yaml")
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    return config
