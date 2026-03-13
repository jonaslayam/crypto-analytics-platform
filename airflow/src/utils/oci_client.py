import os
from dotenv import load_dotenv
from src.utils.logger import get_logger
from oci.object_storage import ObjectStorageClient

load_dotenv()

logger = get_logger("OCI_CLIENT")

def get_oci_client():
    """
    Create an OCI Object Storage client using environment variables.
    """
    config = {
        "user": os.getenv("OCI_USER_OCID"),
        "key_content": os.getenv("OCI_PRIVATE_KEY_CONTENT"),
        "fingerprint": os.getenv("OCI_FINGERPRINT"),
        "tenancy": os.getenv("OCI_TENANCY_OCID"),
        "region": os.getenv("OCI_REGION"),
        "namespace": os.getenv("OCI_NAMESPACE")
    }
    
    # Validate that no required variable is missing
    for key, value in config.items():
        if not value:
            logger.error(f"Missing environment variable: {key}")
            raise ValueError(f"Missing environment variable: {key}")

    return config

def get_object_storage_client() -> ObjectStorageClient:
    """
    Creates and returns the Object Storage client.
    """
    config = get_oci_client()
    return ObjectStorageClient(config)