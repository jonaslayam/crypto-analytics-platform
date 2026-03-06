import os
import requests
from abc import ABC, abstractmethod
from typing import Dict
from pathlib import Path
from datetime import datetime
from src.utils.logger import get_logger
from src.utils.oci_client import get_object_storage_client
from dotenv import load_dotenv

load_dotenv()

logger = get_logger("EXTRACT_COINCAP")

API_URL = os.getenv('COINCAP_API_URL')
BASE_DIR = Path(__file__).resolve().parent.parent 
RAW_DATA_DIR = BASE_DIR / "data" / "raw"


class BaseExtractor(ABC):
    """Abstract class for extractors."""
    @abstractmethod
    def extract(self) -> str:
        pass

class CoincapExtractor(BaseExtractor):
    """
    Extractor for Coincap API.

    This class is responsible for the connection, directory management and 
    download of financial assets in JSON format.

    Attributes:
        api_key (str): API key to increase rate limits.
        limit (int): Maximum number of assets to obtain per request.
    """
    def __init__(self, api_key: str, limit: int = 100):
        self.api_key = api_key
        self.limit = limit
        self.url = API_URL
        self.namespace = os.getenv("OCI_NAMESPACE")
        self.bucket_name = os.getenv("OCI_BUCKET_NAME")
        self._setup_directories()

    def _setup_directories(self) -> None:
        """Create directories."""
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept-Encoding": "gzip", "Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def extract(self) -> str:
        """
        Executes the extraction with streaming logic for memory efficiency.
        """
        params = {"limit": self.limit}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = RAW_DATA_DIR / f"assets_{timestamp}.json"

        logger.info(f"Starting extraction from {self.url}...")

        try:
            with requests.get(
                self.url, 
                headers=self._get_headers(), 
                params=params, 
                timeout=30,
                stream=True
            ) as response:
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk: # Filter out keep-alive chunks
                            f.write(chunk)
            
            now = datetime.now()
            partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
            object_name = f"raw/{partition_path}/{file_path.name}"
            client = get_object_storage_client()

            logger.info(f"Uploading {file_path.name} to OCI Bucket: {self.bucket_name}...")
            with open(file_path, "rb") as f:
                client.put_object(
                    namespace_name=self.namespace,
                    bucket_name=self.bucket_name,
                    object_name=object_name,
                    put_object_body=f
                )

            oci_uri = f"oci://{self.bucket_name}@{self.namespace}/{object_name}"
            logger.info(f"Upload successful. URI: {oci_uri}")
 
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP Error: {http_err} at {self.url}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
        else:
            if file_path.exists():
                file_path.unlink()
            return oci_uri
    
if __name__ == "__main__":
    extractor = CoincapExtractor(api_key=os.getenv('COINCAP_API_KEY'))
    path = extractor.extract()
    print(f"Extraction completed. File saved to: {path}")