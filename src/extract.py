import os
import requests
from abc import ABC, abstractmethod
from typing import Optional, Dict
from pathlib import Path
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger("EXTRACT_COINCAP")

API_URL = os.getenv('COINCAP_API_URL')
BASE_DIR = Path(__file__).resolve().parent.parent 
RAW_DATA_DIR = BASE_DIR / "data" / "raw"


class BaseExtractor(ABC):
    """Abstract class for extractors."""
    @abstractmethod
    def extract(self) -> Path:
        pass

class CoincapExtractor(BaseExtractor):
    """
    Extractor for Coincap API.

    This class is responsible for the connection, directory management and 
    download of financial assets in JSON format.

    Attributes:
        api_key (Optional[str]): API key to increase rate limits.
        limit (int): Max ammount of assets to obtain per request.
    """
    def __init__(self, api_key: Optional[str] = None, limit: int = 100):
        self.api_key = api_key
        self.limit = limit
        self.url = API_URL
        self._setup_directories()

    def _setup_directories(self) -> None:
        """Create directories."""
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept-Encoding": "gzip", "Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def extract(self) -> Path:
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

            logger.info(f"Successful extraction. File saved to: {file_path}")
            return file_path

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP Error: {http_err} at {self.url}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

if __name__ == "__main__":
    extractor = CoincapExtractor(api_key=os.getenv('COINCAP_API_KEY'))
    path = extractor.extract()