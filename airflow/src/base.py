from abc import ABC, abstractmethod

class BaseExtractor(ABC):
    """
    Interface for data extraction from external sources (APIs, Databases, etc.)
    """
    @abstractmethod
    def extract(self) -> str:
        """
        Executes the extraction process and returns the URI of the raw data.
        """
        raise NotImplementedError("Subclasses must implement the 'extract' method.")

class BaseTransformer(ABC):
    """
    Interface for data transformation (Cleaning, Aggregating, Refining).
    """
    @abstractmethod
    def transform(self, input_uri: str) -> str:
        """
        Processes data from an input URI and returns the URI of the refined data.
        """
        raise NotImplementedError("Subclasses must implement the 'transform' method.")