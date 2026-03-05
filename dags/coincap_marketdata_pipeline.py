import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from src.extract import CoinCapExtractor

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["[EMAIL_ADDRESS]"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="coincap_marketdata_pipeline",
    start_date=None,
    schedule="*/5 * * * *",
    catchup=False,
    tags=["coincap", "marketdata", "etl"],
    default_args=default_args,
)

def coincap_marketdata_pipeline():
    """Main pipeline function."""
    @task
    def extract_task():
        """Extracts data from the Coincap API."""
        api_key = os.getenv('COINCAP_API_KEY')
        file_path = CoinCapExtractor(api_key=api_key).extract()

        return str(file_path)

    @task
    def transform_task(raw_file_path: str):
        """Transforms the extracted data."""
        pass

    @task
    def load_task(transformed_data: List[Dict[str, Any]]):
        """Loads the transformed data."""
        pass
    
    path_to_raw = extract_task()
    path_to_transformed = transform_task(path_to_raw)
    load_task(path_to_transformed)

coincap_marketdata_pipeline()