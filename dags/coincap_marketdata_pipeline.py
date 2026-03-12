import os
from datetime import timedelta
from airflow.decorators import dag, task
from src.extract import CoincapExtractor
from src.transform import CryptoTransformer

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
    schedule="0 */6 * * *",
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
        file_path = CoincapExtractor(api_key=api_key).extract()

        return str(file_path)

    @task
    def transform_task(raw_file_path: str):
        """Transforms the extracted data."""
        transformer = CryptoTransformer()
        transformed_path = transformer.transform(raw_file_path)
        return transformed_path

    @task
    def dbt_run_task(trigger_signal: str):
        """
        Executes dbt run to move data from the Silver layer to the Gold layer in ADW.
        Uses dynamic path discovery to remain portable across different environments.
        """
        import os
        import subprocess

        # Get the directory where the current DAG file is located
        # This assumes your DAG is in /crypto-analytics-platform/dags/
        dag_path = os.path.dirname(os.path.abspath(__file__))
        
        # Calculate the project root (going up one level from the dags folder)
        project_root = os.path.dirname(dag_path)
        
        # Define relative paths to the dbt project and profiles
        dbt_project_dir = os.path.join(project_root, "crypto_analytics")
        
        # Best practice: keep the profiles.yml in the root of your dbt project for portability
        profiles_dir = dbt_project_dir 

        print(f"Starting dbt run after successful upload: {trigger_signal}")
        print(f"Working Directory: {dbt_project_dir}")

        # Execute dbt run using the thin-mode adapter
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", profiles_dir],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"dbt Errors:\n{result.stderr}")
            raise Exception("dbt run failed. Please check the logs for transformation errors.")
        
        print(f"dbt Output:\n{result.stdout}")
        return "Gold Layer successfully updated"
    
    path_to_raw = extract_task()
    path_to_transformed = transform_task(path_to_raw)
    dbt_run_task(path_to_transformed)

coincap_marketdata_pipeline()