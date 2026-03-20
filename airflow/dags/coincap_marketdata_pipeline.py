import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.extract import CoincapExtractor
from src.transform import CryptoTransformer
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


K8S_CONFIG_WORKER = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            node_selector={"role": "worker"},
            tolerations=[
                k8s.V1Toleration(
                    key="workload",
                    operator="Equal",
                    value="heavy",
                    effect="NoSchedule"
                )
            ],
            containers=[]
        )
    )
}

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "email": ["[EMAIL_ADDRESS]"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="coincap_marketdata_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["coincap", "marketdata", "etl"],
    default_args=default_args,
)   

def coincap_marketdata_pipeline():
    """Main pipeline function."""

    @task(executor_config=K8S_CONFIG_WORKER)
    def extract_task():
        """Extracts data from the Coincap API."""
        api_key = os.getenv('COINCAP_API_KEY')
        file_path = CoincapExtractor(api_key=api_key).extract()

        return str(file_path)

    @task(executor_config=K8S_CONFIG_WORKER)
    def transform_task(raw_file_path: str):
        """Transforms the extracted data."""
        transformer = CryptoTransformer()
        transformed_path = transformer.transform(raw_file_path)
        return transformed_path

    dbt_run = KubernetesPodOperator(
        task_id="dbt_run_task",
        name="dbt-run-pod",
        image="crypto-dbt:latest",
        image_pull_policy="IfNotPresent",
        pod_template_file="/opt/airflow/k8s/pod_template.yaml",
        cmds=["dbt", "run"],
        arguments=[
            "--project-dir", "/app",
            "--profiles-dir", "/app"
        ],
        node_selector={"role": "worker"},
        tolerations=[
            k8s.V1Toleration(
                key="workload",
                operator="Equal",
                value="heavy",
                effect="NoSchedule"
            )
        ],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="secrets-local")
            )
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        executor_config=K8S_CONFIG_WORKER
    )
    
    path_to_raw = extract_task()
    path_to_transformed = transform_task(path_to_raw)
    path_to_transformed >> dbt_run

coincap_marketdata_pipeline()