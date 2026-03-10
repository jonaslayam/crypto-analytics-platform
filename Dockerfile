FROM apache/airflow:slim-3.1.7-python3.11

USER root

WORKDIR /opt/airflow

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ ./dags
COPY src/ ./src
COPY crypto_analytics/ ./crypto_analytics

ENV PYTHONPATH=/opt/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"
