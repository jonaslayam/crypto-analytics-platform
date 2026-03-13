FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    dbt-core==1.10.0 \
    dbt-oracle==1.10.0 \
    oracledb==3.4.1

COPY dbt_project.yml .
COPY models ./models
COPY seeds ./seeds
COPY macros ./macros
COPY snapshots ./snapshots
COPY profiles.yml .

CMD ["dbt", "run"]