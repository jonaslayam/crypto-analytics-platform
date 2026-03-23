#!/bin/bash
set -e

# Ensure we are in the root
cd "$(dirname "$0")"

CLUSTER_NAME="crypto-etl-cluster"

echo "🚀 Building images locally..."
# Build for local architecture to avoid errors with --load
docker build -t airflow-crypto:latest ./airflow
docker build -t crypto-dbt:latest ./dbt

echo "📦 Importing images into k3d..."
k3d image import airflow-crypto:latest -c $CLUSTER_NAME
k3d image import crypto-dbt:latest -c $CLUSTER_NAME

echo "🔐 Applying secrets and ConfigMaps..."
# Ensure the .env paths are correct relative to where the script runs
cp airflow/.env k8s/secrets/airflow.env
cp dbt/.env k8s/secrets/dbt.env
kubectl apply -k k8s

echo "📄 Updating Pod Template ConfigMap..."
kubectl create configmap worker-pod-template \
  --from-file=pod_template.yaml=k8s/pod_template.yaml \
  -n airflow \
  --dry-run=client -o yaml | kubectl apply -f -

echo "☸️ Upgrading Airflow with Helm..."
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f k8s/airflow-config.yaml \
  --set images.airflow.repository=airflow-crypto \
  --set images.airflow.tag=latest \
  --timeout 10m

# Restart deployments to apply changes
echo "♻️ Restarting Airflow deployments..."
kubectl rollout restart deployment airflow-scheduler -n airflow
kubectl rollout restart deployment airflow-webserver -n airflow

echo "✅ Cluster update complete!"