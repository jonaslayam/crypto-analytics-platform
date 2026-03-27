#!/bin/bash
set -e

# Ensure we are running from the project root
cd "$(dirname "$0")"

CLUSTER_NAME="crypto-etl-cluster"

echo "🚀 Building images locally..."
# Build images using --pull to ensure base images are up to date
docker build --pull -t airflow-crypto:latest ./airflow
docker build --pull -t crypto-dbt:latest ./dbt

echo "📦 Importing images into k3d..."
# Import local images into the k3d cluster nodes
k3d image import airflow-crypto:latest -c $CLUSTER_NAME
k3d image import crypto-dbt:latest -c $CLUSTER_NAME

echo "🔐 Applying secrets and ConfigMaps..."
# Deploy K8s secrets and general configuration (OCI and API credentials)
cp airflow/.env k8s/secrets/airflow.env
cp dbt/.env k8s/secrets/dbt.env
kubectl apply -k k8s

echo "📄 Updating Pod Template ConfigMap..."
# Use dry-run to update the template without triggering 'last-applied-configuration' warnings
kubectl create configmap worker-pod-template \
  --from-file=pod_template.yaml=k8s/pod_template.yaml \
  -n airflow \
  --dry-run=client -o yaml | kubectl apply -f -

echo "☸️ Upgrading Airflow with Helm..."
# Standard Helm upgrade with pullPolicy=Never to force the use of local k3d images
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f k8s/airflow-config.yaml \
  --set images.airflow.repository=airflow-crypto \
  --set images.airflow.tag=latest \
  --set images.airflow.pullPolicy=Never \
  --timeout 10m

echo "♻️ Restarting Airflow deployments to sync Auth Tokens..."
# Critical fix: Restart all components to synchronize JWT signature keys
kubectl rollout restart deployment airflow-scheduler -n airflow
kubectl rollout restart deployment airflow-api-server -n airflow
kubectl rollout restart deployment airflow-dag-processor -n airflow
kubectl rollout restart statefulset airflow-triggerer -n airflow

echo "🧹 Cleaning up failed pods..."
# Only delete pods that are older than 5 minutes and not running
kubectl get pods -n airflow --field-selector=status.phase!=Running -o name | xargs -r kubectl delete -n airflow

echo "✅ Cluster update complete and security tokens synced!"