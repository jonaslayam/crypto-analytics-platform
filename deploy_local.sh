#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Always run from repo root
cd "$(dirname "$0")"

echo "Starting Crypto ETL Cluster deployment..."

# 1. Cluster Cleanup and Creation
echo "Recreating k3d cluster..."
k3d cluster delete crypto-etl-cluster || true
k3d cluster create crypto-etl-cluster \
  --servers 1 \
  --agents 1 \
  --k3s-node-label "role=core@server:0" \
  --k3s-node-label "role=worker@agent:0" \
  --port "8080:8080@server:0" \
  --port "3000:3000@server:0" \
  --runtime-label "com.docker.cpus=3.0@server:0" \
  --runtime-label "com.docker.cpus=1.0@agent:0"

# 2. Node Configuration (Taints)
echo "Applying taints to worker node..."
kubectl taint nodes k3d-crypto-etl-cluster-agent-0 workload=heavy:NoSchedule

# 3. Namespace and Image Preparation
echo "Preparing Kubernetes namespace..."
kubectl create namespace airflow || true

echo "Building ARM64 image locally..."
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t airflow-crypto:latest \
  --load ./airflow

echo "Building DBT image..."
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t crypto-dbt:latest \
  --load ./dbt

echo "Importing image into the cluster (this may take a moment)..."
k3d image import airflow-crypto:latest -c crypto-etl-cluster
k3d image import crypto-dbt:latest -c crypto-etl-cluster

# 4. Secrets and ConfigMaps (Infrastructure as Code)
echo "Applying secrets using Kustomize..."
cp airflow/.env k8s/secrets/airflow.env
cp dbt/.env k8s/secrets/dbt.env
kubectl apply -k k8s

echo "Loading Pod Template ConfigMap..."
# Remove existing ConfigMap to prevent duplicate errors
kubectl delete configmap worker-pod-template -n airflow || true
kubectl create configmap worker-pod-template \
  --from-file=pod_template.yaml=k8s/pod_template.yaml \
  -n airflow

# 5. Helm Deployment
echo "Installing Airflow with Helm..."
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  -f k8s/airflow-config.yaml \
  --set images.airflow.repository=airflow-crypto \
  --set images.airflow.tag=latest \
  --set config.kubernetes.pod_template_file=/opt/airflow/k8s/pod_template.yaml \
  --timeout 10m