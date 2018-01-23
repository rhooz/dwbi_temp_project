#!/usr/bin/env bash
export PROJECT_ROOT=`git rev-parse --show-toplevel`
kubectl delete secret airflow-config --namespace=i2ap
kubectl create secret generic airflow-config --namespace=i2ap --from-file=airflow.cfg="$PROJECT_ROOT/docker/i2ap-airflow/airflow.cfg"