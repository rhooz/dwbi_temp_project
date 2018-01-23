# service implementation of i2ap airflow
FROM docker.io/tectoniccloud/i2ap-airflow-base:1.0.16
MAINTAINER Tectonic

# deploy the latest dags
COPY i2ap-airflow.tar.gz /usr/local/src/airflow/i2ap-airflow.tar.gz
RUN cd /usr/local/src/airflow; tar -xvf i2ap-airflow.tar.gz
