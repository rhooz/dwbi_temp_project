#!/bin/bash
echo "Building the base image locally"
#docker build --no-cache -t us.gcr.io/tt-cust-analytics/i2ap-processor:1.0.22 .
docker build -t us.gcr.io/tt-cust-analytics/i2ap-processor:1.0.22 .

