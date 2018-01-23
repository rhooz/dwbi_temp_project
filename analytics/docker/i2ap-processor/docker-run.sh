#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PORT=80
echo $SCRIPT_DIR

IMAGE_NAME=us.gcr.io/tt-cust-analytics/i2ap-processor

if [[ $# -eq 0 ]] ; then
    POD_VERSION=`docker images ${IMAGE_NAME} --format "{{.Tag}}"`
else
    POD_VERSION=$1
fi
echo "POD_VERSION detected as "$POD_VERSION

docker run --memory=1g \
-p ${PORT}:${PORT}  \
-v ${SCRIPT_DIR}/../../:/usr/local/src/mystuff \
-v ${SCRIPT_DIR}/../../docker:/secrets \
-v ${SCRIPT_DIR}/../../content:/css \
-v ${SCRIPT_DIR}/../../content:/template \
--env I2AP_PORT=3101 \
--env I2AP_OVERRIDE_PORT= \
--env I2AP_DDL_DIRECTORY=/usr/local/src/mystuff/ddl \
--env I2AP_SQL_DIRECTORY=/usr/local/src/mystuff/sql \
--env I2AP_LIB_DIRECTORY=/usr/local/src/mystuff/lib \
--env INTERACTIVE=true \
--env I2AP_MART_TYPE=oracle \
--env I2AP_MART_SERVER=127.0.0.1 \
--env I2AP_MART_PORT=5432 \
--env I2AP_MART_NAME=commix \
--env I2AP_MART_USER=postgres \
--env I2AP_MART_PASSWORD=T3ct0n1c \
--env I2AP_LOG_NAME=processing-log \
--env I2AP_SALESFORCE_USER=user@gettectonic.com \
--env I2AP_SALESFORCE_PASSWORD=sfpwd \
--env I2AP_SALESFORCE_TOKEN=sftoken \
--env I2AP_SALESFORCE_SANDBOX=False \
-t -i  \
--name i2ap-processor  \
--hostname i2ap-processor  \
${IMAGE_NAME}:$POD_VERSION