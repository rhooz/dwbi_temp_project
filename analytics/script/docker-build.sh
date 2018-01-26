#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT=`git rev-parse --show-toplevel`
PROJECT_ROOT=$PROJECT_ROOT"/analytics"
POD_PATCH_VERSION='0'
SERVICE_NAME=''

# arg1: SERVICE (optional)
if [[ $# -eq 1 ]] ; then
    SERVICE_NAME=$1
else
    echo "$0: Need to specify the Service name. Exiting"
    exit 1
fi

POD_MAJOR_MINOR_VERSION=`cat ${PROJECT_ROOT}/script/config/${SERVICE_NAME}-version.txt`
POD_PATCH_VERSION=`gcloud container images list-tags us.gcr.io/tt-cust-analytics/i2ap-${SERVICE_NAME} --limit=1 |cut -d' ' -f3|tail -1|cut -d'.' -f3`

if [[ $POD_PATCH_VERSION == "Listed 0 items." ||  $POD_PATCH_VERSION == '' ]]
then
    POD_PATCH_VERSION='0'
else
    POD_PATCH_VERSION=$((POD_PATCH_VERSION+1))
fi
POD_VERSION=${POD_MAJOR_MINOR_VERSION}"."${POD_PATCH_VERSION}

echo ${POD_VERSION} > $PROJECT_ROOT/script/config/latest-${SERVICE_NAME}-version

echo "POD_VERSION="${POD_VERSION}
echo "Building the base image locally"
docker build -t us.gcr.io/tt-cust-analytics/i2ap-${SERVICE_NAME}:${POD_VERSION} .
#echo "Pushing base image to project container repository"
#gcloud docker -- push us.gcr.io/tt-cust-analytics/i2ap-${SERVICE_NAME}:${POD_VERSION}
