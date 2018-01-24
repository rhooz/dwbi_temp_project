#!/bin/bash

declare -a validServices=("airflow" "listener" "processor")
DESIRED_SERVICE=''
# make sure it is executed from project root
PROJECT_ROOT=`git rev-parse --show-toplevel`
PROJECT_ROOT=$PROJECT_ROOT"/analytics"


#GCLOUD_PROJECT="tt-cust-"`basename $PROJECT_ROOT`
#GCLOUD_CLUSTER=$GCLOUD_PROJECT"-kube"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "PROJECT_ROOT = "$PROJECT_ROOT
echo "SCRIPT_DIR = "$SCRIPT_DIR

function createSourceTarBall ()
{
    echo "createSourceTarBall"
    cd ${PROJECT_ROOT}
    find_command_file="script/config/"${DESIRED_SERVICE}"-find.txt"
    if [ -f $find_command_file ]
    then
        echo "Building the source tarBall for upload to service "${DESIRED_SERVICE}"..."
        FIND_CMD=`cat $find_command_file`
        rm -f $PROJECT_ROOT/dist/i2ap-${DESIRED_SERVICE}.tar.gz $PROJECT_ROOT/docker/i2ap-${DESIRED_SERVICE}/i2ap-${DESIRED_SERVICE}.tar.gz
        eval ${FIND_CMD} | tar -czf $PROJECT_ROOT/dist/i2ap-${DESIRED_SERVICE}.tar.gz -T -
        cp $PROJECT_ROOT/dist/i2ap-${DESIRED_SERVICE}.tar.gz $PROJECT_ROOT/docker/i2ap-${DESIRED_SERVICE}/i2ap-${DESIRED_SERVICE}.tar.gz
    else
        echo "No Find command configured in: "$find_command_file
    fi
}

function buildDockerImage()
{
    echo "buildDockerImage"

    cd ${PROJECT_ROOT}
    docker_build_file=${SCRIPT_DIR}"/docker-build.sh"
    if [ -f $docker_build_file ]
    then
        echo "Building Docker image for service '"${DESIRED_SERVICE}"'..."
        BUILD_CMD=$docker_build_file
        cd ${PROJECT_ROOT}/docker/i2ap-${DESIRED_SERVICE}
        ${BUILD_CMD} ${DESIRED_SERVICE}
    else
        echo "No Docker Build command configured in: "$docker_build_file
    fi
}

function :yService()
{
    echo "deployService"
    cd ${PROJECT_ROOT}
    POD_VERSION=`cat script/config/latest-${DESIRED_SERVICE}-version`
    deployment_yaml_file="docker/i2ap-"${DESIRED_SERVICE}"/deployment.yaml"
    versioned_yaml_file="docker/i2ap-"${DESIRED_SERVICE}"/versioned.yaml"
    if [ -f $deployment_yaml_file ]
    then
        echo "Deploying service '"${DESIRED_SERVICE}"'..."
        cp ${deployment_yaml_file} ${versioned_yaml_file}
        SED_REPLACE="sed -i -e s,\(image:.*"${DESIRED_SERVICE}":\).*$,\1"${POD_VERSION}",g "${versioned_yaml_file}
        ${SED_REPLACE}
        echo "Deploying to Cluster: "`kubectl config current-context | cut -d '_' -f 4`

        x=`kubectl --namespace=i2ap rollout status deploy/${DESIRED_SERVICE} 2>&1 |grep NotFound`
        if [ "${x:-"FOUND"}" == "FOUND" ]
        then
            kubectl replace -f ${versioned_yaml_file}
        else
            kubectl create -f ${versioned_yaml_file}
        fi
    else
        echo "No Docker Build command configured in: "$deployment_yaml_file
    fi

    rm -f script/config/latest-${DESIRED_SERVICE}-version ${versioned_yaml_file}-e ${versioned_yaml_file}
}

function displayServiceList()
{
	echo "	Valid Services: "
	for i in "${validServices[@]}"
	do
		echo "	    $i"
	done

}

function summarizeSettings()
{
    echo "	Settings: "
    echo "		Project: "$GCLOUD_PROJECT
    echo "		Cluster: "$GCLOUD_CLUSTER
    echo "		PROJECT_ROOT: "$PROJECT_ROOT
    echo "		DESIRED_SERVICE: "$DESIRED_SERVICE
    echo '====================================='

}

function containsValue()
{
	local e
	for e in "${@:2}"; do [[ "$e" == "$1" ]] && return 0; done
	return 1
}

function sampleExecutions()
{
	echo "	Sample Execution Command: "
	for i in "${validServices[@]}"
	do
		echo "	   $0 -s $i"
	done
}
function displayUsage()
{
    echo '====================================='
    echo 'Usage: ' >&2
    echo '====================================='
    echo '-1	Verbose execution.... Really, really, excruciatingly, verbose'
    echo '-?	The Help text you see here.'
    echo '-p	PROJECT ROOT: optional, defaults to git highest level directory, if not set'
    echo '-s	The Service you wish to build'
    displayServiceList
    echo '-x	Sample Execution Line Commands with Arguments for a table'
    echo '====================================='
}


cleanup() {
        local pids=$(jobs -pr)
        [ -n "$pids" ] && kill $pids
}

function minimumArgsEntered()
{
    returnVal=0
    if [ "${DESIRED_SERVICE:-"NOT SET"}" == "NOT SET" ]
    then
        echo -n ">>>>>>>>>>>>>>> "
        echo -n "DESIRED_SERVICE Parameter not found"
        echo " <<<<<<<<<<<<<<<"
        returnVal=1
    fi
    if [ "${PROJECT_ROOT:-"NOT SET"}" == "NOT SET" ]
    then
        echo -n ">>>>>>>>>>>>>>> "
        echo -n "PROJECT_ROOT Parameter not found"
        echo " <<<<<<<<<<<<<<<"
        returnVal=1
    fi

    return $returnVal

}




if [ $# -eq 0 ]
then
    displayUsage
    exit 1
fi

trap "cleanup" INT QUIT TERM EXIT
while getopts "1:?:s:x" opt;
do
  case $opt in
    s)
		if containsValue $OPTARG "${validServices[@]}"
		then
#			echo "Desired Service: $OPTARG" >&2
            DESIRED_SERVICE=$OPTARG
		else
			echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			echo "Invalid Service specified."
			echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			displayServiceList
			exit 1
		fi
      ;;
    x)
        sampleExecutions
    	exit 1
      ;;
    1)
	    set -x
      ;;
    ?)
	    displayUsage
        exit 1
      ;;
    \?)
      echo "Invalid option: -$opt" >&2
      exit 1
      ;;
    :)
      echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
      echo "Option -$OPTARG requires an argument." >&2
      echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
      exit 1
      ;;
  esac
done

#gcloud config set project $GCLOUD_PROJECT
#kubectl config set-cluster $GCLOUD_CLUSTER
#

summarizeSettings

cd $PROJECT_ROOT
createSourceTarBall
buildDockerImage
#deployService

echo "Done with "$0" -s "$DESIRED_SERVICE" execution"


