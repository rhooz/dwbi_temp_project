#!/usr/bin/env bash
# description: establishes and configures a jenkins master within a given kubernetes cluster
# reference for script: https://cloud.google.com/solutions/jenkins-on-container-engine-tutorial
# reference for child setup (within script)

# arguments:
# -i --id: GCP project id
# -n --name: GCP project name
# -z --zone: GCE zone
# -a --airflow: Include an airflow instance default = true
# -j --jenkins: Include a jenkins instance default = true
# -m --metabase: Include a metabase instance default = true
# -p --processor: Include data-processor default = true
# -l --listener: Include listener default = true
# -d --deployment-type: Deployment Type default = dev

# usage: $ ./setup-i2ap-project.sh wynn "Wynn SF Data Migration" --zone=us-west1-c --airflow=false --deployment-type=dev
# note: project id will be "tt-cust-" what is passed in for project_id
# note: kubectl cluster will be built/named as <GCP project name>-kube
# output: i2ap-config-<GCP project name>.yaml will contain the external IP that can be used to log into Jenkins and Metabase

#
# PROJECT BUILD
#
# set the build defaults
BUILDJENKINS=true
BUILDAIRFLOW=true
BUILDLISTENER=true
BUILDPROCESSOR=true
BUILDMETABASE=true
DEPTYPE=dev
# parse out the command line arguments
for i in "$@"
do
case $i in
  -i=*|--id=*)
  PROJID="${i#*=}"
  shift
  ;;
  -d=*|--deployment-type=*)
  DEPTYPE="${i#*=}"
  shift
  ;;
  -n=*|--name=*)
  PROJNAME="${i#*=}"
  shift
  ;;
  -j=*|--jenkins=*)
  BUILDJENKINS="${i#*=}"
  shift
  ;;
  -a=*|--airflow=*)
  BUILDAIRFLOW="${i#*=}"
  shift
  ;;
  -l=*|--listener=*)
  BUILDLISTENER="${i#*=}"
  shift
  ;;
  -p=*|--processor=*)
  BUILDPROCESSOR="${i#*=}"
  shift
  ;;
  -m=*|--metabase=*)
  BUILDMETABASE="${i#*=}"
  shift
  ;;
  -z=*|--zone=*)
  ZONE="${i#*=}"
  shift
  ;;
esac
done

TTPROJECTID=tt-cust-$PROJID
#convert into an under-scored version
UND_PROJID=`echo $PROJID | sed 's/-/_/g'`
STREND=${#ZONE}
STREND=`expr $STREND - 2`
REGION=${ZONE:0:$STREND}
I2AP_HOME=`pwd`
I2AP_HOME=${I2AP_HOME%/*}
GIT_HOME=${I2AP_HOME%/*}
I2APSTARTTIME=$SECONDS

# create the project
echo Building project $TTPROJECTID...
gcloud projects create $TTPROJECTID --name="$PROJNAME"
# change the environment to the new project
gcloud config set project $TTPROJECTID
# enable billing on the project
gcloud alpha billing projects link $TTPROJECTID --billing-account=00BB30-E07465-DBEF9F
#
# DNS BUILD
#
echo "Setting up the project's sub-domain..."
# enable the dns api
gcloud service-management enable dns.googleapis.com
# establish the dns zone for the new project
gcloud dns managed-zones create $PROJID-tectonic-cloud --dns-name $PROJID.tectonic-cloud.com. --description "$PROJNAME DNS Base"
# pull the configured dns servers for the new zone
DNS=`gcloud dns record-sets list --zone $PROJID-tectonic-cloud | sed -n '/^'$PROJID'/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 2,4 | sed -n '/^NS/p' | cut -d ' ' -f 2`
# grab the 4 pieces
NS1=`echo $DNS | cut -d ',' -f 1`
NS2=`echo $DNS | cut -d ',' -f 2`
NS3=`echo $DNS | cut -d ',' -f 3`
NS4=`echo $DNS | cut -d ',' -f 4`
# mapping the project dns in the parent can take a bit... get started (in the background) now
# switch back to the parent project
gcloud config set project tectonic-methods-lab
# establish the subdomain in the parent DNS
gcloud dns record-sets transaction start --zone tectonic-cloud
gcloud dns record-sets transaction add --zone tectonic-cloud --name $PROJID.tectonic-cloud.com. --ttl 21600 --type NS "$NS1" "$NS2" "$NS3" "$NS4"
gcloud dns record-sets transaction execute --zone tectonic-cloud
# switch back to the new project and move on with other work.
# Note: project level DNS mapping (A records) can continue, but will not be visible until sub-domain "percolates" the internet
#
# KUBERNETES BUILD
#
gcloud config set project $TTPROJECTID
# enable the container engine API
gcloud service-management enable compute.googleapis.com
# establish the cluster
echo Building Kubernetes cluster $$TTPROJECTID-kube...
gcloud container clusters create $TTPROJECTID-kube \
 --zone $ZONE \
 --num-nodes=3 \
 --machine-type=n1-highmem-4 \
 --scopes "https://www.googleapis.com/auth/projecthosting,https://www.googleapis.com/auth/cloud-platform,storage-rw"
# retrieve the credentials
gcloud container clusters get-credentials $TTPROJECTID-kube --project $TTPROJECTID --zone $ZONE
#
#
# GIT REPO BUILD
#
cd $GIT_HOME
# build out the base repository
echo "Building development repository"
# enable the source repositories API
gcloud beta service-management enable sourcerepo.googleapis.com
# create the new repository
gcloud source repos create $PROJID
# change the project over to tectonic-methods-lab and pull the base software
gcloud source repos clone i2ap i2ap-tmp --project=tectonic-methods-lab
cd $GIT_HOME/i2ap-tmp
git checkout dev
# add the new repo as a remote
git remote add google https://source.developers.google.com/p/$TTPROJECTID/r/$PROJID
# push in the i2ap code
git push --all google
# cleanup the tmp i2ap repo
cd $GIT_HOME
rm -rf $GIT_HOME/i2ap-tmp
#
# SERVICE ACCOUNT AND REPO BUILD
#
# build out the initial service account
echo "Building service account for base pod..."
# build the account
gcloud iam service-accounts create i2ap-service --display-name="Insight2Action Platform"
# grant editor priv to the service account
gcloud projects add-iam-policy-binding $TTPROJECTID \
    --member serviceAccount:i2ap-service@$TTPROJECTID.iam.gserviceaccount.com --role roles/editor
# move to the root git directory
cd $GIT_HOME
# bring down the new project repo
gcloud source repos clone $PROJID
cd $GIT_HOME/$PROJID
# move into the project, then to the base image directory
git checkout dev
cd $GIT_HOME/$PROJID/docker
# create the key file for the service account
gcloud iam service-accounts keys create credentials.json --iam-account=i2ap-service@$TTPROJECTID.iam.gserviceaccount.com
# add the key and commit the change
git add credentials.json
git commit -am 'added service account key credentials.json'
git push origin dev
# Create a namespace to separate the service from others
kubectl create ns i2ap
# add the key to kubernets as a secret
kubectl create secret generic i2ap-glcoud-credentials --namespace=i2ap \
                       --from-file=credentials.json="$GIT_HOME/$PROJID/docker/credentials.json"
# grant the new service account access to the parent repository
gsutil defacl ch -u i2ap-service@$TTPROJECTID.iam.gserviceaccount.com:READ gs://us.artifacts.tectonic-methods-lab.appspot.com
gsutil acl ch -r -u i2sap-service@$TTPROJECTID.iam.gserviceaccount.com:READ gs://us.artifacts.tectonic-methods-lab.appspot.com
# enable the container registry API
gcloud beta service-management enable containerregistry.googleapis.com
# build the initial google storage buckets
gsutil mb gs://$TTPROJECTID-stage-$DEPTYPE/
gsutil mb gs://$TTPROJECTID-archive-$DEPTYPE/
# build the initial big query dataset
bq mk --project_id=$TTPROJECTID $UND_PROJID'_'$DEPTYPE
# generate a fun secret
TTSECRET=`openssl enc -aes-256-cbc -k secret -P -md sha1 | sed -n '/^iv/p' | cut -d "=" -f2`
# create the i2ap secret
kubectl create secret generic i2ap-service-access-$DEPTYPE --from-literal=client-secret=$TTSECRET --namespace=i2ap
# write out the configuration yaml
echo "i2ap-service-access-key: "$TTSECRET >> $GIT_HOME/i2ap-config-$TTPROJECTID-$DEPTYPE.yaml
# create the google storage secret
kubectl create secret generic i2ap-gs-access --from-literal=client-id=NEEDINFO --from-literal=client-secret=NEEDINFO --namespace=i2ap
#
# Let's Encrypt BUILD
#
echo "Building the LEGO service..."
cd $GIT_HOME/$PROJID/docker/i2ap-lego
# create the LEGO configuration
kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-lego/lego-configmap.yaml
# create the deployment for the LEGO instance
kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-lego/lego.yaml
#
# See if we need a CloudSQL Instance
#
if [ "$BUILDAIRFLOW" = "true" ] || [ "$BUILDMETABASE" = "true" ]
  then
    echo "Establishing the CloudSQL instance..."
    # Build out the Cloud SQL service
    gcloud beta service-management enable sqladmin.googleapis.com
    gcloud sql instances create $TTPROJECTID-mysql --tier=db-f1-micro --region=$REGION
    gcloud sql users set-password root % --instance $TTPROJECTID-mysql --password T3ct0n1c
    # grab the status associated with the new instance
    CLOUDSQLSTATUS=`gcloud sql instances describe $TTPROJECTID-mysql | sed -n '/^state/p' | cut -d ' ' -f 2`
    # wait until you get a runnable status back before trying to build the database
    while [ "$CLOUDSQLSTATUS" != "RUNNABLE" ]
    do
      echo "waiting another 60s for CloudSQL instance to become available"
      sleep 60s
      CLOUDSQLSTATUS=`gcloud sql instances describe $TTPROJECTID-mysql | sed -n '/^state/p' | cut -d ' ' -f 2`
    done
fi

#
# DEV DOCKER IMAGE BUILDS
#
# build the base docker image
if [ "$BUILDPROCESSOR" = "true" ]
  then
    echo "Building I2AP data processor container and pushing to registry..."
    # assemble the baseline processor software
    cd $GIT_HOME/$PROJID/script
    /bin/bash build-i2ap-processor.sh $GIT_HOME/$PROJID
    # copy the template build script over and modify
    cd $GIT_HOME/$PROJID/docker/i2ap-processor
    cp docker-build-template.sh docker-build.sh
    chmod +x docker-build.sh
    # set the script variables
    sed -i -e 's/I2AP_PROJECT_ID/'"$TTPROJECTID"'/g' docker-build.sh
    # grab the project number
    TTPROJECTNO=`gcloud projects describe $TTPROJECTID |  sed -n '/^  id/p' | cut -d "'" -f2`
    # build and push the docker image
    /bin/bash docker-build.sh
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-processor/docker-build.sh
    # build the kubernetes config map file
    cp configmap-template.yaml configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DEP_TYPE/'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_PROJECT_ID/'"$TTPROJECTID"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_REGION/'"$REGION"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_ZONE/'"$ZONE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_STAGE_BUCKET/'"$PROJID"'-stage-'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_ARCHIVE_BUCKET/'"$PROJID"'-archive-'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DATASET/'"$UND_PROJID"'_lake_'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_PORT/3101/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_OVERRIDE_PORT//g' configmap-$DEPTYPE.yaml
    # submit the configuration file
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-processor/configmap-$DEPTYPE.yaml
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-processor/configmap-$DEPTYPE.yaml
    # create the i2ap deployment
    cp deployment-template.yaml deployment-$DEPTYPE.yaml
    sed -i -e 's/I2AP_IMAGE_ID/'"$TTPROJECTID"'/g' deployment-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DEP_TYPE/'"$DEPTYPE"'/g' deployment-$DEPTYPE.yaml
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-processor/deployment-$DEPTYPE.yaml
    git add $GIT_HOME/$PROJID/docker/i2ap-processor/deployment-$DEPTYPE.yaml
    # create the associated service
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-processor/service.yaml
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-processor/processor-$DEPTYPE.yaml
    # create the ingress
    cp ingress-template.yaml ingress-$DEPTYPE.yaml
    sed -i -e 's/I2AP_PROJECT_ID_BASE/'"$PROJID"'/g' ingress-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DEP_TYPE/'"$DEPTYPE"'/g' ingress-$DEPTYPE.yaml
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-processor/ingress-$DEPTYPE.yaml
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-processor/ingress-$DEPTYPE.yaml
    # build a local run script for dev docker purposes
    cd $GIT_HOME/$PROJID/script
    cp run-i2ap-processor-template.sh run-i2ap-processor.sh
    sed -i -e 's/I2AP_PROJECT_ID_BASE/'"$PROJID"'/g' run-i2ap-processor.sh
    sed -i -e 's/I2AP_PROJECT_ID_UND/'"$UND_PROJID"'/g' run-i2ap-processor.sh
    sed -i -e 's/I2AP_REGION_VAR/'"$REGION"'/g' run-i2ap-processor.sh
    sed -i -e 's/I2AP_ZONE_VAR/'"$ZONE"'/g' run-i2ap-processor.sh

    git commit -am 'added processor deployment scripts'
    git push origin dev
    # need to wait for the external IP address to be added
    sleep 60s
    PROCIPADDRESS=`kubectl get ingress processor --namespace=i2ap | sed -n '/^processor/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    while [ "$PROCIPADDRESS" = "80," ]
    do
      sleep 60s
      PROCIPADDRESS=`kubectl get ingress processor --namespace=i2ap | sed -n '/^processor/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    done

    # write out the configuration yaml
    echo "i2ap-processor-ip-address: "$PROCIPADDRESS >> $GIT_HOME/i2ap-config-$TTPROJECTID-$DEPTYPE.yaml
    # add the dns entry for the jenkins server
    gcloud dns record-sets transaction start --zone $PROJID-tectonic-cloud
    gcloud dns record-sets transaction add --zone $PROJID-tectonic-cloud --name processor.$PROJID.tectonic-cloud.com. --ttl 300 --type A "$PROCIPADDRESS"
    gcloud dns record-sets transaction execute --zone $PROJID-tectonic-cloud
fi
#
# DEV LISTENER IMAGE BUILDS
#
# build the base docker image for the listener
if [ "$BUILDLISTENER" = "true" ]
  then
    echo "Building I2AP listener container and pushing to registry..."
    # enable pup/sub
    gcloud service-management enable pubsub.googleapis.com
    # create a pub/sub notification on the staging bucket (only for new/uploads)
    gsutil notification create -t $TTPROJECTID-stage-$DEPTYPE-listen -e OBJECT_FINALIZE -f json gs://$TTPROJECTID-stage-$DEPTYPE
    # assemble the baseline processor software
    cd $GIT_HOME/$PROJID/script
    /bin/bash build-i2ap-listener.sh $GIT_HOME/$PROJID
    # copy the template build script over and modify
    cd $GIT_HOME/$PROJID/docker/i2ap-listener
    cp docker-build-template.sh docker-build.sh
    chmod +x docker-build.sh
    # set the script variables
    sed -i -e 's/I2AP_PROJECT_ID/'"$TTPROJECTID"'/g' docker-build.sh
    # build and push the docker image
    /bin/bash docker-build.sh
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-listener/docker-build.sh
    # build the kubernetes config map file
    cp configmap-template.yaml configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DEP_TYPE/'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_PROJECT_ID/'"$TTPROJECTID"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_REGION/'"$REGION"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_ZONE/'"$ZONE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_STAGE_BUCKET/'"$PROJID"'-stage-'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_ARCHIVE_BUCKET/'"$PROJID"'-archive-'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DATASET/'"$UND_PROJID"'_lake_'"$DEPTYPE"'/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_PORT/3101/g' configmap-$DEPTYPE.yaml
    sed -i -e 's/I2AP_OVERRIDE_PORT//g' configmap-$DEPTYPE.yaml
    # submit the configuration file
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-listener/configmap-$DEPTYPE.yaml
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-listener/configmap-$DEPTYPE.yaml
    # create the google storage secret
    kubectl create secret generic i2ap-gs-access --from-literal=client-id=NEEDINFO --from-literal=client-secret=NEEDINFO --namespace=i2ap
    # create the i2ap deployment
    cp deployment-template.yaml deployment-$DEPTYPE.yaml
    sed -i -e 's/I2AP_IMAGE_ID/'"$TTPROJECTID"'/g' deployment-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DEP_TYPE/'"$DEPTYPE"'/g' deployment-$DEPTYPE.yaml
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-listener/deployment-$DEPTYPE.yaml
    # create the associated service
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-listener/service.yaml
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-listener/listener-$DEPTYPE.yaml
    # create the ingress
    cp ingress-template.yaml ingress-$DEPTYPE.yaml
    sed -i -e 's/I2AP_PROJECT_ID_BASE/'"$PROJID"'/g' ingress-$DEPTYPE.yaml
    sed -i -e 's/I2AP_DEP_TYPE/'"$DEPTYPE"'/g' ingress-$DEPTYPE.yaml
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-listener/ingress-$DEPTYPE.yaml
    # add script to repo
    git add $GIT_HOME/$PROJID/docker/i2ap-listener/ingress-$DEPTYPE.yaml
    git commit -am 'added listener deployment scripts'
    git push origin dev
    # need to wait for the external IP address to be added
    sleep 60s
    LISTIPADDRESS=`kubectl get ingress listener --namespace=i2ap | sed -n '/^listener/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    while [ "$LISTIPADDRESS" = "80," ]
    do
      sleep 60s
      LISTIPADDRESS=`kubectl get ingress listener --namespace=i2ap | sed -n '/^listener/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    done

    # write out the configuration yaml
    echo "i2ap-listener-ip-address: "$LISTIPADDRESS >> $GIT_HOME/i2ap-config-$TTPROJECTID-$DEPTYPE.yaml
    # add the dns entry for the jenkins server
    gcloud dns record-sets transaction start --zone $PROJID-tectonic-cloud
    gcloud dns record-sets transaction add --zone $PROJID-tectonic-cloud --name listener.$PROJID.tectonic-cloud.com. --ttl 300 --type A "$LISTIPADDRESS"
    gcloud dns record-sets transaction execute --zone $PROJID-tectonic-cloud
fi
#
# AIRFLOW BUILD
#
if [ "$BUILDAIRFLOW" = "true" ]
  then
    echo "establishing an instance of Airflow..."
    cd $GIT_HOME/$PROJID/docker/i2ap-airflow
    # update the configmap with project details
    sed -i -e 's/I2AP_PROJECT_ID/'"$TTPROJECTID"'/g' configmap.yaml
    sed -i -e 's/I2AP_REGION/'"$REGION"'/g' configmap.yaml
    sed -i -e 's/I2AP_ZONE/'"$ZONE"'/g' configmap.yaml
    # create the MySQL database for Airflow
    gcloud sql databases create airflow --instance $TTPROJECTID-mysql
    gcloud sql users create dbuser_airflow cloudsqlproxy~% --instance=$TTPROJECTID-mysql --password T3ct0n1c
    # submit the configuration file
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-airflow/configmap.yaml
    # create the connection string secret
    kubectl create secret generic airflow-connection --from-literal=connection-string=mysql://dbuser_airflow:T3ct0n1c@127.0.0.1:3306/airflow --namespace=i2ap
    # set the project variables in the deployment
    sed -i -e 's/I2AP_PROJECT_ID/'"$TTPROJECTID"'/g' $GIT_HOME/$PROJID/docker/i2ap-airflow/deployment.yaml
    sed -i -e 's/I2AP_PROJECT_REGION/'"$REGION"'/g' $GIT_HOME/$PROJID/docker/i2ap-airflow/deployment.yaml
    # submit the deployment
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-airflow/deployment.yaml
    # create the associated service
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-airflow/service.yaml
    # update the ingress
    sed -i -e 's/I2AP_PROJECT_ID_BASE/'"$PROJID"'/g' ingress.yaml
    # create the associated ingress
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-airflow/ingress.yaml
    # clean up git
    git commit -am 'added airflow deployment scripts'
    git push origin dev
    # need to wait for the external IP address to be added
    sleep 60s
    AIRIPADDRESS=`kubectl get ingress airflow --namespace=i2ap | sed -n '/^airflow/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    while [ "$AIRIPADDRESS" = "80," ]
    do
      sleep 60s
      AIRIPADDRESS=`kubectl get ingress airflow --namespace=i2ap | sed -n '/^airflow/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    done
    # write out the configuration yaml
    echo "i2ap-airlfow-ip-address: "$AIRIPADDRESS >> $GIT_HOME/i2ap-config-$TTPROJECTID-$DEPTYPE.yaml
    # add the dns entry for the jenkins server
    gcloud dns record-sets transaction start --zone $PROJID-tectonic-cloud
    gcloud dns record-sets transaction add --zone $PROJID-tectonic-cloud --name airflow.$PROJID.tectonic-cloud.com. --ttl 300 --type A "$AIRIPADDRESS"
    gcloud dns record-sets transaction execute --zone $PROJID-tectonic-cloud
fi
#
# JENKINS BUILD
#
if [ "$BUILDJENKINS" = "true" ]
  then
    # pull the image from the reference in GCP
    echo "Building Jenkins master pod in cluster..."
    gcloud compute images create jenkins-home-image --source-uri https://storage.googleapis.com/solutions-public-assets/jenkins-cd/jenkins-home-v3.tar.gz
    # create the persistent disk for the jenkins cluster from the image
    gcloud compute disks create jenkins-home --image jenkins-home-image --zone $ZONE
    # create jenkins secret within kubernetes
    kubectl create secret generic jenkins --from-file=$GIT_HOME/$PROJID/docker/i2ap-jenkins/options --namespace=i2ap
    # create the kubernetes deployment for the jenkins master
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-jenkins/jenkins.yaml
    # create the associated services to expose the jenkins master
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-jenkins/jenkins-service.yaml
    # build the custom ingress yaml file
    cp $GIT_HOME/$PROJID/docker/i2ap-jenkins/jenkins-ingress-template.yaml $GIT_HOME/$PROJID/docker/i2ap-jenkins/jenkins-ingress-$TTPROJECTID.yaml
    # set the project variables
    sed -i -e 's/I2AP_PROJECT_ID_BASE/'"$PROJID"'/g' $GIT_HOME/$PROJID/docker/i2ap-jenkins/jenkins-ingress-$TTPROJECTID.yaml
    # establish the load balancer for the external endpoint
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-jenkins/jenkins-ingress-$TTPROJECTID.yaml
    # need to wait for the external IP address to be added
    sleep 60s
    JENKINSIPADDRESS=`kubectl get ingress jenkins --namespace=i2ap | sed -n '/^jenkins/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    while [ "$JENKINSIPADDRESS" = "80," ]
    do
      sleep 60s
      JENKINSIPADDRESS=`kubectl get ingress jenkins --namespace=i2ap | sed -n '/^jenkins/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    done
    # commit the new build script
    git add $GIT_HOME/$PROJID/docker/i2ap-jenkins/jenkins-ingress-$TTPROJECTID.yaml
    git commit -am 'added docker base image build script and kubernetes yamls'
    git push origin dev
    # write out the configuration yaml
    echo "jenkins-ip-address: "$JENKINSIPADDRESS > $GIT_HOME/i2ap-config-$TTPROJECTID-$DEPTYPE.yaml
    # add the dns entry for the jenkins server
    gcloud dns record-sets transaction start --zone $PROJID-tectonic-cloud
    gcloud dns record-sets transaction add --zone $PROJID-tectonic-cloud --name jenkins.$PROJID.tectonic-cloud.com. --ttl 300 --type A "$JENKINSIPADDRESS"
    gcloud dns record-sets transaction execute --zone $PROJID-tectonic-cloud
fi
#
# METABASE BUILD
#
if [ "$BUILDMETABASE" = "true" ]
  then
    echo "Building the metabase service set..."
    cd $GIT_HOME/$PROJID/docker/i2ap-metabase
    # Build the Tectonic metabase image and push to the registry
    docker build -t us.gcr.io/$TTPROJECTID/i2ap-metabase:1.0.0 .
    gcloud docker -- push us.gcr.io/$TTPROJECTID/i2ap-metabase:1.0.0
    # Build the metabase MySQL database
    gcloud sql databases create metabase-repos --instance $TTPROJECTID-mysql
    gcloud sql users create metabase cloudsqlproxy~% --instance=$TTPROJECTID-mysql --password metabase
    # associate the service account using the keyfile
    kubectl create secret generic cloudsql-instance-credentials --namespace=i2ap \
                           --from-file=credentials.json="$GIT_HOME/$PROJID/docker/credentials.json"
    kubectl create secret generic cloudsql-db-credentials --from-literal=username=metabase --from-literal=password=metabase --namespace=i2ap
    # build the custom yaml file
    cp $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-template.yaml $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-$TTPROJECTID.yaml
    # set the project variables
    sed -i -e 's/I2AP_PROJECT_ID/'"$TTPROJECTID"'/g' $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-$TTPROJECTID.yaml
    sed -i -e 's/I2AP_PROJECT_REGION/'"$REGION"'/g' $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-$TTPROJECTID.yaml
    # create the deployment for the metabase instance
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-$TTPROJECTID.yaml
    # create the associated services to expose the metabase instance
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-service.yaml
    # build the custom ingress yaml file
    cp $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-ingress-template.yaml $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-ingress-$TTPROJECTID.yaml
    # set the project variables
    sed -i -e 's/I2AP_PROJECT_ID_BASE/'"$PROJID"'/g' $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-ingress-$TTPROJECTID.yaml
    # establish the load balancer for the external endpoint
    kubectl create -f $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-ingress-$TTPROJECTID.yaml
    # need to wait for the external IP address to be added
    sleep 60s
    METABASEIPADDRESS=`kubectl get ingress metabase --namespace=i2ap | sed -n '/^metabase/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    while [ "$METABASEIPADDRESS" = "80," ]
    do
      sleep 60s
      METABASEIPADDRESS=`kubectl get ingress metabase --namespace=i2ap | sed -n '/^metabase/p' | sed -n 's/  */ /gp' | cut -d ' ' -f 3`
    done
    # add the new deployment yaml to the git repo
    git add $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-$TTPROJECTID.yaml
    git add $GIT_HOME/$PROJID/docker/i2ap-metabase/metabase-ingress-$TTPROJECTID.yaml
    git commit -am 'added project information to deployment yaml'
    git push origin dev
    # write out the configuration yaml
    echo "metabase-ip-address: "$METABASEIPADDRESS >> $GIT_HOME/i2ap-config-$TTPROJECTID-$DEPTYPE.yaml
    # add the dns entry for the metabase server
    gcloud dns record-sets transaction start --zone $PROJID-tectonic-cloud
    gcloud dns record-sets transaction add --zone $PROJID-tectonic-cloud --name metabase.$PROJID.tectonic-cloud.com. --ttl 300 --type A "$METABASEIPADDRESS"
    gcloud dns record-sets transaction execute --zone $PROJID-tectonic-cloud
fi
# cleanup
cd $GIT_HOME
rm -rf $PROJID
I2APDURATION=$(( SECONDS - I2APSTARTTIME ))
echo "----------------------"
echo "This process took "$I2APDURATION" seconds to complete"
if [ "$BUILDAIRFLOW" = "true" ]
  then
    echo "You can access the Airflow Instance at https://"$AIRIPADDRESS
fi
if [ "$BUILDJENKINS" = "true" ]
  then
    echo "You can access the Jenkins master at https://"$JENKINSIPADDRESS
fi
if [ "$BUILDMETABASE" = "true" ]
  then
    echo "You can access Metabase at https://"$METABASEIPADDRESS
fi
if [ "$BUILDPROCESSOR" = "true" ]
  then
    echo "You can access the I2AP data processor service endpoint at https://"$PROCIPADDRESS
fi
if [ "$BUILDLISTENER" = "true" ]
  then
    echo "You can access the I2AP listener service endpoint at https://"$LISTIPADDRESS
fi
echo "Note: The service DNS names should be accessible within 5 minutes"
echo "Note: SSL keys will take 15 minutes to resolve correctly through the GCP ingress"
echo "You can view your starting dev branch at https://console.cloud.google.com/code/develop/browse/"$PROJID"/dev?project="$TTPROJECTID
echo "----------------------"
echo "Project setup complete"
