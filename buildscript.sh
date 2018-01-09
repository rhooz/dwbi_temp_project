#!/bin/bash

#build the python container
cd dva_python
docker build -t dva_python
cd ../

#build the oracle container
cd OracleDatabase/dockerfiles
./buildDockerImage.sh -v 12.2.0.1 -s
cd ../..

#alter the volume mapping in the docker-compose.yml
#start the docker compose file
cd dockercomposetest
docker-compose up

