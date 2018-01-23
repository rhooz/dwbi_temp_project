#!/bin/bash
# build the js graph generation service project
# arg1: project root or $WORKSPACE for Jenkins

# make sure it is executed from project root
# usage example: build-i2ap-processor.sh /usr/local/src/mystuff
cd $1
echo "building the I2AP processor distribution for data pull service"
  { find lib ddl service/processor -path lib/lib -prune -o -name "*.py" -o -name "*.json" -o -name "*.yaml"; \
  find script -name "*.sh"; } \
  | tar -czf $1/dist/i2ap-processor.tar.gz -T -
cp $1/dist/i2ap-processor.tar.gz $1/docker/i2ap-processor/i2ap-processor.tar.gz
