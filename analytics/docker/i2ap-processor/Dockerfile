# service implementation of i2ap
FROM docker.io/tectoniccloud/i2ap-base:1.0.5
MAINTAINER Tectonic

# deploy any additional python components
RUN pip install simple_salesforce
RUN pip install salesforce_bulkipy

# deploy the TT software
RUN mkdir /i2ap-processor
COPY i2ap-processor.tar.gz /i2ap-processor/i2ap-processor.tar.gz
RUN cd /i2ap-processor; tar -xvf i2ap-processor.tar.gz

COPY run_processor.sh /root

# Note: You need to expose/publish your port with a -p in the case of a docker run

ENTRYPOINT /root/run_processor.sh
