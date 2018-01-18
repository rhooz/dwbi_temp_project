# service implementation of i2ap
FROM docker.io/tectoniccloud/i2ap-base:1.0.5
MAINTAINER Tectonic

# deploy any additional python components

# deploy the TT software
RUN mkdir /i2ap-listener
COPY i2ap-listener.tar.gz /i2ap-listener/i2ap-listener.tar.gz
RUN cd /i2ap-listener; tar -xvf i2ap-listener.tar.gz

COPY run_listener.sh /root

# Note: You need to expose/publish your port with a -p in the case of a docker run

ENTRYPOINT /root/run_listener.sh
