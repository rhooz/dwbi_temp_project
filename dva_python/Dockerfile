# full linux image for google cloud sdk and python 2.7
# supports the development of Insight2Action Platform Projects in GCP
# current version 1.0.5
FROM centos:latest
MAINTAINER Tectonic

# install ssh
RUN yum -y update
RUN yum -y install which wget; yum clean all

# install development build tools
RUN yum -y groupinstall "Development Tools"
RUN yum -y install gettext-devel openssl-devel perl-CPAN perl-devel zlib-devel curl-devel

# get pip installed
RUN wget -r --no-parent -A 'epel-release-*.rpm' http://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/e/
RUN rpm -Uvh dl.fedoraproject.org/pub/epel/7/x86_64/Packages/e/epel-release-*.rpm
RUN yum -y install python-pip gcc openssl-devel python-devel python-setuptools libxslt-devel libxml2-devel libffi-devel git;  yum clean all

# upgrade pip to latest version
RUN pip install --upgrade pip

# get the latest version of git installed
# note, just installing would not change the path, needed to place on top of existing /usr/bin which is weird/wrong
# look into hash -d git to see if this can be cleaner
RUN yum -y remove git
RUN wget https://github.com/git/git/archive/v2.15.1.tar.gz
RUN tar -xvf v2.15.1.tar.gz
RUN cd git-2.15.1 && \
    make configure && \
    ./configure --prefix=/usr && \
    make install

# python libraries for yaml file manipulation
RUN pip install pyyaml

# python libraries for Testing
RUN pip install behave

# python library for performing ETL functions
RUN pip install petl

# table manipulation and cleanup library
RUN pip install messytables

# numpy for stats and array functions
RUN pip install numpy

# pandas for data transformations
RUN pip install pandas

# dateutil for converting datasets
RUN pip install python-dateutil

# library to run microservice
RUN pip install tornado

# do some fun stuff with json
RUN pip install --upgrade ujson

# add database libraries
RUN pip install cx-oracle

# build Oracle client
ADD oracle-instantclient*.rpm /tmp/
RUN  yum -y install /tmp/oracle-instantclient*.rpm && \
     rm -f /tmp/oracle-instantclient*.rpm && \
     echo /usr/lib/oracle/12.1/client64/lib > /etc/ld.so.conf.d/oracle-instantclient12.1.conf && \
     ldconfig

ENV PATH=$PATH:/usr/lib/oracle/12.1/client64/bin

# cleanup
RUN rm -f v2.15.1.tar.gz

ENTRYPOINT /bin/bash
