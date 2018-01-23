#!/usr/bin/env bash

# if the container is not run interactive then start the web server
if [ "$INTERACTIVE" != "true" ]; then
    # start/execute the micro-service
    echo "starting the data processor web server"
    /usr/bin/python /i2ap-processor/service/processor/app.py
else
    # start the terminal
    export SHELLOPTS
    /bin/bash
fi
