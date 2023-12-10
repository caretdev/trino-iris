#!/bin/bash

pip install superset-iris apache-superset[trino]

superset db upgrade

superset fab create-admin \
            --username admin \
            --firstname Superset \
            --lastname Admin \
            --email admin@superset.com \
            --password ${ADMIN_PASSWORD:-admin}

superset init

# Load examples to IRIS
#superset load-examples

# change examples database uri to trino
# superset set-database-uri -d examples -u trino://trino:8080/iris

/usr/bin/run-server.sh