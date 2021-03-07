#!/bin/bash

# clone docker-airflow repo
git clone git@github.com:puckel/docker-airflow.git

# Install needed Python dependencies
# NOTE: https://github.com/puckel/docker-airflow/issues/535
docker build --rm \
--build-arg PYTHON_DEPS="google-cloud-storage==1.36.1 pandas-gbq==0.14.1 geopandas==0.9.0 SQLAlchemy==1.3.15 env-file==2020.12.3" \
-t puckel/docker-airflow docker-airflow/

# Run container
docker-compose up -d