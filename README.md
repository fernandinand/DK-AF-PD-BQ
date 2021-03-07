# DK-AF-PD-BQ playground

## Nomenclature

# DK - Docker
# AF - Airflow
# PD - Pandas
# BQ - BigQuery

# deploy
`docker-compose up -d`

# setup your env
Create an `.env` from `.env.template` file and fill contents:
* gcp_auth_file - place your GCP JSON auth file under `auth/` folder
* gcp_bq_project_id - your GCP project id name
* gcp_bq_dataset_id - your GCP dataset name
* afl_workdir - local path to Airflow workdir

# Drill down some data
Check minimal report/sql samples in file `report/summary.sql`
