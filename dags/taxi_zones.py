import pandas
import geopandas
import pandas_gbq
import requests

from datetime import timedelta, datetime
from os import path, mkdir, environ

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from google.oauth2 import service_account

# FIXME: Passing env seems not to work
auth_file = environ.get('gcp_auth_file')

gcp_credentials = service_account.Credentials.from_service_account_file(
    "/auth/CARTO-DS-0de73c6e03d7.json"
)

default_args = {
    'owner': 'fernandinand',
    'email': ['fernandinand@gmail.com'],
    'start_date': datetime(2021, 3, 3),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'params': {
        "project_id": environ.get('gcp_bq_project_id'),
        "table_id": environ.get('gcp_bq_dataset_id') + 'taxi_zones',
        "workdir": environ.get('afl_workdir') + '/taxi_zones',
        "taxi_zones_uri": environ.get('data_taxi_zones_uri')
    }
}

dag = DAG('import_taxi_zone', default_args=default_args,
          schedule_interval=None, max_active_runs=1, catchup=False)

cleanup_workspace = "rm -rf {}".format(default_args["params"]["workdir"])


def check_dir(workdir):
    try:
        if not path.exists(workdir):
            mkdir(workdir)
            print("Workdir created....")
            return True
        else:
            return True
    except OSError as e:
        print(e.strerror)
        exit(0)


def download_url(url, save_path, chunk_size=2048):
    try:
        r = requests.get(url, stream=True, allow_redirects=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
        return True
    except requests.ConnectionError as e:
        exit(0)
        print(e.strerror)
    except requests.RequestException as e:
        exit(0)
        print(e.strerror)


def write_to_gbq(shape):
    shp = geopandas.read_file(shape)
    pandas_gbq.to_gbq(dataframe=pandas.DataFrame(shp.to_crs(4326)),
                      destination_table=default_args["params"]["table_id"],
                      project_id=default_args["params"]["project_id"],
                      credentials=gcp_credentials)


workdir = PythonOperator(
    task_id='create_workdir',
    python_callable=check_dir,
    op_args=[default_args["params"]["workdir"]],
    dag=dag
)

cleanup = BashOperator(
    task_id='clean_workdir',
    bash_command=cleanup_workspace,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
    depends_on_past=False
)

download = PythonOperator(
    task_id='download_taxi_zones',
    python_callable=download_url,
    op_args=[default_args["params"]["taxi_zones_uri"], default_args["params"]["workdir"] + '/taxi_zones.zip', 1024],
    dag=dag
)

ingest = PythonOperator(
    task_id='ingest_taxi_zones_to_GBQ',
    python_callable=write_to_gbq,
    op_args=[default_args["params"]["workdir"] + '/taxi_zones.zip'],
    dag=dag
)

workdir >> download >> ingest >> cleanup
