import pandas
import pandas_gbq

from datetime import timedelta, datetime
from os import environ

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from google.oauth2 import service_account
from google.cloud import storage

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
        "table_id": environ.get('gcp_bq_dataset_id') + 'taxi_data',
        "workdir": '/tmp/taxi_data/',
        "taxi_data_bucket": environ.get('data_gcp_carto_bucket')
    }
}

dag = DAG('ingest_taxi_data', default_args=default_args,
          schedule_interval=None, catchup=False,
          max_active_runs=1, concurrency=4)

blobs = []

storage_client = storage.Client.create_anonymous_client()

cleanup_workspace = "rm -rf {}".format(default_args["params"]["workdir"])

create_workspace = "mkdir {}".format(default_args["params"]["workdir"])

columns = [
    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
    'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RateCodeID',
    'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type',
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
    'improvement_surcharge', 'total_amount'
]

# NOTE: forcing most offending columns to strings...will try to perform the cast on big query side
dtypes = {
    'VendorID': int, 'tpep_pickup_datetime': 'str', 'tpep_dropoff_datetime': 'str', 'passenger_count': 'str',
    'trip_distance': 'str', 'pickup_longitude': 'float', 'pickup_latitude': 'float', 'RateCodeID': 'str',
    'store_and_fwd_flag': 'str', 'dropoff_longitude': 'float', 'dropoff_latitude': 'float', 'payment_type': 'str',
    'fare_amount': 'float', 'extra': 'float', 'mta_tax': 'str', 'tip_amount': 'str', 'tolls_amount': 'str',
    'improvement_surcharge': 'str', 'total_amount': 'str'
}

dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

workdir = BashOperator(
    task_id='create_workdir',
    bash_command=create_workspace,
    dag=dag
)

cleanup = BashOperator(
    task_id='clean_workdir',
    bash_command=cleanup_workspace,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
    depends_on_past=False
)


def list_blobs_in_public_gbq(bucket, prefix='.csv.zip'):
    blob_ls = storage_client.list_blobs(bucket)
    for blob in blob_ls:
        if prefix in blob.name:
            blobs.append(blob)


def ingest_blobs_to_gbq(blob):
    # Workaround to skip first row on files containing headers (*_00.csv.zip)
    rn = 0
    if '_00.csv.zip' in blob.name:
        rn = 1

    df = pandas.read_csv(default_args["params"]["taxi_data_bucket"] + blob.name,
                         skiprows=rn,
                         header=None,
                         names=columns,
                         dtype=dtypes,
                         sep="\t",
                         parse_dates=dates,
                         error_bad_lines=False,
                         warn_bad_lines=True,
                         skip_blank_lines=True,
                         low_memory=False)

    pandas_gbq.to_gbq(
        dataframe=pandas.DataFrame(df),
        destination_table="carto.taxi_data",
        project_id="carto-ds",
        credentials=gcp_credentials,
        if_exists='append')


# Fixme: string from env not allowed?
list_blobs_in_public_gbq("data_eng_test")

with dag:
    cleanup
    for b in blobs:
        ingest = PythonOperator(
            task_id='ingest_taxi_data_{}'.format(b.name),
            python_callable=ingest_blobs_to_gbq,
            op_args=[b],
            task_concurrency=2,  # it seems we are limited to 2 concurrent uploads...
            dag=dag
        )

        workdir >> ingest
