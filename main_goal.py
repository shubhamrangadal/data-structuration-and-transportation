import enum
from datetime import datetime, timedelta
from typing import Optional
import json
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


def read_flight_data(ds, **kwargs):
    # Get flight data from OpenSky API for CDG airport on 2022-12-01
    response = requests.get(
        'https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1609459200&end=1609545600')
    flight_data = response.json()

    # Return flight data as dictionary
    return {'flight_data': flight_data}


def write_to_file(ds, **kwargs):
    # Read flight data from the previous task
    flight_data = kwargs['ti'].xcom_pull(task_ids='read_flight_data', key='flight_data')

    # Write flight data to a JSON file
    with open('flight_data.json', 'w') as file:
        file.write(json.dumps(flight_data))


# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2022-12-01',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 1 * * *'
}

# Create the DAG
dag = DAG(
    'flight_data_pipeline',
    default_args=default_args,
    catchup=False
)

# Create the tasks
read_task = PythonOperator(
    task_id='read_flight_data',
    python_callable=read_flight_data,
    provide_context=True,
    op_args=[],
    op_kwargs={},
    dag=dag
)

write_task = PythonOperator(
    task_id='write_to_file',
    python_callable=write_to_file,
    provide_context=True,
    dag=dag
)

# Set the dependencies between the tasks
read_task >> write_task
