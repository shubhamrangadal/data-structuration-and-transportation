from airflow import DAG
from typing import List, Dict
import json
from datetime import timedelta

from airflow.operators.python import PythonOperator

from utils import read_weather_data


def transform_data(ds, **kwargs):
    # Read weather data from the previous task
    weather_data = kwargs['ti'].xcom_pull(task_ids='read_weather_data', key='weather_data')

    # Check if weather data is not None
    if weather_data is not None:
        # Perform transformation on the weather data
        temperature = weather_data['main']['temp']

        # Return transformed data as dictionary
        return {'temperature': temperature}
    else:
        return None


def write_to_file(ds, **kwargs):
    # Read transformed data from the previous task
    temperature = kwargs['ti'].xcom_pull(task_ids='transform_data', key='temperature')

    # Write transformed data to a JSON file
    with open('weather_data.json', 'w') as file:
        file.write(json.dumps({'temperature': temperature}))


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
    'weather_data_pipeline',
    default_args=default_args,
    catchup=False
)

# Create the tasks
read_task = PythonOperator(
    task_id='read_weather_data',
    python_callable=read_weather_data,
    provide_context=True,
    op_args=['Paris', '2022-12-01'],
    op_kwargs={},
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

write_task = PythonOperator(
    task_id='write_to_file',
    python_callable=write_to_file,
    provide_context=True,
    dag=dag
)

# Set the dependencies between the tasks
read_task >> transform_task >> write_task
