import calendar
import logging
import time

import requests
import json
import sqlite3
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator


def read_data():
    now_datetime = datetime.now()
    now_unix_time = int(time.mktime(now_datetime.utctimetuple()))

    seven_days_before_datetime = now_datetime - timedelta(days=7)
    seven_days_before_unix = int(time.mktime(seven_days_before_datetime.utctimetuple()))

    url = f"https://opensky-network.org/api/flights/aircraft?icao24=3c675a&begin={seven_days_before_unix}&end={now_unix_time}"

    logging.info(url)
    response = requests.get(url)
    logging.info(response)
    flights = response.json()
    return {"flights": flights}


def transform_data(**kwargs):
    flights = kwargs['ti'].xcom_pull(task_ids='read_data', key='flights')

    if flights is not None:
        most_flights = None
        max_flights = 0
        for flight in flights['states']:
            if flight[1] > max_flights:
                most_flights = flight[1]
                max_flights = flight[0]
        return {"most_flights": most_flights}
    else:
        return None


def write_data(**kwargs):
    flights = kwargs['ti'].xcom_pull(task_ids='transform_data', key="most_flights")
    with open('flights.json', 'w') as json_file:
        json.dump(flights, json_file)


def write_to_db(**kwargs):
    conn = sqlite3.connect('flights.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS flights (airport text, flights int)''')
    flights = kwargs['ti'].xcom_pull(task_ids='transform_data')

    if flights is not None:
        c.execute(f"INSERT INTO flights VALUES ('{flights['most_flights']}', 1)")
        conn.commit()
        conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'flight_tracker',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    catchup=False,
    description='Tracks flight data from the OpenSky API'
)

read_task = PythonOperator(
    task_id='read_data',
    python_callable=read_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

write_task = PythonOperator(
    task_id='write_data',
    python_callable=write_data,
    provide_context=True,
    dag=dag,
)

write_to_db_task = PythonOperator(
    task_id='write_to_db',
    python_callable=write_to_db,
    provide_context=True,
    dag=dag,
)

var = read_task >> transform_task >> write_task >> write_to_db_task
