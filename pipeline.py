from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
from google.cloud import bigquery
import os

INPUT_PATH = '' #ainda tenho que colocar a origem do bucket
BQ_TABLE = '' #verificar qual é o dataset no BQ


def read_csv():
    global df
    df = pd.read_csv(INPUT_PATH)


def transform_data():
    global df
    df['Time(GMT+8)'] = pd.to_datetime(df['Time(GMT+8)'])
    df = df.sort_values(by='Time(GMT+8)', ascending=True)
    df = df.reset_index(drop=True)
    df['id_transaction'] = df.index + 1

    wear = ['Battle-Scarred', 'Factory New', 'Minimal Wear', 'Field-Tested', 'Well-Worn']
    join_wear = f"({'|'.join(wear)})"
    df['wear'] = df['Items'].str.extract(join_wear, expand=False)

    df = df.rename(columns={'Time(GMT+8)': 'Time'})
    df['store_link'] = df['Items'].str.extract(r'HYPERLINK\("([^"]+)"')
    df['item'] = df['Items'].str.extract(r',\s*"([^"]+)"\)')
    df = df.drop(columns=['Items'])

    df['Price_CNY'] = df['Price'].str.replace('\u00a5', '', regex=True).astype(float)
    df = df.drop('Price', axis=1)

    df['Timestamp_GMT8'] = pd.to_datetime(df['Time'])
    df['Transaction_Date'] = df['Time'].dt.date
    df = df.drop('Time', axis=1)


def cny_to_usd():
    global df
    def get_usd_rate(date_str):
        url = f"https://api.frankfurter.app/{date_str}?from=CNY&to=USD"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['rates']['USD']
        return None

    df['Rate'] = df['Transaction_Date'].astype(str).apply(get_usd_rate)
    df['Price_USD'] = df['Price_CNY'] * df['Rate']


def load_to_bigquery():
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )
    temp_path = '/tmp/transformed_compras.csv'
    df.to_csv(temp_path, index=False)

    with open(temp_path, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            BQ_TABLE,
            job_config=job_config
        )
    job.result()


def build_dag():
    with DAG(
        dag_id='csgo_compras_transformations',
        schedule_interval='@once',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['csgo', 'buff']
    ) as dag:

        t1 = PythonOperator(
            task_id='read_csv',
            python_callable=read_csv
        )

        t2 = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data
        )

        t3 = PythonOperator(
            task_id='cny_to_usd',
            python_callable=cny_to_usd
        )

        t4 = PythonOperator(
            task_id='load_to_bigquery',
            python_callable=load_to_bigquery
        )

        t1 >> t2 >> t3 >> t4

    return dag


globals()['csgo_compras_transformations'] = build_dag()
