# import os
# from pathlib import Path
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from google.cloud import storage
# import pandas as pd
# from io import StringIO
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateEmptyDatasetOperator,
#     BigQueryCreateExternalTableOperator,
#     BigQueryDeleteDatasetOperator,
# )

# DAG_ID = 'BQ_cs_buff'
# DATASET_NAME = 'projeto_cs_buff'

# class etl:
#     def process (self, element, idx = beam.DoFn.ElementParam):
        


# with DAG(
#     DAG_ID,
#     schedule = '@once',
#     start_date = datetime(2025, 1, 1),
#     catchup = False,
#     tags = ['cs', 'buff']
# ) as dag:
    
    
    
#     create_external_table = BigQueryCreateExternalTableOperator(
#         task_id = 'create_external_table',
#         destination_project_dataset_table = f'{DATASET_NAME}.external_table',
#         source_objects = 'gs://projeto_cs_buff/compras.csv'
        
#     )


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
from google.cloud import bigquery
import os

INPUT_PATH = '/path/para/seu/csv/compras.csv'
BQ_TABLE = 'seu_projeto.seu_dataset.sua_tabela'


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
    regex_pattern = f"({'|'.join(wear)})"
    df['wear'] = df['Items'].str.extract(regex_pattern, expand=False)

    df = df.rename(columns={'Time(GMT+8)': 'Time'})
    df['store_link'] = df['Items'].str.extract(r'HYPERLINK\("([^"]+)"')
    df['item'] = df['Items'].str.extract(r',\s*"([^"]+)"\)')
    df = df.drop(columns=['Items'])

    df['Price_CNY'] = df['Price'].str.replace('\u00a5', '', regex=True).astype(float)
    df = df.drop('Price', axis=1)

    df['Timestamp_GMT8'] = pd.to_datetime(df['Time'])
    df['Transaction_Date'] = df['Time'].dt.date
    df = df.drop('Time', axis=1)


def fetch_exchange_rate():
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
        start_date=datetime(2024, 1, 1),
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
            task_id='fetch_exchange_rate',
            python_callable=fetch_exchange_rate
        )

        t4 = PythonOperator(
            task_id='load_to_bigquery',
            python_callable=load_to_bigquery
        )

        t1 >> t2 >> t3 >> t4

    return dag


globals()['csgo_compras_transformations'] = build_dag()
