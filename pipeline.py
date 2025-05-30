from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud import storage
import os
from io import StringIO

bucket_gcp = 'projeto_cs_buff'
csv_compras = 'compras.csv'
temp1 = '/temporarios/df1.csv'
temp2 = '/temporarios/df2.csv'
temp3 = '/temporarios/df3.csv'
bigquery_table = 'proud-outpost-455911-s8.projeto_cs_buff.compras_final'

def read_csv():
    client = storage.Client()
    bucket = client.bucket(bucket_gcp)
    blob = bucket.blob(csv_compras)
    data = blob.download_as_text()
    df = pd.read_csv(StringIO(data))
    df.to_csv(temp1, index=False)


def transform_data():
    df = pd.read_csv(temp1)
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
    
    df.to_csv(temp2, index=False)


def cny_to_usd():
    
    df = pd.read_csv(temp2)
    
    def get_usd_rate(date_str):
        url = f"https://api.frankfurter.app/{date_str}?from=CNY&to=USD"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['rates']['USD']
        return None

    df['Rate'] = df['Transaction_Date'].astype(str).apply(get_usd_rate)
    df['Price_USD'] = df['Price_CNY'] * df['Rate']
    
    df.to_csv(temp3, index=False)


def load_to_bigquery():
    client = bigquery.Client()
    df = pd.read_csv(temp3)
    
    job = client.load_table_from_dataframe(
        df,
        bigquery_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
    )
    job.result()


dag = DAG(
    dag_id='csgo_compras_transformations',
    schedule_interval='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['csgo', 'buff']
)

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
