from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import os
import logging
from botocore.exceptions import ClientError

# Configurações do MinIO
MINIO_ENDPOINT = 'http://minio:9000'
ACCESS_KEY =  'flashbus'
SECRET_KEY = 'flushbus'
BUCKET_RAW = 'raw/posicao'
BUCKET_TRUSTED = 'trusted'

# Inicializa o cliente S3 do Boto3
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

def list_objects(bucket_name):
    """Lista objetos no bucket e retorna uma lista."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except ClientError as e:
        logging.error(f"Error listing objects in {bucket_name}: {e}")
        return []

def download_json_from_minio(object_key):
    """Baixa o objeto JSON do MinIO."""
    try:
        s3_client.download_file(BUCKET_RAW, object_key, object_key)
        logging.info(f"Downloaded {object_key} from MinIO.")
        return object_key
    except ClientError as e:
        logging.error(f"Error downloading {object_key}: {e}")
        raise

def convert_json_to_parquet(json_file):
    """Converte o arquivo JSON para Parquet."""
    try:
        df = pd.read_json(json_file)
        parquet_file = json_file.replace('.json', '.parquet')
        df.to_parquet(parquet_file, index=False)
        logging.info(f"Converted {json_file} to {parquet_file}.")
        return parquet_file
    except Exception as e:
        logging.error(f"Error converting {json_file} to Parquet: {e}")
        raise

def upload_parquet_to_minio(parquet_file):
    """Faz o upload do arquivo Parquet para o bucket Trusted."""
    try:
        s3_client.upload_file(parquet_file, BUCKET_TRUSTED, os.path.basename(parquet_file))
        logging.info(f"Uploaded {parquet_file} to MinIO.")
        return True
    except ClientError as e:
        logging.error(f"Error uploading {parquet_file} to MinIO: {e}")
        return False

def process_data(**kwargs):
    """Processa os dados do MinIO, acionado pelo NiFi."""
    objects = list_objects(BUCKET_RAW)
    for obj in objects:
        try:
            json_file = download_json_from_minio(obj)
            parquet_file = convert_json_to_parquet(json_file)
            if upload_parquet_to_minio(parquet_file):
                # Remove o arquivo local após o upload bem-sucedido
                if os.path.exists(parquet_file):
                    os.remove(parquet_file)
                    logging.info(f"Removed local file {parquet_file}.")
        except Exception as e:
            logging.error(f"Failed processing {obj}: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),  # Início no dia anterior
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'minio_to_parquet_dag',
    default_args=default_args,
    schedule_interval=None,  # DAG acionada manualmente
)

process_task = PythonOperator(
    task_id='process_minio_data',
    python_callable=process_data,
    provide_context=True,  # Fornecer contexto para acesso a parâmetros
    dag=dag,
)