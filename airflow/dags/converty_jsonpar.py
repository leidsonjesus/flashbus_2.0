from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from minio import Minio
from minio.error import S3Error
from airflow.hooks.base import BaseHook

def connect_minio():
    """Conecta ao MinIO e retorna o cliente."""
    #conn_id = 'flashbus'
    #conn_details = BaseHook.get_connection(conn_id)

    minio_endpoint = 'http://minio:9000'
    access_key = 'flashbus'
    secret_key = 'flashbus'
    
    try:
        minio_client = Minio(
            endpoint='hhttp://minio:9000',
            access_key='flashbus',
            secret_key='flashbus',
            secure=False
        )
        logging.info(f"Conexão realizada com sucesso. {minio_client}")
        return minio_client
    except Exception as e:
        logging.info(f"erro url. {minio_client}")
        logging.error(f"Erro ao se conectar ao MinIO: {str(e)}. url {minio_client}")
        logging.info(f"erro url. {minio_client}")
        return None

BUCKET_RAW = 'raw/posicao'
BUCKET_TRUSTED = 'trusted'

def list_objects(s3_client, bucket_name):
    """Lista objetos no bucket e retorna uma lista."""
    logging.info(f"Listando objetos no bucket: {bucket_name}")
    try:
        response = s3_client.list_objects(bucket_name)
        return [obj.object_name for obj in response] if response else []
    except S3Error as e:
        logging.error(f"Erro ao listar objetos em {bucket_name}: {e}")
        return []

def download_json_from_minio(s3_client, object_key):
    """Baixa o objeto JSON do MinIO."""
    logging.info(f"Baixando objeto: {object_key} do bucket {BUCKET_RAW}")
    try:
        s3_client.fget_object(BUCKET_RAW, object_key, object_key)
        logging.info(f"Baixado {object_key} do MinIO.")
        return object_key
    except S3Error as e:
        logging.error(f"Erro ao baixar {object_key}: {e}")
        raise

def convert_json_to_parquet(json_file):
    """Converte o arquivo JSON para Parquet."""
    logging.info(f"Convertendo JSON para Parquet: {json_file}")
    try:
        df = pd.read_json(json_file)
        parquet_file = json_file.replace('.json', '.parquet')
        df.to_parquet(parquet_file, index=False)
        logging.info(f"Convertido {json_file} para {parquet_file}.")
        return parquet_file
    except Exception as e:
        logging.error(f"Erro ao converter {json_file} para Parquet: {e}")
        raise

def upload_parquet_to_minio(s3_client, parquet_file):
    """Faz o upload do arquivo Parquet para o bucket Trusted."""
    logging.info(f"Fazendo upload do arquivo Parquet para o bucket {BUCKET_TRUSTED}: {parquet_file}")
    try:
        s3_client.fput_object(BUCKET_TRUSTED, os.path.basename(parquet_file), parquet_file)
        logging.info(f"Uploaded {parquet_file} para o MinIO.")
        return True
    except S3Error as e:
        logging.error(f"Erro ao fazer upload de {parquet_file} para o MinIO: {e}")
        return False

def process_data(**kwargs):
    """Processa os dados do MinIO."""
    logging.info("Iniciando o processamento de dados...")
    
    # Conectar ao MinIO dentro da função
    s3_client = connect_minio()
    if s3_client is None:
        logging.error("Não foi possível conectar ao MinIO.")
        return
    
    objects = list_objects(s3_client, BUCKET_RAW)
    logging.info(f"Objetos encontrados: {objects}")
    
    for obj in objects:
        try:
            logging.info(f"Processando objeto: {obj}")
            json_file = download_json_from_minio(s3_client, obj)
            parquet_file = convert_json_to_parquet(json_file)
            if upload_parquet_to_minio(s3_client, parquet_file):
                # Remove o arquivo local após o upload bem-sucedido
                if os.path.exists(parquet_file):
                    os.remove(parquet_file)
                    logging.info(f"Arquivo local removido: {parquet_file}.")
        except Exception as e:
            logging.error(f"Falha ao processar {obj}: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 30),  # Defina uma data fixa
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'minio_jsonpar_dag',
    default_args=default_args,
    schedule_interval=None,  # DAG acionada manualmente
)

process_task = PythonOperator(
    task_id='process_minio_data',
    python_callable=process_data,
    provide_context=True,  # Fornecer contexto para acesso a parâmetros
    dag=dag,
)
