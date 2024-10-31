from pyspark.sql import SparkSession
import json
import boto3

# Configurações do MinIO
minio_endpoint = 'http://minio:9000'
access_key = 'flashbus'
secret_key = 'flushbus'
bucket_name = 'trusted/posicao'

# Criar cliente S3
s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                         aws_access_key_id=access_key,
                         aws_secret_access_key=secret_key)

# Iniciar Spark
spark = SparkSession.builder \
    .appName("JsonToParquet") \
    .getOrCreate()

def convert_json_to_parquet(json_data):
    records = []
    for entry in json_data['l']:
        for vs in entry['vs']:
            record = {
                "hr": json_data['hr'],
                "c": entry['c'],
                "cl": entry['cl'],
                "sl": entry['sl'],
                "lt0": entry['lt0'],
                "lt1": entry['lt1'],
                "qv": entry['qv'],
                "p": vs['p'],
                "a": vs['a'],
                "ta": vs['ta'],
                "py": vs['py'],
                "px": vs['px'],
                "sv": vs['sv'],
                "is": vs['is'],
            }
            records.append(record)

    try:
        df = spark.createDataFrame(records)
    except Exception as e:
        print("Erro ao criar DataFrame: " + str(e))  # Corrigido
        return

    trusted_key = bucket_name + '/posicao.parquet'

    try:
        df.write.parquet('s3a://' + trusted_key, mode='overwrite')
    except Exception as e:
        print("Erro ao salvar Parquet: " + str(e))  # Corrigido

# Acessando o conteúdo do FlowFile
flow_file_content = ''
try:
    flow_file_content = session.read(flowFile)  # Leitura do FlowFile
except Exception as e:
    print("Erro ao ler o FlowFile: " + str(e))  # Corrigido

# Converter JSON e salvar Parquet
if flow_file_content:
    json_data = json.loads(flow_file_content)
    convert_json_to_parquet(json_data)

# Encerrar Spark
spark.stop()