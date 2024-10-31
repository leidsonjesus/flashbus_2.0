import json
import os
import requests
import hashlib
import hmac
import datetime
import csv

# Configurações do MinIO
minio_endpoint = 'http://minio:9000'
access_key = 'flashbus'
secret_key = 'flushbus'
bucket_name = 'trusted/posicao'

def sign_request(method, bucket, object_key):
    now = datetime.datetime.utcnow()
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = now.strftime('%Y%m%d')

    host = minio_endpoint.split('//')[1]
    canonical_request = "{}\n/{}/{}\n\nhost:{}\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\nUNSIGNED-PAYLOAD".format(
        method, bucket, object_key, host, amz_date
    )

    string_to_sign = "AWS4-HMAC-SHA256\n{}\n{}/{}/s3/aws4_request\n{}".format(
        amz_date, date_stamp, bucket, hashlib.sha256(canonical_request.encode()).hexdigest()
    )

    signing_key = hmac.new(('AWS4' + secret_key).encode(), date_stamp.encode(), hashlib.sha256)
    signing_key = hmac.new(signing_key.digest(), 's3'.encode(), hashlib.sha256)
    signing_key = hmac.new(signing_key.digest(), 'aws4_request'.encode(), hashlib.sha256)
    signing_key = hmac.new(signing_key.digest(), string_to_sign.encode(), hashlib.sha256)

    return signing_key.hexdigest(), amz_date

def upload_file_to_s3(file_path, object_key):
    with open(file_path, 'rb') as f:
        content = f.read()

    method = 'PUT'
    signed_request, amz_date = sign_request(method, bucket_name, object_key)

    headers = {
        'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
        'x-amz-date': amz_date,
        'Authorization': "AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}".format(
            access_key, amz_date, bucket_name, signed_request
        )
    }

    response = requests.put("{}/{}/{}".format(minio_endpoint, bucket_name, object_key), headers=headers, data=content)

    if response.status_code == 200:
        print('Upload bem-sucedido!')
    else:
        print('Erro ao fazer upload:', response.text)

def convert_json_to_csv(json_data):
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

    # Caminho do arquivo CSV temporário
    csv_file = '/tmp/posicao.csv'  # Usar /tmp para evitar conflitos
    
    # Salvar os registros como CSV
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    # Fazer o upload do CSV para o MinIO
    upload_file_to_s3(csv_file, 'posicao.csv')

    # Limpar o arquivo temporário
    os.remove(csv_file)

# Processar o FlowFile do NiFi
flow_file_content = None  # Aqui você receberá o conteúdo do FlowFile

# Verifique se o conteúdo do FlowFile não é vazio antes de processar
if flow_file_content:
    json_data = json.loads(flow_file_content)
    convert_json_to_csv(json_data)
else:
    print("Nenhum conteúdo para processar.")
