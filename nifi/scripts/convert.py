import json
import pyarrow as pa
import pyarrow.parquet as pq
import os

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

    # Criar tabela Arrow a partir dos registros
    table = pa.Table.from_pylist(records)

    # Caminho do arquivo Parquet temporário
    parquet_file = '/tmp/posicao.parquet'  # Ajuste conforme necessário

    # Salvar a tabela como Parquet
    pq.write_table(table, parquet_file)

    return parquet_file

# Processar o FlowFile do NiFi
flow_file = session.get()  # Receba o FlowFile
if flow_file is not None:
    flow_file_content = flow_file.read()  # Lê o conteúdo do FlowFile
    json_data = json.loads(flow_file_content)  # Converte para JSON

    # Converter e salvar o Parquet
    parquet_file_path = convert_json_to_parquet(json_data)

    # Enviar o arquivo Parquet para o próximo processador
    with open(parquet_file_path, 'rb') as f:
        new_flow_file = session.create()  # Crie um novo FlowFile
        new_flow_file.write(f)  # Escreva o conteúdo do Parquet no novo FlowFile
        new_flow_file = session.putAttribute(new_flow_file, "mime.type", "application/octet-stream")
        new_flow_file = session.putAttribute(new_flow_file, "filename", "posicao.parquet")
        session.transfer(new_flow_file, REL_SUCCESS)  # Transfira para o próximo processador

    # Remova o arquivo temporário
    os.remove(parquet_file_path)  # Limpeza opcional

else:
    print("Nenhum FlowFile recebido para processar.")
