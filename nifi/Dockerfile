# Use a imagem oficial do Apache NiFi como base
FROM apache/nifi:latest

# Mudar para o usuário root para instalação
USER root

# Atualizar o sistema e instalar Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip 

# Instalar PySpark e boto3 diretamente em /usr/bin
#RUN pip3 install pyspark boto3
#UN pip3 install --target=/usr/bin pyspark boto3 pyarrow Spark
RUN pip3 install pyspark boto3 Spark
RUN pip3 install requests
RUN pip3 install pandas
RUN pip install --no-cache-dir pyarrow
# Reverter para o usuário nifi
USER nifi

# Definir o diretório de trabalho (opcional)
WORKDIR /opt/nifi/nifi-current

# Comando para iniciar o NiFi (opcional, pois já está definido na imagem base)
CMD ["bin/nifi.sh", "run"]