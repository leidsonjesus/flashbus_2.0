FROM apache/airflow:2.7.3

# Instalar dependências do sistema
USER root
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    build-essential \
    python3-dev \
    gcc \
    libffi-dev \
    libssl-dev \
    && apt-get clean

# Configurar JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
ENV PATH=$JAVA_HOME/bin:$PATH

# Mudar para o usuário airflow
USER airflow

# Inicializar o banco de dados
RUN airflow db init
RUN pip install apache-airflow-providers-cncf-kubernetes
RUN pip install apache-airflow-providers-celery
RUN pip install redis  
RUN pip install minio

# Criar diretório e alterar permissões

# Instalar dependências do Python
RUN pip install --upgrade pip && \
    pip install boto3 botocore apache-airflow-providers-amazon

 
RUN airflow connections add \
    's3_minio' \
    --conn-type 'S3' \
    --conn-extra '{"aws_access_key_id": "flashbus", "aws_secret_access_key": "flashbus", "host": "http://minio:9000"}'

CMD ["bash", "-c", "airflow webserver"]
