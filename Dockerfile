FROM apache/airflow:3.2.2

USER root

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libffi-dev \
    libssl-dev \
    && apt-get clean
RUN apt-get update && apt-get install -y docker.io


USER airflow

COPY airflow/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
