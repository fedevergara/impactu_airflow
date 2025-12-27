FROM apache/airflow:3.1.5
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.12.txt"
