FROM apache/airflow:2.5.0
COPY requirements.txt /requirements.txt
USER root
RUN apt-get update && apt-get install -y python3-dev gcc libc-dev && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt