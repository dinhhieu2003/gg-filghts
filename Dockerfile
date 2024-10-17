FROM python:3.11-bullseye

RUN echo "Hello World!"

COPY requirements.txt /opt/airflow/

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
