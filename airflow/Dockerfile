# Airflow 2.10 + the libraries our batch tasks need.
FROM apache/airflow:2.10.4-python3.11

# Installing with the airflow version pinned stops pip from up/downgrading Airflow itself.
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.4" -r /tmp/requirements.txt
