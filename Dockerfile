FROM apache/airflow:2.6.1

RUN pip install apache-airflow-providers-amazon \
    apache-airflow-providers-mongo
