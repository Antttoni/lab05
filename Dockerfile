FROM python:3.8.13
RUN echo "nameserver 8.8.8.8" >> /etc/resolv.conf
RUN pip install --user psycopg2-binary==2.9.3 apache-airflow==2.3.0
RUN pip install pandas==1.5.1 clickhouse-driver==0.2.4 redis==4.3.5

WORKDIR /usr/local/airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV PATH=/root/.local/bin:$PATH

COPY ./airflow.cfg /usr/local/airflow/airflow.cfg
COPY ./anton_dolgikh_lab05_dag.py /usr/local/airflow/dags/anton_dolgikh_lab05_dag.py