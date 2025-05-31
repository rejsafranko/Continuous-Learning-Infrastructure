FROM apache/airflow:2.10.3

USER root

WORKDIR /opt/airflow

COPY . /opt/airflow

RUN pip install --no-cache-dir -r requirements.txt

RUN chown -R airflow: /opt/airflow

USER airflow

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "-c", "airflow standalone"]
