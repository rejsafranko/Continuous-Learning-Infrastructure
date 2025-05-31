FROM apache/airflow:2.10.3

USER root

WORKDIR /opt/airflow

COPY . /opt/airflow

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/init_airflow.sh /opt/airflow/scripts/init_airflow.sh
RUN chmod +x /opt/airflow/scripts/init_airflow.sh

RUN chown -R airflow: /opt/airflow

USER airflow

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "/opt/airflow/scripts/init_airflow.sh"]
