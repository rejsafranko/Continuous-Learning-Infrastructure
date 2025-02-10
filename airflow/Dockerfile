FROM apache/airflow:2.10.3

ARG AWS_ACCESS_KEY
ARG AWS_SECRET_KEY
ARG AIRFLOW_HOME
ARG AIRFLOW_FIRSTNAME
ARG AIRFLOW_LASTNAME
ARG AIRFLOW_EMAIL
ARG AIRFLOW_PASSWORD

ENV AWS_ACCESS_KEY=${AWS_ACCESS_KEY} \
    AWS_SECRET_KEY=${AWS_SECRET_KEY} \
    AIRFLOW_HOME=${AIRFLOW_HOME} \
    AIRFLOW_FIRSTNAME=${AIRFLOW_FIRSTNAME} \
    AIRFLOW_LASTNAME=${AIRFLOW_LASTNAME} \
    AIRFLOW_EMAIL=${AIRFLOW_EMAIL} \
    AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD}

WORKDIR ${AIRFLOW_HOME}

COPY . ${AIRFLOW_HOME}

RUN pip install --no-cache-dir -r requirements.txt    

USER root
RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

RUN airflow db init

RUN airflow users create \
    --username admin \
    --firstname "$AIRFLOW_FIRSTNAME" \
    --lastname "$AIRFLOW_LASTNAME" \
    --role Admin \
    --email "$AIRFLOW_EMAIL" \
    --password Test1234

RUN airflow connections add 'aws_sns' --conn-type 'aws' --conn-extra '{"aws_access_key_id": "'$AWS_ACCESS_KEY'", "aws_secret_access_key": "'$AWS_SECRET_KEY'", "region_name": "eu-central-1"}'


EXPOSE 8080

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD ["bash", "-c", "airflow scheduler & airflow webserver"]