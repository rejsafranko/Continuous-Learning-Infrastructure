import io
import json
import logging
import os
from datetime import datetime

import pandas as pd
import scipy.stats
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator
from airflow.providers.amazon.aws.operators.ssm import SsmCommandOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import boto3

AWS_CONN_ID = "aws_default"
S3_BUCKET = Variable.get("cv_s3_bucket", default_var="your-bucket-name")
S3_PREFIX = Variable.get("cv_s3_prefix", default_var="data/")
SNS_TARGET_ARN = Variable.get("cv_sns_target_arn")
EC2_INSTANCE_ID = Variable.get("cv_ec2_instance_id")
SIGNIFICANCE_LEVEL = 0.05
CATEGORY_DRIFT_VAR = "category_drift_detected"

logger = logging.getLogger("airflow.drift_detection")


def get_s3_client():
    return boto3.client("s3")


def get_s3_data(key: str) -> pd.DataFrame:
    client = get_s3_client()
    obj = client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}{key}")
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def detect_category_drift(**kwargs):
    try:
        current_df = get_s3_data("class_distribution.csv")
        new_df = get_s3_data("labels.csv")

        expected = current_df.set_index("label")["count"]
        observed = new_df["label"].value_counts()

        # Align and pad categories.
        all_labels = sorted(set(expected.index).union(set(observed.index)))
        expected = expected.reindex(all_labels, fill_value=1)  # avoid 0 counts
        observed = observed.reindex(all_labels, fill_value=0)

        stat, p_value = scipy.stats.chisquare(f_obs=observed, f_exp=expected)

        drift = p_value < SIGNIFICANCE_LEVEL
        Variable.set(CATEGORY_DRIFT_VAR, json.dumps({"detected": drift, "p_value": p_value}))

        logger.info(f"Drift detected: {drift}, p={p_value}")
    except Exception as e:
        logger.exception("Drift detection failed")
        raise


def branch_on_drift(**kwargs):
    result = Variable.get(CATEGORY_DRIFT_VAR, default_var=json.dumps({"detected": False}))
    result = json.loads(result)
    return "drift_notification_task" if result.get("detected") else "no_op_task"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": pd.Timedelta("5min"),
    "start_date": days_ago(1),
}

with DAG(
    dag_id="category_drift_detection_s3",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Detect category drift using chi-square test and trigger retraining",
) as dag:

    detect_drift = PythonOperator(
        task_id="detect_drift",
        python_callable=detect_category_drift,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_drift",
        python_callable=branch_on_drift,
    )

    notify = SnsPublishOperator(
        task_id="drift_notification_task",
        target_arn=SNS_TARGET_ARN,
        message="Category drift detected. Retraining pipeline triggered.",
        aws_conn_id=AWS_CONN_ID,
        region_name="eu-central-1",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    start_ec2 = EC2StartInstanceOperator(
        task_id="start_ec2_instance",
        instance_id=EC2_INSTANCE_ID,
        aws_conn_id=AWS_CONN_ID,
        region_name="eu-central-1",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    run_training = SsmCommandOperator(
        task_id="run_training_script",
        instance_ids=[EC2_INSTANCE_ID],
        document_name="AWS-RunShellScript",
        parameters={
            "commands": [
                "cd /home/Projects/computervision-ds",
                "source env/bin/activate",
                "python3 train.py",
            ]
        },
        aws_conn_id=AWS_CONN_ID,
        region_name="eu-central-1",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    no_op = PythonOperator(
        task_id="no_op_task",
        python_callable=lambda: logger.info("No drift detected — no action taken."),
    )

    detect_drift >> branch
    branch >> notify >> start_ec2 >> run_training
    branch >> no_op
