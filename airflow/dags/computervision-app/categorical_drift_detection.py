import io
import logging
import os
import pendulum

import airflow
import airflow.models
import airflow.operators.python
import airflow.providers.amazon.aws.operators.ec2
import airflow.providers.amazon.aws.operators.sns
import airflow.providers.amazon.aws.operators.ssm
import boto3
import dotenv
import pandas as pd
import scipy.stats

dotenv.load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
SNS_TARGET_ARN = os.getenv("SNS_TARGET_ARN")
AWS_CONN_ID = os.getenv("AWS_CONN_ID")
EC2_INSTANCE_ID = os.getenv("AWS_EC2_INSTANCE_ID")
SNS_REGION = os.getenv("SNS_REGION", "eu-central-1")
CATEGORY_DRIFT_VAR = "category_drift_detected"
SIGNIFICANCE_LEVEL = 0.05
S3_BUCKET_NAME = "computervision-app/data/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_env_vars() -> None:
    """Ensure all necessary environment variables are set."""
    required_vars = {
        "AWS_SECRET_KEY": AWS_SECRET_KEY,
        "AWS_ACCESS_KEY": AWS_ACCESS_KEY,
        "SNS_TARGET_ARN": SNS_TARGET_ARN,
        "AWS_CONN_ID": AWS_CONN_ID,
        "EC2_INSTANCE_ID": EC2_INSTANCE_ID,
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")
    return True


s3_client = (
    boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=SNS_REGION,
    )
    if validate_env_vars()
    else None
)

assert s3_client is not None, "S3 Client is not configured, check .env variables"


def get_s3_data(bucket_name: str, file_key: str) -> pd.DataFrame:
    """Fetch CSV data from S3 and return it as a Pandas DataFrame."""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        logger.info(f"Data retrieved from S3: {file_key}")
        return df
    except Exception as e:
        logger.error(f"Error retrieving S3 data: {e}")
        raise


def get_current_distribution(bucket_name: str) -> dict[str, int]:
    """Retrieve the initial category distribution from S3."""
    df_distribution = get_s3_data(bucket_name, "class_distribution.csv")
    return df_distribution.set_index("label")["count"].to_dict()


def count_images_per_category(bucket_name: str) -> dict[str, int]:
    """Count the number of images per category from labels.csv in S3."""
    df_labels = get_s3_data(bucket_name, "labels.csv")
    return df_labels["label"].value_counts().to_dict()


def compare_with_current_distribution(bucket_name: str):
    """Perform chi-square test to detect category shifts."""
    try:
        current_distribution = get_current_distribution(bucket_name)
        new_distribution = count_images_per_category(bucket_name)

        current_series = pd.Series(current_distribution)
        new_series = pd.Series(new_distribution)

        _, p_value = scipy.stats.chisquare(new_series, current_series)

        airflow.models.Variable.set(
            CATEGORY_DRIFT_VAR, str(p_value < SIGNIFICANCE_LEVEL)
        )

    except Exception as e:
        logger.error(f"Error during comparison: {e}")
        raise


def check_and_notify_category_drift(**kwargs):
    """Check if category drift is detected and trigger SNS notification if needed."""
    drift_detected = (
        airflow.models.Variable.get(CATEGORY_DRIFT_VAR, default_var=False) == True
    )
    if drift_detected:
        logger.info("Drift detected. Triggering notification.")
        return "drift_notification_task"
    else:
        logger.info("No drift detected. Skipping notification.")
        return "no_op_task"


with airflow.DAG(
    dag_id="label_distribution_detection_with_chisq_s3",
    default_args={
        "owner": "airflow",
        "start_date": pendulum.datetime(2025, 2, 10, 8, tz="Europe/Zagreb"),
        "retries": 1,
    },
    description="Detect label drift in image categories on S3 with Statistical Test",
    schedule_interval="0 8 10 * *",
    catchup=False,
) as dag:

    task_count_images = airflow.operators.python.PythonOperator(
        task_id="count_images",
        python_callable=count_images_per_category,
        op_kwargs={"bucket_name": S3_BUCKET_NAME},
    )

    task_compare_with_current_distribution = airflow.operators.python.PythonOperator(
        task_id="compare_with_initial_distribution",
        python_callable=compare_with_current_distribution,
        op_kwargs={"bucket_name": S3_BUCKET_NAME},
    )

    task_check_and_notify = airflow.operators.python.BranchPythonOperator(
        task_id="check_and_notify_category_drift",
        python_callable=check_and_notify_category_drift,
        provide_context=True,
    )

    notification_task = airflow.providers.amazon.aws.operators.sns.SnsPublishOperator(
        task_id="drift_notification_task",
        target_arn=SNS_TARGET_ARN,
        message="Category drift detected.",
        aws_conn_id=AWS_CONN_ID,
        region_name=SNS_REGION,
        trigger_rule="none_failed_or_skipped",
    )

    task_start_training_machine = (
        airflow.providers.amazon.aws.operators.ec2.EC2StartInstanceOperator(
            task_id="start_ec2_training",
            instance_id=EC2_INSTANCE_ID,
            aws_conn_id=AWS_CONN_ID,
            region_name=SNS_REGION,
            trigger_rule="none_failed_or_skipped",
        )
    )

    task_run_training_script = (
        airflow.providers.amazon.aws.operators.ssm.SsmCommandOperator(
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
            region_name=SNS_REGION,
            trigger_rule="none_failed_or_skipped",
        )
    )

    no_op_task = airflow.operators.python.PythonOperator(
        task_id="no_op_task", python_callable=lambda: None
    )

    task_count_images >> task_compare_with_current_distribution >> task_check_and_notify
    task_check_and_notify >> [notification_task, no_op_task]
    task_check_and_notify >> task_start_training_machine >> task_run_training_script
