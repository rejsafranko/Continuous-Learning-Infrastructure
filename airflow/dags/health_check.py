import os
import pendulum

import airflow.operators.bash
import airflow.operators.python
import airflow.providers.amazon.aws.operators.sns
import dotenv

dotenv.load_dotenv()

AWS_CONN_ID = os.getenv("AWS_CONN_ID")
SNS_REGION = os.getenv("SNS_REGION")
SNS_TARGET_ARN = os.getenv("SNS_TARGET_ARN")

with airflow.DAG(
    "daily_health_check",
    default_args={
        "owner": "airflow",
        "start_date": pendulum.datetime(2025, 2, 10, 8, tz="Europe/Zagreb"),
    },
    schedule_interval="0 8 * * *",
    catchup=False,
) as dag:

    notification_task = airflow.providers.amazon.aws.operators.sns.SnsPublishOperator(
        task_id="health_check_notification_task",
        target_arn=SNS_TARGET_ARN,
        message="Daily health check!",
        aws_conn_id=AWS_CONN_ID,
        region_name=SNS_REGION,
    )

    bash_task = airflow.operators.bash.BashOperator(
        task_id="daily_health_check_task",
        bash_command="echo 'Task executed successfully'",
    )

    notification_task >> bash_task
