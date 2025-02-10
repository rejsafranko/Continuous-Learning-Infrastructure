import argparse
import logging
import os

import boto3
import dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

dotenv.load_dotenv()

AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")


def validate_env_vars() -> None:
    """Ensure all necessary environment variables are set."""
    required_vars = {
        "AWS_SECRET_KEY": AWS_SECRET_KEY,
        "AWS_ACCESS_KEY": AWS_ACCESS_KEY,
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata_file_path", type=str, required=True)
    parser.add_argument("--bucket_name", type=str, required=True)
    return parser.parse_args()


def upload_metadata_to_s3(file_path: str, bucket_name: str) -> None:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    file_name = os.path.basename(file_path)

    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception as e:
        s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' created.")

    try:
        s3_client.upload_file(file_path, bucket_name, file_name)
        logging.info(f"Successfully uploaded {file_name} to {bucket_name}.")
    except Exception as e:
        logging.error(f"Failed to upload {file_name}: {e}")


def main(args: argparse.Namespace) -> None:
    metadata_file_path: str = args.metadata_file_path
    bucket_name: str = args.bucket_name
    validate_env_vars()
    upload_metadata_to_s3(file_path=metadata_file_path, bucket_name=bucket_name)


if __name__ == "__main__":
    args = parse_args()
    main(args)
