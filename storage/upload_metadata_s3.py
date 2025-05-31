import argparse
import logging
import os

import boto3
from botocore.exceptions import ClientError

# ------------------ Config ------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DEFAULT_REGION = "eu-central-1"

# ------------------ Utilities ------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload metadata file to S3.")
    parser.add_argument("--metadata_file_path", type=str, required=True, help="Path to the metadata file.")
    parser.add_argument("--bucket_name", type=str, required=True, help="Target S3 bucket.")
    parser.add_argument("--s3_prefix", type=str, default="", help="Optional prefix path in the bucket.")
    return parser.parse_args()


def ensure_bucket_exists(s3_client, bucket_name: str, region: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' exists.")
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
            logging.info(f"Bucket '{bucket_name}' created in {region}.")
        else:
            raise e


def upload_metadata_to_s3(file_path: str, bucket_name: str, s3_prefix: str, region: str) -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Metadata file not found: {file_path}")

    s3_client = boto3.client("s3", region_name=region)

    ensure_bucket_exists(s3_client, bucket_name, region)

    file_name = os.path.basename(file_path)
    s3_key = os.path.join(s3_prefix, file_name).replace("\\", "/")

    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logging.info(f"Successfully uploaded {file_name} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload metadata to {s3_key}: {e}", exc_info=True)
        raise


def main() -> None:
    args = parse_args()
    upload_metadata_to_s3(
        file_path=args.metadata_file_path,
        bucket_name=args.bucket_name,
        s3_prefix=args.s3_prefix,
        region=DEFAULT_REGION,
    )


if __name__ == "__main__":
    main()
