import argparse
import concurrent.futures
import logging
import os

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

# ------------------ Config ------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

IMG_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".npy")
DEFAULT_REGION = "eu-central-1"

# ------------------ Utilities ------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload image dataset to S3.")
    parser.add_argument("--dataset_dir", type=str, required=True, help="Path to dataset directory.")
    parser.add_argument("--bucket_name", type=str, required=True, help="S3 bucket name.")
    parser.add_argument("--s3_prefix", type=str, default="", help="Prefix path in the S3 bucket.")
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


def gather_image_files(dataset_dir: str, s3_prefix: str) -> list[tuple[str, str]]:
    file_pairs = []
    for root, _, files in os.walk(dataset_dir):
        for file in files:
            if file.lower().endswith(IMG_EXTENSIONS):
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, start=dataset_dir)
                s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                file_pairs.append((local_path, s3_key))
    return file_pairs


def upload_image(s3_client, bucket_name: str, file_path: str, s3_key: str) -> None:
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logging.info(f"Uploaded: {s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload {s3_key}: {e}", exc_info=True)


def upload_images_to_s3(bucket_name: str, directory_path: str, s3_prefix: str, region: str) -> None:
    s3_client = boto3.client("s3", region_name=region)

    ensure_bucket_exists(s3_client, bucket_name, region)

    file_pairs = gather_image_files(directory_path, s3_prefix)
    logging.info(f"Discovered {len(file_pairs)} files to upload.")

    max_workers = min(32, (os.cpu_count() or 1) * 2)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(tqdm(
            executor.map(
                lambda pair: upload_image(s3_client, bucket_name, *pair),
                file_pairs
            ),
            total=len(file_pairs),
            desc="Uploading images"
        ))


def main() -> None:
    args = parse_args()
    upload_images_to_s3(
        bucket_name=args.bucket_name,
        directory_path=args.dataset_dir,
        s3_prefix=args.s3_prefix,
        region=DEFAULT_REGION
    )


if __name__ == "__main__":
    main()
