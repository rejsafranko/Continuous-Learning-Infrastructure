import argparse
import concurrent.futures
import logging
import os

import boto3
import dotenv
import tqdm

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
    parser.add_argument("--dataset_dir", type=str, required=True)
    parser.add_argument("--bucket_name", type=str, required=True)
    return parser.parse_args()


def upload_image(s3_client, bucket_name: str, file_path: str, file_name: str) -> None:
    try:
        s3_client.upload_file(file_path, bucket_name, file_name)
    except Exception as e:
        print(f"Failed to upload {file_name}: {e}")


def upload_images_to_s3(bucket_name: str, directory_path: str) -> None:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception as e:
        s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' created.")

    file_paths = [
        (os.path.join(root, file_name), file_name)
        for root, _, files in os.walk(directory_path)
        for file_name in files
        if file_name.lower().endswith(
            (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".npy")
        )
    ]

    num_cpu_cores = os.cpu_count()

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=2 * num_cpu_cores if num_cpu_cores else 8
        ) as executor:
            list(
                tqdm.tqdm(
                    executor.map(
                        lambda file: upload_image(
                            s3_client=s3_client,
                            bucket_name=bucket_name,
                            file_name=file[1],
                            file_path=file[0],
                        ),
                        file_paths,
                    ),
                    total=len(file_paths),
                )
            )
    except MemoryError:
        print("Memory limit exceeded. Consider reducing max_workers.")
    except Exception as e:
        print(f"An error occurred: {e}")


def main(args: argparse.Namespace) -> None:
    dataset_dir: str = args.dataset_dir
    bucket_name: str = args.bucket_name
    upload_images_to_s3(bucket_name=bucket_name, directory_path=dataset_dir)


if __name__ == "__main__":
    args = parse_args()
    main(args)
