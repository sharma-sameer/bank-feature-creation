import boto3
from sagemaker.core.helper.session_helper import Session
import os
from dotenv import load_dotenv
from sagemaker.core.remote_function import remote
import math
import re
import glob
from multiprocessing import Pool
import logging
from .generate_flags import *

# Load the environment variables.
load_dotenv()
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

logger.info(f"AWS_ACCESS_KEY_ID: {os.environ.get('AWS_ACCESS_KEY_ID')}")
logger.info(
    f"AWS_SECRET_ACCESS_KEY: {'[SET]' if os.environ.get('AWS_SECRET_ACCESS_KEY') else '[NOT SET]'}"
)
logger.info(
    f"AWS_SESSION_TOKEN: {'[SET]' if os.environ.get('AWS_SESSION_TOKEN') else '[NOT SET]'}"
)

boto_session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name="us-east-1",
)

# Establish sagemaker session.
sm_session = Session(boto_session=boto_session)

try:
    role = os.getenv("AWS_ROLE")
    logger.info(f"Execution role ARN: {role}")
except ValueError:
    logger.info(
        "Could not find an execution role associated with the current environment."
    )

# Get all the arguments for sagemaker remote function as a dict.
settings = dict(
    sagemaker_session=sm_session,
    instance_count=1,
    keep_alive_period_in_seconds=21600,
    volume_size=300,
)


@remote(**settings)
def parse_parquet(bucket : str, prefix : str) -> bool:
    """Function to parse all the parquet files in the s3 directory provided.

    Args:
        bucket (str): s3 bucket in which the parquet files are stored.
        prefix (str): the directory path in the bucket where the parquet files are stored.

    Returns:
        Bool: Boolean value indicating if there has been a version change for the table.
    """
    # Get an s3 client
    s3 = boto3.client("s3")
    # list all the objects in the bucket directory.
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Filter to ensure you only get .parquet files
    all_parquet_files = [
        f"s3://{bucket}/{obj['Key']}"
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    logger.info(f"Found {len(all_parquet_files)} files.")
    # NCPU = os.cpu_count()
    NCPU = 20  # Because stupid snowflake.
    logger.info(f"Processing the files in {NCPU} threads.")

    # Parse all the files.
    files_to_process = all_parquet_files
    logger.info(
        f"Processing {len(files_to_process)}/{len(all_parquet_files)} files"
    )

    # calculate exact chunk sizes
    n = math.ceil(len(files_to_process) / NCPU)  # chunk size
    a = NCPU * n - len(files_to_process)  # number of "small" chunks
    b = NCPU - a  # number of "large" chunks
    exact_chunk_sizes = a * [n - 1] + b * [n]

    chunks = list()
    last = 0

    logger.info("Assigning the files to chunks.")
    # Assign files to chunks.
    for size in exact_chunk_sizes:
        first = last
        last = first + size
        chunk = files_to_process[first:last]
        chunks.append(chunk)

    # Mapping the chunk to a thread to process subset of the files.
    logger.info("Mapping one chunks to a thread")
    with Pool(processes=NCPU) as pool:
        results = pool.map(process_chunk, chunks)

    return any(results)


if __name__ == "__main__":
    # Get the s3 client.
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    bucket = "omwbp-s3-prod-data-science-modeling-shared-data-ue1-all"
    prefix = "Sameer_S/bank_data_tables/"

    kwargs = {"Bucket": bucket, "Delimiter": "/", "Prefix": prefix}

    data_directories = set()

    # Get list of all the directories with parquet files we want to process.

    for page in paginator.paginate(**kwargs):
        # 'CommonPrefixes' acts like a list of subdirectories
        for folder in page.get("CommonPrefixes", []):
            folder_prefix = folder.get("Prefix")
            # Check if the folder name contains 'bank-feature-tables-'
            if "bank-feature-tables-" in folder_prefix.lower():
                data_directories.add(folder_prefix)

    # Get the directories not processed yet.
    completed = set()

    if Path("completed.txt").is_file():
        with open("completed.txt", "r") as file:
            completed = {line.strip() for line in file}

    data_directories = data_directories - completed
    logger.info(f"The directories to look for the data in: {data_directories}")

    # Loop over the directories to be processed and get the data.
    for data_directory in data_directories:
        logger.info(
            f"Getting list of all the parquet files in the directory s3://{bucket}/{data_directory}"
        )
        result = parse_parquet(bucket, data_directory)

        if result:
            config_path = Path.cwd() / "config" / "table_config.yaml"

            with open(config_path, "r") as f:
                config = yaml.load(f)

            config["table"][1]["version"] += 1
            logger.info("Updating the table version in config file.")
            with open(config_path, "w") as f:
                yaml.dump(config, f)

        with open("completed.txt", "a") as f:
            f.write(f"{data_directory}\n")

    logger.info("Processed all parquet files.")
