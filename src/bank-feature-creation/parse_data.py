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

load_dotenv()
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
    aws_session_token=os.getenv(
        "AWS_SESSION_TOKEN"
    ),  # Required for temporary/SSO keys
    region_name="us-east-1",
)

sm_session = Session(boto_session=boto_session)

try:
    role = os.getenv("AWS_ROLE")
    print(f"Execution role ARN: {role}")
except ValueError:
    print(
        "Could not find an execution role associated with the current environment."
    )

settings = settings = dict(
    sagemaker_session=sm_session,
    instance_count=1,
    keep_alive_period_in_seconds=21600,
    volume_size=300,
)


@remote(**settings)
def parse_parquet(bucket, prefix):
    s3 = boto3.client("s3")

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # 2. Extract keys and slice to get only the first 10
    # Filter to ensure you only get .parquet files
    all_parquet_files = [
        f"s3://{bucket}/{obj['Key']}"
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    logger.info(f"Found {len(all_parquet_files)} files.")
    # NCPU = os.cpu_count()
    NCPU = 20 # Because stupid snowflake.
    logger.info(f"Processing the files in {NCPU} threads.")

    # Parse first 25% files only.
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

    for size in exact_chunk_sizes:
        first = last
        last = first + size
        chunk = files_to_process[first:last]
        chunks.append(chunk)

    def get_int_suffix(s):
        suffix = re.match("chunks(.*)", s).group(1)
        try:
            return int(suffix)
        except:
            return 0

    chunkfiles = glob.glob("chunks*")
    if len(chunkfiles) == 0:
        m = 0
    else:
        m = max(get_int_suffix(c) for c in glob.glob("chunks*"))

    logger.info("Mapping one chunks to a thread")
    with Pool(processes=NCPU) as pool:
        pool.map(process_chunk, enumerate(chunks))


s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")

bucket = "omwbp-s3-prod-data-science-modeling-shared-data-ue1-all"
prefix = "Sameer_S/bank_data_tables/"

kwargs = {"Bucket": bucket, "Delimiter": "/", "Prefix": prefix}

data_directories = set()

for page in paginator.paginate(**kwargs):
    # 'CommonPrefixes' acts like a list of subdirectories
    for folder in page.get("CommonPrefixes", []):
        folder_prefix = folder.get("Prefix")
        # Check if the folder name contains 'bank-feature-tables-'
        if "bank-feature-tables-" in folder_prefix.lower():
            data_directories.add(folder_prefix)

completed = set()

if Path("completed.txt").is_file():
    with open('completed.txt', 'r') as file:
        # .strip() removes newline characters (\n) and surrounding whitespace
        completed = {line.strip() for line in file}

data_directories = data_directories - completed
logger.info(f"The directories to look for the data in: {data_directories}")

for data_directory in data_directories:
    logger.info(
        f"Getting list of all the parquet files in the directory s3://{bucket}/{data_directory}"
    )
    parse_parquet(bucket, data_directory)
    with open("completed.txt", 'a') as f:
        f.write(f"{data_directory}\n")

logger.info("Processed all parquet files.")
