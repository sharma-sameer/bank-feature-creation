import boto3

# from sagemaker.core.helper.session_helper import Session
import os
from dotenv import load_dotenv

# from sagemaker.core.remote_function import remote
import math
import re
import glob
from multiprocessing import Pool
import logging
from generate_flags import *
# from write_to_database import *
import s3fs

# Load the environment variables.
load_dotenv()
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

# Establish sagemaker session.
# sm_session = Session(boto_session=boto3.Session())

try:
    role = os.getenv("AWS_ROLE")
    logger.info(f"Execution role ARN: {role}")
except ValueError:
    logger.info(
        "Could not find an execution role associated with the current environment."
    )

# # Get all the arguments for sagemaker remote function as a dict.
# settings = dict(
#     sagemaker_session=sm_session,
#     instance_count=1,
#     keep_alive_period_in_seconds=21600,
#     volume_size=300,
# )


# @remote(**settings)
def parse_parquet(bucket: str, prefix: str) -> bool:
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

    return pl.concat(results, how="diagonal")


if __name__ == "__main__":

    # 1. Get the instance's rank (0 through 4)
    # SageMaker provides this via 'sagemaker_host' or environmental variables
    conf = json.loads(os.environ.get("SM_TRAINING_ENV", "{}"))
    instance_rank = conf.get("hosts", []).index(conf.get("current_host"))

    # 2. Get the specific directories for THIS instance
    all_groups = json.loads(os.environ.get("SM_HP_ALL_GROUPS"))
    input_directories = all_groups[instance_rank].split(",")
    bucket = os.environ.get("SM_HP_S3_BUCKET")

    logger.info(
        f"I am instance {instance_rank}. Processing: {input_directories}"
    )

    dfs = [parse_parquet(bucket, path) for path in input_directories]
    result = pl.concat(dfs, how="diagonal")

    s3_path = "s3://omwbp-s3-prod-data-science-modeling-shared-data-ue1-all/Sameer_S/bank_data_tables/bank-data-tables/output_updated.csv"

    logger.info(f"Writing data to file Data to table {s3_path}")

    # # Initialize the filesystem
    fs = s3fs.S3FileSystem()
    file_exists = fs.exists(s3_path)

    with fs.open(s3_path, "ab") as f:
        result.write_csv(f, include_header=not file_exists)
    # save_to_snowflake(result)
    logger.info("Process Completed Successfully.")
