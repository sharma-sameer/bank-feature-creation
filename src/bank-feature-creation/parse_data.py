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

# from .helper import *
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

s3_path = "s3://omwbp-s3-prod-data-science-modeling-shared-data-ue1-all/Sameer_S/bank_data_tables/bank-feature-tables/*.parquet"

settings = settings = dict(
    sagemaker_session=sm_session,
    instance_count=1,
    keep_alive_period_in_seconds=21600,
    volume_size=300,
)


@remote(**settings)
def parse_parquet():
    s3 = boto3.client("s3")
    bucket = "omwbp-s3-prod-data-science-modeling-shared-data-ue1-all"
    prefix = "Sameer_S/bank_data_tables/bank-feature-tables/"

    logger.info(
        f"Getting list of all the parquet files in the directory s3://{bucket}/{prefix}"
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # 2. Extract keys and slice to get only the first 10
    # Filter to ensure you only get .parquet files
    all_parquet_files = [
        f"s3://{bucket}/{obj['Key']}"
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    logger.info(f"Found {len(all_parquet_files)} files.")
    NCPU = os.cpu_count()
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

    with Pool(processes=NCPU) as pool:
        pool.map(process_chunk, enumerate(chunks))


parse_parquet()
