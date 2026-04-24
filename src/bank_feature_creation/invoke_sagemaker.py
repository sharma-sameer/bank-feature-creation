import json
import boto3
from sagemaker.pytorch import PyTorch
from sagemaker.inputs import TrainingInput
from sagemaker.session import Session
import logging
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

sm_session = Session(boto_session=boto3.Session())

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

data_directories = list(data_directories)
logger.info(f"The directories to look for the data in: {data_directories}")

logger.info("Defining Estimator and Compute settings.")

groups = [[data_directories[i]] for i in range(len(data_directories))]

group_strings = [",".join(g) for g in groups]

# 2.x uses Estimator directly rather than ModelTrainer/Compute configs
estimator = PyTorch(
    # image_uri = '607826602275.dkr.ecr.us-east-1.amazonaws.com/ecr-public/sagemaker/sagemaker-distribution:3.8.1-cpu',
    framework_version="2.0",  # Specify version instead of image_uri
    py_version="py310",
    role="arn:aws:iam::607826602275:role/omwbp-iam_role-prod-data-science-modeling-ml-ue1-all",
    instance_count=15,  # Moved from Compute config
    instance_type="ml.m5.12xlarge",  # Moved from Compute config
    volume_size=300,  # Moved from Compute config
    max_run=86400,  # Default timeout
    keep_alive_period_in_seconds=0,
    source_dir=".",  # Moved from SourceCode config
    entry_point="src/bank_feature_creation/parse_data.py",  # Moved from SourceCode config
    sagemaker_session=sm_session,
    base_job_name="sagemaker-instance-ssharma",
    hyperparameters={
        "all_groups": json.dumps(group_strings),  # Send the whole map
        "s3_bucket": bucket,
    },
)

logger.info("Estimator Created. Defining InputData.")

logger.info("Starting the training job.")
estimator.fit()

logger.info("Training job submitted.")
# The job name is accessible directly
training_job_name = estimator.latest_training_job.name
logger.info(f"Job Completed: {training_job_name}")
