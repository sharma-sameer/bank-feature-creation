import os
import snowflake.connector as snf
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
from snowflake.connector.connection import SnowflakeConnection
import json
import logging

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

os.environ["AWS_DEFAULT_REGION"] = (
    "us-east-1"  # Set default region for botocore
)


def get_connector() -> SnowflakeConnection:
    """
    Function to establish connection with snowflake and return the connection object.

    Args:
        None
    Returns:
        ctx (SnowflakeConnection): A connection object to the snowflake database.
    """
    # . Establish connection with the snowflake database.
    logger.info("Establishing connection with the snowflake database.")
    client = botocore.session.get_session().create_client("secretsmanager")
    cache_config = SecretCacheConfig()
    cache = SecretCache(config=cache_config, client=client)
    secret = cache.get_secret_string("snowflake/user-login")
    secret_json = json.loads(secret)

    crdntls = {
        "user": secret_json["username"],
        "password": secret_json["password"],
        "account": "onemainfinancial-prod",
    }
    conn = snf.connect(**crdntls)

    # Return the connection object.
    logger.info(
        "Established connection to snowflake. Returning the connection object."
    )
    return conn
