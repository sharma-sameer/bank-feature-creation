from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime as dt, time, date
import datetime
from .get_connection import *
import polars as pl
from typing import Optional
from ruamel.yaml import YAML
from pathlib import Path

logger.info("Getting snowflake connector to save data.")

yaml = YAML()
yaml.preserve_quotes = True  # Optional: helps keep original quoting
config_path = Path.cwd() / "config" / "table_config.yaml"
# config_path = "table_config.yaml"

with open(config_path, "r") as f:
    config = yaml.load(f)

table_name = f"{config["table"][0]["name"]}_{config["table"][1]["version"]}"


def save_to_snowflake(table_df: pl.DataFrame) -> str:
    """
    Function to save the data to snowflake.

    Args:
        table_df (pl.DataFrame): Data to be inserted.
    Returns:
        str: Status of the data insertion.
    """
    global table_name
    conn = get_connector()
    cursor = conn.cursor()
    logger.info("Using Database EDS.")
    cursor.execute("USE DATABASE EDS;")
    logger.info("Using Database SB_DATA_SCIENCE.")
    cursor.execute("USE SCHEMA SB_DATA_SCIENCE;")
    logger.info("Checking if the table already exists.")
    if (
        conn.cursor().execute(f"SHOW TABLES LIKE '{table_name}'").fetchone()
        is not None
    ):
        logger.info(
            "The table already exists. Need to check if the table structure has changed."
        )
        query = f"SELECT * FROM {table_name} LIMIT 0"
        columns = {col[0].upper() for col in cursor.execute(query).description}
        table_df_cols = {col.upper() for col in table_df.columns}

        if columns == table_df_cols:
            logger.info("There is no change to the table. ")
            insert_time = dt.now(datetime.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            create_update_table(table_df, conn)
            logger.info(
                "Data inserted successfully. Now need to update the Metadata table to reflect the refresh time."
            )

            query = """UPDATE BANK_FEATURES_METADATA 
                SET LAST_REFRESH_DATE = TO_TIMESTAMP(%s),
                DATA_AS_OF_DATE = TO_TIMESTAMP(%s) 
                WHERE TABLE_NAME = %s;"""
            max_date = pl.read_database(
                f"SELECT MAX(DATA_AS_OF_DATE) FROM BANK_FEATURES_METADATA WHERE TABLE_NAME='{table_name.upper()}'",
                conn,
            ).item(0, 0)
            max_date = dt.combine(max_date, time.min) if max_date else dt.combine(date(2024, 1, 1), time.min)
            cursor.execute(
                query,
                (
                    insert_time,
                    max(
                        dt.combine(table_df["appl_entry_dt"].max(), time.min),
                        dt.combine(max_date, time.min),
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    table_name.upper(),
                ),
            )
        else:
            logger.info(
                "New version of the table needs to be created as table definition has changed."
            )
            insert_time = dt.now(datetime.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            create_update_table(table_df, conn, version_change=True)
            logger.info(
                "Data inserted successfully. Now need to update the Metadata table to reflect the refresh time."
            )

            query = """INSERT INTO BANK_FEATURES_METADATA (TABLE_NAME, LAST_REFRESH_DATE, DATA_AS_OF_DATE)
                VALUES(%s, 
                    TO_TIMESTAMP(%s), 
                    TO_TIMESTAMP(%s));"""
            conn.cursor().execute(
                query,
                (
                    table_name.upper(),
                    insert_time,
                    table_df["appl_entry_dt"]
                    .max()
                    .strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )

    else:
        logger.info("The table needs to be created.")
        insert_time = dt.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        create_update_table(table_df, conn)
        logger.info(
            "Data inserted successfully. Now need to update the Metadata table to reflect the refresh time."
        )

        query = """INSERT INTO BANK_FEATURES_METADATA (TABLE_NAME, LAST_REFRESH_DATE, DATA_AS_OF_DATE)
            VALUES(%s, 
                   TO_TIMESTAMP(%s), 
                   TO_TIMESTAMP(%s));"""
        conn.cursor().execute(
            query,
            (
                table_name.upper(),
                insert_time,
                table_df["appl_entry_dt"].max().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    return "Saved to snowflake table successfully."


def create_update_table(
    table_df: pl.DataFrame,
    conn: SnowflakeConnection,
    version_change: bool = False,
) -> None:
    """
    Function to run the insert or create table command in snowflake.

    Args:
        table_df (pl.DataFrame): Data to be inserted.
        table_name (str): Name of the table to which the table is to be inserted.
        conn (SnowflakeConnection): A connection object to the snowflake database.
    Returns:
        None
    """
    global table_name
    if version_change:
        logger.info("Updating the table name.")
        config["table"]["version"] += 1
        table_name = f"{config["table"]["name"]}_{config["table"]["version"]}"
        logger.info(f"The new table name is {table_name}")
        logger.info("Updating the table version in config file.")
        with open(config_path, "w") as f:
            yaml.dump(config, f)

    logger.info(f"Inserting Data to table {table_name}")
    write_pandas(
        conn=conn,
        df=table_df.to_pandas(),
        use_logical_type=True,
        database="EDS",
        schema="SB_DATA_SCIENCE",
        table_name=table_name.upper(),
        auto_create_table=True,
        overwrite=False,
    )
    return
