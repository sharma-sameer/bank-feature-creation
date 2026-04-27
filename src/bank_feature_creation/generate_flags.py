import polars as pl
import json
import logging
from itertools import chain
import re
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import csv
import itertools
from typing import List, Dict
from pathlib import Path
import polars.selectors as cs

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Read all the global variables.
list_path = Path.cwd() / "lookup-lists"

with open(list_path / "BNPL_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    BNPL_LOOKUP_LIST = list(
        {item.upper() for item in itertools.chain.from_iterable(reader)}
    )

with open(list_path / "Gambling_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    GAMBLING_LOOKUP_LIST = list(
        {item.upper() for item in itertools.chain.from_iterable(reader)}
    )

with open(list_path / "Rideshare_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    RIDESHARE_LOOKUP_LIST = list(
        {item.upper() for item in itertools.chain.from_iterable(reader)}
    )

with open(
    list_path / "P2P_Platform_Lookup_List.csv", mode="r", newline=""
) as f:
    reader = csv.reader(f)
    P2P_PLATFORM_LOOKUP_LIST = list(
        {item.upper() for item in itertools.chain.from_iterable(reader)}
    )

with open(list_path / "Payday_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    PAYDAY_LOAN_LOOKUP_LIST = list(
        {item.upper() for item in itertools.chain.from_iterable(reader)}
    )

with open(list_path / "DSA_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    DSA_LOOKUP_LIST = list(
        {item.upper() for item in itertools.chain.from_iterable(reader)}
    )

with open(
    list_path / "Delivery_Services_Lookup_List.csv", mode="r", newline=""
) as f:
    reader = csv.reader(f)
    DELIVERY_SERVICES_LOOKUP_LIST = list(
        {item.upper() for item in itertools.chain.from_iterable(reader)}
    )


def process_chunk(chunk: List) -> pl.DataFrame:
    """
    Takes in a Chunk of files and processes them to extract features.

    Args:
        chunk (List): A tuple consisting of the chunk index and the list of files to be processed.

    Returns:
        pl.DataFrame: A polars dataframe with all the flags.
    """
    files = chunk
    flags = list()
    # Iterate through the files to extract the features.
    for f in files:
        df = pl.read_parquet(f)
        try:
            df = df.with_columns(
                pl.Series(
                    "DATA", [json.loads(s) for s in df["DATA"]], strict=False
                )
            )
        except Exception as e:
            logger.info(f"Data is: {df.head(1)}")
            continue

        flags.append(get_flags(df))

    logger.info("Concatenating the Dataframes.")
    return pl.concat(flags, how="diagonal")


# def process_chunk(chunk: List) -> bool:
#     """
#     Takes in a Chunk of files and processes them to extract features.

#     Args:
#         chunk (List): A tuple consisting of the chunk index and the list of files to be processed.

#     Returns:
#         Bool: A boolean value ascertaining if there was a change to table structure.
#     """
#     # Get the list of files.
#     files = chunk

#     flags = list()
#     # Iterate through the files to extract the features.
#     for f in files:
#         df = pl.read_parquet(f)
#         try:
#             df = df.with_columns(
#                 pl.Series(
#                     "DATA", [json.loads(s) for s in df["DATA"]], strict=False
#                 )
#             )
#         except Exception as e:
#             logger.info(f"Data is: {df.head(1)}")
#             continue

#         flags.append(get_flags(df))

#     logger.info("Converting this 2D list to 1D list.")
#     flags = list(chain.from_iterable(flags))
#     # Save the features to a snowflake table.
#     return save_to_snowflake(pl.DataFrame(flags))


def get_flags(df):
    # Flatten the json reports.
    try:
        df = (
            df.unnest("DATA")
            .unnest("report")
            .explode("items")
            .unnest("items")
            .explode("accounts")
            .unnest("accounts")
            .explode("transactions")
            .with_columns(
                pl.col("transactions").struct.rename_fields(
                    [
                        f"tx_{f}"
                        for f in [
                            "account_id",
                            "amount",
                            "date",
                            "iso_currency_code",
                            "original_description",
                            "pending",
                            "transaction_id",
                            "unofficial_currency_code",
                        ]
                    ]
                )
            )
            .unnest("transactions")
            .with_columns(
                pl.col("tx_date")
                .cast(pl.String)
                .str.to_date("%Y-%m-%d")  # Convert to Date object
            )
            .with_columns(
                # Matches any column name containing "date" and converts to Datetime
                (cs.contains("date") - cs.by_name("tx_date")).str.to_datetime(
                    "%+"
                )
            )
        )

    except:
        df = (
            df.unnest("DATA")
            .unnest("report")
            .explode("items")
            .unnest("items")
            .explode("accounts")
            .unnest("accounts")
            .explode("transactions")
            .with_columns(
                pl.col("transactions").struct.rename_fields(
                    [
                        f"tx_{f}"
                        for f in [
                            "account_id",
                            "account_owner",
                            "amount",
                            "category",
                            "category_id",
                            "date",
                            "date_transacted",
                            "iso_currency_code",
                            "location",
                            "name",
                            "original_description",
                            "payment_meta",
                            "pending",
                            "pending_transaction_id",
                            "transaction_id",
                            "transaction_type",
                            "unofficial_currency_code",
                        ]
                    ]
                )
            )
            .unnest("transactions")
            .with_columns(
                pl.col(["tx_date", "tx_date_transacted"])
                .cast(pl.String)
                .str.to_date("%Y-%m-%d")  # Convert to Date object
            )
            .with_columns(
                # Matches any column name containing "date" and converts to Datetime
                (
                    cs.contains("date")
                    - cs.by_name(["tx_date", "tx_date_transacted"])
                ).str.to_datetime("%+")
            )
        )
    return get_all_flags(df)


def get_all_flags(df):
    consolidated_df = get_periodic_rideshare_credits_flag(df)
    consolidated_df = consolidated_df.join(
        get_periodic_rideshare_credits_flag(df, is_delivery=True),
        on=["ACAP_REFR_ID", "APPL_KEY", "APPL_ENTRY_DT"],
        how="left",
    )
    lookup_lists = {
        "bnpl_flag": BNPL_LOOKUP_LIST,
        "dsa_flag": DSA_LOOKUP_LIST,
        "gambling_flag": GAMBLING_LOOKUP_LIST,
        "p2p_flag": P2P_PLATFORM_LOOKUP_LIST,
        "payday_flag": PAYDAY_LOAN_LOOKUP_LIST,
    }
    for lookup_list in lookup_lists.keys():
        for offset in [None, "-30d", "-60d", "-90d", "-6mo"]:
            processed_df = get_static_flags(
                df,
                lookup_list=lookup_lists[lookup_list],
                col_name=lookup_list,
                offset=offset,
            )
            # processed_df = this_df.select(pl.col(["ACAP_REFR_ID", col_name]))
            consolidated_df = consolidated_df.join(
                processed_df, on="ACAP_REFR_ID", how="left"
            )
    consolidated_df = consolidated_df.fill_null(False)
    return consolidated_df


def get_periodic_rideshare_credits_flag(df, is_delivery=False):

    # Now we have 1 row per transaction for each ACAP_REF_ID. We do not care how many accounts a customer has just look for rideshare credits throughout them.

    # Start by considering only the transactions that interest us.
    if is_delivery:
        df_filtered = filter_df(df, DELIVERY_SERVICES_LOOKUP_LIST)

    else:
        df_filtered = filter_df(df)

    # Pre-calculate the subset to scan for the window flags.
    recent_transactions_30_days = filter_df(df_filtered, offset="-30d")
    recent_transactions_60_days = filter_df(df_filtered, offset="-60d")
    recent_transactions_90_days = filter_df(df_filtered, offset="-90d")
    recent_transactions_6mo = filter_df(df_filtered, offset="-6mo")

    dataframes = {
        "": df_filtered,  # The original/main one
        "30days": recent_transactions_30_days,
        "60days": recent_transactions_60_days,
        "90days": recent_transactions_90_days,
        "6mo": recent_transactions_6mo,
    }

    consolidated_df = df.select(
        ["ACAP_REFR_ID", "APPL_KEY", "APPL_ENTRY_DT"]
    ).unique()

    # Iteratively apply logic and join
    for suffix, df in dataframes.items():
        if is_delivery:
            processed_df = get_periodic_income_flag(
                df, suffix, is_delivery=True
            )
        else:
            processed_df = get_periodic_income_flag(df, suffix)
        consolidated_df = consolidated_df.join(
            processed_df, on="ACAP_REFR_ID", how="left"
        )

    consolidated_df = consolidated_df.fill_null(False)

    return consolidated_df


def filter_df(df, lookup_list=RIDESHARE_LOOKUP_LIST, offset=None):
    if not offset:
        return df.filter(
            (pl.col("tx_amount") < 0)
            & (
                pl.col("tx_original_description").str.contains_any(
                    lookup_list, ascii_case_insensitive=True
                )
            )
        )
    else:
        return df.filter(
            (
                pl.col("tx_date").is_between(
                    pl.col("APPL_ENTRY_DT").dt.offset_by(offset),
                    pl.col("APPL_ENTRY_DT"),
                )
            )
        )


def get_periodic_income_flag(df, suffix="", is_delivery=False):
    """
    Applies the weekly/bi-weekly rolling logic and returns
    a DF with ACAP_REFR_ID and the boolean flag.
    """
    if is_delivery:
        col_name = (
            f"delivery_services_flag_{suffix}"
            if suffix
            else "delivery_services_flag_ever"
        )
    else:
        col_name = (
            f"rideshare_flag_{suffix}" if suffix else "rideshare_flag_ever"
        )

    return (
        df.sort("ACAP_REFR_ID", "tx_date")
        .with_columns(
            gap=(pl.col("tx_date").diff().dt.total_days()).over("ACAP_REFR_ID"),
        )
        .with_columns(
            is_weekly_hit=pl.col("gap").is_between(6, 8).cast(pl.Int32),
            is_biweekly_hit=pl.col("gap").is_between(13, 15).cast(pl.Int32),
        )
        .with_columns(
            weekly_count=pl.col("is_weekly_hit")
            .rolling_sum_by(window_size="1mo", by="tx_date")
            .over("ACAP_REFR_ID"),
            biweekly_count=pl.col("is_biweekly_hit")
            .rolling_sum_by(window_size="1mo", by="tx_date")
            .over("ACAP_REFR_ID"),
        )
        .group_by("ACAP_REFR_ID")
        .agg(
            **{
                col_name: (pl.col("weekly_count") >= 3).any()
                | (pl.col("biweekly_count") >= 1).any()
            }
        )
    )


def get_static_flags(df, lookup_list, col_name, offset=None):
    if offset:
        suffix = f"{offset[1:]}ays" if offset[-1] == "d" else offset[1:]
        this_col = f"{col_name}_{suffix}"
    else:
        this_col = f"{col_name}_ever"
    if not offset:
        return (
            df.group_by("ACAP_REFR_ID")
            .agg(
                # Check if ANY transaction for this ID contains the lookup keywords
                pl.col("tx_original_description")
                .str.contains_any(lookup_list, ascii_case_insensitive=True)
                .any()
                .alias(this_col)
            )
            .fill_null(False)  # Ensure IDs with no matches are False, not Null
        )

    else:
        return (
            df.group_by("ACAP_REFR_ID")
            .agg(
                (
                    # Condition 1: Date must be within the window
                    pl.col("tx_date").is_between(
                        pl.col("APPL_ENTRY_DT").dt.offset_by(offset),
                        pl.col("APPL_ENTRY_DT"),
                    )
                    &
                    # Condition 2: Description must match the lookup list
                    pl.col("tx_original_description").str.contains_any(
                        lookup_list, ascii_case_insensitive=True
                    )
                )
                .any()  # Returns True if at least one transaction meets BOTH conditions
                .alias(this_col)
            )
            .fill_null(False)
            # Ensure IDs with no matches are False, not Null
        )
