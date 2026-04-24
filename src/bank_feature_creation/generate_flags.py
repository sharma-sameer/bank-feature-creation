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
    BNPL_LOOKUP_LIST = {
        item.upper() for item in itertools.chain.from_iterable(reader)
    }

with open(list_path / "Gambling_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    GAMBLING_LOOKUP_LIST = {
        item.upper() for item in itertools.chain.from_iterable(reader)
    }

with open(list_path / "Rideshare_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    RIDESHARE_LOOKUP_LIST = {
        item.upper() for item in itertools.chain.from_iterable(reader)
    }

with open(
    list_path / "P2P_Platform_Lookup_List.csv", mode="r", newline=""
) as f:
    reader = csv.reader(f)
    P2P_PLATFORM_LOOKUP_LIST = {
        item.upper() for item in itertools.chain.from_iterable(reader)
    }

with open(list_path / "Payday_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    PAYDAY_LOAN_LOOKUP_LIST = {
        item.upper() for item in itertools.chain.from_iterable(reader)
    }

with open(list_path / "DSA_Lookup_List.csv", mode="r", newline="") as f:
    reader = csv.reader(f)
    DSA_LOOKUP_LIST = {
        item.upper() for item in itertools.chain.from_iterable(reader)
    }

with open(
    list_path / "Delivery_Services_Lookup_List.csv", mode="r", newline=""
) as f:
    reader = csv.reader(f)
    DELIVERY_SERVICES_LOOKUP_LIST = {
        item.upper() for item in itertools.chain.from_iterable(reader)
    }


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

    return all_flags(df)


# def get_flags(df: pl.DataFrame) -> List[Dict]:
#     """Parse each file and extract the boolean features. Add them to a list. Each list element is a dictionary.

#     Args:
#         df (pl.DataFrame): A polars datafeame with all the jsons in the current file.

#     Returns:
#         List[Dict]: A dictionary of all the boolean flags.
#     """
#     # Partition the df by application key.
#     rows = df.partition_by("APPL_KEY", include_key=True)

#     all_flags = []
#     # Each row represents a new application. Go through json for individual applicant to extract features of interest.
#     for this_iteration in rows:
#         # Get all the accounts for current applicant.
#         accounts = this_iteration["DATA"][0]["report"]["items"][0]

#         # Intitialize all features to False. We have features for BNPL, Gambling, P2P services, Payday Loans, DSA, Rideshare and Delivery Services.
#         bnpl_flag_ever = bnpl_flag_30days = bnpl_flag_60days = (
#             bnpl_flag_90days
#         ) = bnpl_flag_6mo = False
#         gambling_flag_ever = gambling_flag_30days = gambling_flag_60days = (
#             gambling_flag_90days
#         ) = gambling_flag_6mo = False
#         p2p_flag_ever = p2p_flag_30days = p2p_flag_60days = p2p_flag_90days = (
#             p2p_flag_6mo
#         ) = False
#         payday_flag_ever = payday_flag_30days = payday_flag_60days = (
#             payday_flag_90days
#         ) = payday_flag_6mo = False
#         dsa_flag_ever = dsa_flag_30days = dsa_flag_60days = dsa_flag_90days = (
#             dsa_flag_6mo
#         ) = False
#         rideshare_flag_ever = rideshare_flag_30days = rideshare_flag_60days = (
#             rideshare_flag_90days
#         ) = rideshare_flag_6mo = False
#         delivery_services_flag_ever = delivery_services_flag_30days = (
#             delivery_services_flag_60days
#         ) = delivery_services_flag_90days = delivery_services_flag_6mo = False

#         # For each applicant iterate through all the accounts.
#         for account in accounts["accounts"]:
#             # Initialize cutoff dates for the flags.
#             balance_updated = this_iteration["APPL_ENTRY_DT"][0]
#             cutoff_30_days = balance_updated - timedelta(days=30)
#             cutoff_60_days = balance_updated - timedelta(days=60)
#             cutoff_90_days = balance_updated - timedelta(days=90)
#             cutoff_6_mo = balance_updated - relativedelta(months=6)

#             # Pre-calculate the subset to scan for the window flags.
#             recent_transactions_30_days = [
#                 item
#                 for item in account["transactions"]
#                 if dt.strptime(item["date"], "%Y-%m-%d").date()
#                 >= cutoff_30_days
#             ]

#             recent_transactions_60_days = [
#                 item
#                 for item in account["transactions"]
#                 if dt.strptime(item["date"], "%Y-%m-%d").date()
#                 >= cutoff_60_days
#             ]

#             recent_transactions_90_days = [
#                 item
#                 for item in account["transactions"]
#                 if dt.strptime(item["date"], "%Y-%m-%d").date()
#                 >= cutoff_90_days
#             ]

#             recent_transactions_6mo = [
#                 item
#                 for item in account["transactions"]
#                 if dt.strptime(item["date"], "%Y-%m-%d").date() >= cutoff_6_mo
#             ]

#             # Look for BNPL indicators in the transaction data.
#             bnpl_flag_ever = bnpl_flag_ever or any(
#                 keyword in item["original_description"].upper()
#                 for item in account["transactions"]
#                 for keyword in BNPL_LOOKUP_LIST
#             )
#             bnpl_flag_30days = bnpl_flag_30days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_30_days
#                 for keyword in BNPL_LOOKUP_LIST
#             )
#             bnpl_flag_60days = bnpl_flag_60days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_60_days
#                 for keyword in BNPL_LOOKUP_LIST
#             )
#             bnpl_flag_90days = bnpl_flag_90days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_90_days
#                 for keyword in BNPL_LOOKUP_LIST
#             )
#             bnpl_flag_6mo = bnpl_flag_6mo or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_6mo
#                 for keyword in BNPL_LOOKUP_LIST
#             )

#             # Look for Gambling indicators in the transactions.
#             gambling_flag_ever = gambling_flag_ever or any(
#                 keyword in item["original_description"].upper()
#                 for item in account["transactions"]
#                 for keyword in GAMBLING_LOOKUP_LIST
#             )
#             gambling_flag_30days = gambling_flag_30days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_30_days
#                 for keyword in GAMBLING_LOOKUP_LIST
#             )
#             gambling_flag_60days = gambling_flag_60days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_60_days
#                 for keyword in GAMBLING_LOOKUP_LIST
#             )
#             gambling_flag_90days = gambling_flag_90days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_90_days
#                 for keyword in GAMBLING_LOOKUP_LIST
#             )
#             gambling_flag_6mo = gambling_flag_6mo or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_6mo
#                 for keyword in GAMBLING_LOOKUP_LIST
#             )

#             if (
#                 rideshare_flag_ever
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in account["transactions"]
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 rideshare_flag_ever = True

#             if (
#                 rideshare_flag_30days
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_30_days
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 rideshare_flag_30days = True

#             if (
#                 rideshare_flag_60days
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_60_days
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 rideshare_flag_60days = True

#             if (
#                 rideshare_flag_90days
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_90_days
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 rideshare_flag_90days = True

#             if (
#                 rideshare_flag_6mo
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_6mo
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 rideshare_flag_6mo = True

#             # Check Transactions for any P2P indicators.
#             p2p_flag_ever = p2p_flag_ever or any(
#                 keyword in item["original_description"].upper()
#                 for item in account["transactions"]
#                 for keyword in P2P_PLATFORM_LOOKUP_LIST
#             )
#             p2p_flag_30days = p2p_flag_30days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_30_days
#                 for keyword in P2P_PLATFORM_LOOKUP_LIST
#             )
#             p2p_flag_60days = p2p_flag_60days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_60_days
#                 for keyword in P2P_PLATFORM_LOOKUP_LIST
#             )
#             p2p_flag_90days = p2p_flag_90days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_90_days
#                 for keyword in P2P_PLATFORM_LOOKUP_LIST
#             )
#             p2p_flag_6mo = p2p_flag_6mo or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_6mo
#                 for keyword in P2P_PLATFORM_LOOKUP_LIST
#             )

#             # Look for Payday loan indicators in transactions.
#             payday_flag_ever = payday_flag_ever or any(
#                 keyword in item["original_description"].upper()
#                 for item in account["transactions"]
#                 for keyword in PAYDAY_LOAN_LOOKUP_LIST
#             )
#             payday_flag_30days = payday_flag_30days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_30_days
#                 for keyword in PAYDAY_LOAN_LOOKUP_LIST
#             )
#             payday_flag_60days = payday_flag_60days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_60_days
#                 for keyword in PAYDAY_LOAN_LOOKUP_LIST
#             )
#             payday_flag_90days = payday_flag_90days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_90_days
#                 for keyword in PAYDAY_LOAN_LOOKUP_LIST
#             )
#             payday_flag_6mo = payday_flag_6mo or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_6mo
#                 for keyword in PAYDAY_LOAN_LOOKUP_LIST
#             )

#             # Check for interactions with DSA in the transactions.
#             dsa_flag_ever = dsa_flag_ever or any(
#                 keyword in item["original_description"].upper()
#                 for item in account["transactions"]
#                 for keyword in DSA_LOOKUP_LIST
#             )
#             dsa_flag_30days = dsa_flag_30days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_30_days
#                 for keyword in DSA_LOOKUP_LIST
#             )
#             dsa_flag_60days = dsa_flag_60days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_60_days
#                 for keyword in DSA_LOOKUP_LIST
#             )
#             dsa_flag_90days = dsa_flag_90days or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_90_days
#                 for keyword in DSA_LOOKUP_LIST
#             )
#             dsa_flag_6mo = dsa_flag_6mo or any(
#                 keyword in item["original_description"].upper()
#                 for item in recent_transactions_6mo
#                 for keyword in DSA_LOOKUP_LIST
#             )

#             # Check for any payouts from delivery services in transactions data.
#             if (
#                 delivery_services_flag_ever
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in account["transactions"]
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 delivery_services_flag_ever = True

#             if (
#                 delivery_services_flag_30days
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_30_days
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 delivery_services_flag_30days = True

#             if (
#                 delivery_services_flag_60days
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_60_days
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 delivery_services_flag_60days = True

#             if (
#                 delivery_services_flag_90days
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_90_days
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 delivery_services_flag_90days = True

#             if (
#                 delivery_services_flag_6mo
#                 or sum(
#                     keyword in item["original_description"].upper()
#                     for item in recent_transactions_6mo
#                     if item["amount"] < 0
#                     for keyword in RIDESHARE_LOOKUP_LIST
#                 )
#                 >= 4
#             ):
#                 delivery_services_flag_6mo = True

#         # Add all the flags for this applicant to a dictionary.
#         flags_dict = {
#             "appl_key": this_iteration["APPL_KEY"][0],
#             "acap_key": this_iteration["ACAP_REFR_ID"][0],
#             "has_bnpl_ever": bnpl_flag_ever,
#             "has_bnpl_30_days": bnpl_flag_30days,
#             "has_bnpl_60_days": bnpl_flag_60days,
#             "has_bnpl_90_days": bnpl_flag_90days,
#             "has_bnpl_6mo": bnpl_flag_6mo,
#             "has_gambling_ever": gambling_flag_ever,
#             "has_gambling_30_days": gambling_flag_30days,
#             "has_gambling_60_days": gambling_flag_60days,
#             "has_gambling_90_days": gambling_flag_90days,
#             "has_gambling_6mo": gambling_flag_6mo,
#             "has_rideshare_ever": rideshare_flag_ever,
#             "has_rideshare_30_days": rideshare_flag_30days,
#             "has_rideshare_60_days": rideshare_flag_60days,
#             "has_rideshare_90_days": rideshare_flag_90days,
#             "has_rideshare_6mo": rideshare_flag_6mo,
#             "has_p2p_ever": p2p_flag_ever,
#             "has_p2p_30_days": p2p_flag_30days,
#             "has_p2p_60_days": p2p_flag_60days,
#             "has_p2p_90_days": p2p_flag_90days,
#             "has_p2p_6mo": p2p_flag_6mo,
#             "has_payday_ever": payday_flag_ever,
#             "has_payday_30_days": payday_flag_30days,
#             "has_payday_60_days": payday_flag_60days,
#             "has_payday_90_days": payday_flag_90days,
#             "has_payday_6mo": payday_flag_6mo,
#             "has_dsa_ever": dsa_flag_ever,
#             "has_dsa_30_days": dsa_flag_30days,
#             "has_dsa_60_days": dsa_flag_60days,
#             "has_dsa_90_days": dsa_flag_90days,
#             "has_dsa_6mo": dsa_flag_6mo,
#             "has_delivery_services_ever": delivery_services_flag_ever,
#             "has_delivery_services_30_days": delivery_services_flag_30days,
#             "has_delivery_services_60_days": delivery_services_flag_60days,
#             "has_delivery_services_90_days": delivery_services_flag_90days,
#             "has_delivery_services_6mo": delivery_services_flag_6mo,
#             "appl_entry_dt": this_iteration["APPL_ENTRY_DT"][0],
#         }
#         # Append the dictionary to create a list of flags for all the applicants in this file.
#         all_flags.append(flags_dict)

#     return all_flags


def get_all_flags(df):
    consolidated_df = get_periodic_rideshare_credits_flag(df)
    lookup_lists = {
        "bnpl_flag": BNPL_LOOKUP_LIST,
        "delivery_services_flag": DELIVERY_SERVICES_LOOKUP_LIST,
        "dsa_flag": DSA_LOOKUP_LIST,
        "gambling_flag": GAMBLING_LOOKUP_LIST,
        "p2p_flag": P2P_PLATFORM_LOOKUP_LIST,
        "payday_flag": PAYDAY_LOAN_LOOKUP_LIST,
    }
    for lookup_list in lookup_lists.keys():
        for offset in [None, "-30d", "-60d", "-90d", "6mo"]:
            this_df, col_name = filter_df(
                df,
                lookup_list=lookup_lists[lookup_list],
                col_name=lookup_list,
                offset=offset,
            )
            processed_df = this_df.select(pl.col(["ACAP_REFR_ID", col_name]))
            consolidated_df = consolidated_df.join(
                processed_df, on="ACAP_REFR_ID", how="left"
            )

    consolidated_df = consolidated_df.fill_null(False)
    return consolidated_df


def get_periodic_rideshare_credits_flag(df):

    # Now we have 1 row per transaction for each ACAP_REF_ID. We do not care how many accounts a customer has just look for rideshare credits throughout them.

    # Start by considering only the transactions that interest us.
    df_filtered = filter_df(df, RIDESHARE_LOOKUP_LIST, is_rideshare=True)

    # Pre-calculate the subset to scan for the window flags.
    recent_transactions_30_days = filter_df(
        df_filtered, RIDESHARE_LOOKUP_LIST, "-30d", is_rideshare=True
    )
    recent_transactions_60_days = filter_df(
        df_filtered, RIDESHARE_LOOKUP_LIST, "-60d", is_rideshare=True
    )
    recent_transactions_90_days = filter_df(
        df_filtered, RIDESHARE_LOOKUP_LIST, "-90d", is_rideshare=True
    )
    recent_transactions_6mo = filter_df(df_filtered, "-6mo", is_rideshare=True)

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
        processed_df = get_periodic_income_flag(df, suffix)
        consolidated_df = consolidated_df.join(
            processed_df, on="ACAP_REFR_ID", how="left"
        )

    consolidated_df = consolidated_df.fill_null(False)

    return consolidated_df


def filter_df(df, lookup_list, col_name=None, offset=None, is_rideshare=False):
    if col_name:
        this_col = f"{col_name}_ever" if not offset else f"{col_name}_{offset}"
    if not is_rideshare and not offset:
        return (
            df.filter(
                pl.col("tx_original_description").str.contains(
                    f"(?i){'|'.join(lookup_list)}"
                )
            ).with_columns(pl.lit(True).alias(this_col)),
            this_col,
        )
    elif not is_rideshare:
        return (
            df.filter(
                (
                    pl.col("tx_date").is_between(
                        pl.col("APPL_ENTRY_DT").dt.offset_by(offset),
                        pl.col("APPL_ENTRY_DT"),
                    )
                )
            ).with_columns(pl.lit(True).alias(this_col)),
            this_col,
        )
    elif not offset:
        return df.filter(
            (pl.col("tx_amount") < 0)
            & (
                pl.col("tx_original_description").str.contains(
                    f"(?i){'|'.join(lookup_list)}"
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


def get_periodic_income_flag(df, suffix=""):
    """
    Applies the weekly/bi-weekly rolling logic and returns
    a DF with ACAP_REFR_ID and the boolean flag.
    """
    col_name = f"rideshare_flag_{suffix}" if suffix else "rideshare_flag_ever"

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
