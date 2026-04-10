import polars as pl
import json
import logging
from itertools import chain
import re
from .write_to_database import *
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import csv
import itertools
from typing import List, Dict

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

# pattern_bnpl = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in BNPL_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_gambling = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in GAMBLING_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_rideshare = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in RIDESHARE_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_p2p = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in P2P_PLATFORM_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_payday = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in PAYDAY_LOAN_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_dsa = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in DSA_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )


def process_chunk(chunk: List) -> bool:
    """
    Takes in a Chunk of files and processes them to extract features.

    Args:
        chunk (List): A tuple consisting of the chunk index and the list of files to be processed.

    Returns:
        Bool: A boolean value ascertaining if there was a change to table structure.
    """
    # Get the list of files.
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

    logger.info("Converting this 2D list to 1D list.")
    flags = list(chain.from_iterable(flags))
    # Save the features to a snowflake table.
    return save_to_snowflake(pl.DataFrame(flags))


def get_flags(df: pl.DataFrame) -> List[Dict]:
    """Parse each file and extract the boolean features. Add them to a list. Each list element is a dictionary.

    Args:
        df (pl.DataFrame): A polars datafeame with all the jsons in the current file.

    Returns:
        List[Dict]: A dictionary of all the boolean flags.
    """
    # Partition the df by application key.
    rows = df.partition_by("APPL_KEY", include_key=True)

    all_flags = []
    # Each row represents a new application. Go through json for individual applicant to extract features of interest.
    for this_iteration in rows:
        # Get all the accounts for current applicant.
        accounts = this_iteration["DATA"][0]["report"]["items"][0]

        # Intitialize all features to False. We have features for BNPL, Gambling, P2P services, Payday Loans, DSA, Rideshare and Delivery Services.
        bnpl_flag_ever = bnpl_flag_30days = bnpl_flag_60days = (
            bnpl_flag_90days
        ) = bnpl_flag_6mo = False
        gambling_flag_ever = gambling_flag_30days = gambling_flag_60days = (
            gambling_flag_90days
        ) = gambling_flag_6mo = False
        p2p_flag_ever = p2p_flag_30days = p2p_flag_60days = p2p_flag_90days = (
            p2p_flag_6mo
        ) = False
        payday_flag_ever = payday_flag_30days = payday_flag_60days = (
            payday_flag_90days
        ) = payday_flag_6mo = False
        dsa_flag_ever = dsa_flag_30days = dsa_flag_60days = dsa_flag_90days = (
            dsa_flag_6mo
        ) = False
        rideshare_flag_ever = rideshare_flag_30days = rideshare_flag_60days = (
            rideshare_flag_90days
        ) = rideshare_flag_6mo = False
        delivery_services_flag_ever = delivery_services_flag_30days = (
            delivery_services_flag_60days
        ) = delivery_services_flag_90days = delivery_services_flag_6mo = False

        # For each applicant iterate through all the accounts.
        for account in accounts["accounts"]:
            # Initialize cutoff dates for the flags.
            balance_updated = this_iteration["APPL_ENTRY_DT"][0]
            cutoff_30_days = balance_updated - timedelta(days=30)
            cutoff_60_days = balance_updated - timedelta(days=60)
            cutoff_90_days = balance_updated - timedelta(days=90)
            cutoff_6_mo = balance_updated - relativedelta(months=6)

            # Pre-calculate the subset to scan for the window flags.
            recent_transactions_30_days = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date()
                >= cutoff_30_days
            ]

            recent_transactions_60_days = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date()
                >= cutoff_60_days
            ]

            recent_transactions_90_days = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date()
                >= cutoff_90_days
            ]

            recent_transactions_6mo = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date() >= cutoff_6_mo
            ]

            # Look for BNPL indicators in the transaction data.
            bnpl_flag_ever = bnpl_flag_ever or any(
                keyword in item["original_description"].upper()
                for item in account["transactions"]
                for keyword in BNPL_LOOKUP_LIST
            )
            bnpl_flag_30days = bnpl_flag_30days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_30_days
                for keyword in BNPL_LOOKUP_LIST
            )
            bnpl_flag_60days = bnpl_flag_60days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_60_days
                for keyword in BNPL_LOOKUP_LIST
            )
            bnpl_flag_90days = bnpl_flag_90days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_90_days
                for keyword in BNPL_LOOKUP_LIST
            )
            bnpl_flag_6mo = bnpl_flag_6mo or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_6mo
                for keyword in BNPL_LOOKUP_LIST
            )

            # Look for Gambling indicators in the transactions.
            gambling_flag_ever = gambling_flag_ever or any(
                keyword in item["original_description"].upper()
                for item in account["transactions"]
                for keyword in GAMBLING_LOOKUP_LIST
            )
            gambling_flag_30days = gambling_flag_30days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_30_days
                for keyword in GAMBLING_LOOKUP_LIST
            )
            gambling_flag_60days = gambling_flag_60days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_60_days
                for keyword in GAMBLING_LOOKUP_LIST
            )
            gambling_flag_90days = gambling_flag_90days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_90_days
                for keyword in GAMBLING_LOOKUP_LIST
            )
            gambling_flag_6mo = gambling_flag_6mo or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_6mo
                for keyword in GAMBLING_LOOKUP_LIST
            )

            if (
                rideshare_flag_ever
                or sum(
                    keyword in item["original_description"].upper()
                    for item in account["transactions"]
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                rideshare_flag_ever = True

            if (
                rideshare_flag_30days
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_30_days
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                rideshare_flag_30days = True

            if (
                rideshare_flag_60days
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_60_days
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                rideshare_flag_60days = True

            if (
                rideshare_flag_90days
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_90_days
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                rideshare_flag_90days = True

            if (
                rideshare_flag_6mo
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_6mo
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                rideshare_flag_6mo = True

            # Check Transactions for any P2P indicators.
            p2p_flag_ever = p2p_flag_ever or any(
                keyword in item["original_description"].upper()
                for item in account["transactions"]
                for keyword in P2P_PLATFORM_LOOKUP_LIST
            )
            p2p_flag_30days = p2p_flag_30days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_30_days
                for keyword in P2P_PLATFORM_LOOKUP_LIST
            )
            p2p_flag_60days = p2p_flag_60days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_60_days
                for keyword in P2P_PLATFORM_LOOKUP_LIST
            )
            p2p_flag_90days = p2p_flag_90days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_90_days
                for keyword in P2P_PLATFORM_LOOKUP_LIST
            )
            p2p_flag_6mo = p2p_flag_6mo or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_6mo
                for keyword in P2P_PLATFORM_LOOKUP_LIST
            )

            # Look for Payday loan indicators in transactions.
            payday_flag_ever = payday_flag_ever or any(
                keyword in item["original_description"].upper()
                for item in account["transactions"]
                for keyword in PAYDAY_LOAN_LOOKUP_LIST
            )
            payday_flag_30days = payday_flag_30days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_30_days
                for keyword in PAYDAY_LOAN_LOOKUP_LIST
            )
            payday_flag_60days = payday_flag_60days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_60_days
                for keyword in PAYDAY_LOAN_LOOKUP_LIST
            )
            payday_flag_90days = payday_flag_90days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_90_days
                for keyword in PAYDAY_LOAN_LOOKUP_LIST
            )
            payday_flag_6mo = payday_flag_6mo or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_6mo
                for keyword in PAYDAY_LOAN_LOOKUP_LIST
            )

            # Check for interactions with DSA in the transactions.
            dsa_flag_ever = dsa_flag_ever or any(
                keyword in item["original_description"].upper()
                for item in account["transactions"]
                for keyword in DSA_LOOKUP_LIST
            )
            dsa_flag_30days = dsa_flag_30days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_30_days
                for keyword in DSA_LOOKUP_LIST
            )
            dsa_flag_60days = dsa_flag_60days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_60_days
                for keyword in DSA_LOOKUP_LIST
            )
            dsa_flag_90days = dsa_flag_90days or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_90_days
                for keyword in DSA_LOOKUP_LIST
            )
            dsa_flag_6mo = dsa_flag_6mo or any(
                keyword in item["original_description"].upper()
                for item in recent_transactions_6mo
                for keyword in DSA_LOOKUP_LIST
            )

            # Check for any payouts from delivery services in transactions data.
            if (
                delivery_services_flag_ever
                or sum(
                    keyword in item["original_description"].upper()
                    for item in account["transactions"]
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                delivery_services_flag_ever = True

            if (
                delivery_services_flag_30days
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_30_days
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                delivery_services_flag_30days = True

            if (
                delivery_services_flag_60days
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_60_days
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                delivery_services_flag_60days = True

            if (
                delivery_services_flag_90days
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_90_days
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                delivery_services_flag_90days = True

            if (
                delivery_services_flag_6mo
                or sum(
                    keyword in item["original_description"].upper()
                    for item in recent_transactions_6mo
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                >= 4
            ):
                delivery_services_flag_6mo = True

        # Add all the flags for this applicant to a dictionary.
        flags_dict = {
            "appl_key": this_iteration["APPL_KEY"][0],
            "acap_key": this_iteration["ACAP_REFR_ID"][0],
            "has_bnpl_ever": bnpl_flag_ever,
            "has_bnpl_30_days": bnpl_flag_30days,
            "has_bnpl_60_days": bnpl_flag_60days,
            "has_bnpl_90_days": bnpl_flag_90days,
            "has_bnpl_6mo": bnpl_flag_6mo,
            "has_gambling_ever": gambling_flag_ever,
            "has_gambling_30_days": gambling_flag_30days,
            "has_gambling_60_days": gambling_flag_60days,
            "has_gambling_90_days": gambling_flag_90days,
            "has_gambling_6mo": gambling_flag_6mo,
            "has_rideshare_ever": rideshare_flag_ever,
            "has_rideshare_30_days": rideshare_flag_30days,
            "has_rideshare_60_days": rideshare_flag_60days,
            "has_rideshare_90_days": rideshare_flag_90days,
            "has_rideshare_6mo": rideshare_flag_6mo,
            "has_p2p_ever": p2p_flag_ever,
            "has_p2p_30_days": p2p_flag_30days,
            "has_p2p_60_days": p2p_flag_60days,
            "has_p2p_90_days": p2p_flag_90days,
            "has_p2p_6mo": p2p_flag_6mo,
            "has_payday_ever": payday_flag_ever,
            "has_payday_30_days": payday_flag_30days,
            "has_payday_60_days": payday_flag_60days,
            "has_payday_90_days": payday_flag_90days,
            "has_payday_6mo": payday_flag_6mo,
            "has_dsa_ever": dsa_flag_ever,
            "has_dsa_30_days": dsa_flag_30days,
            "has_dsa_60_days": dsa_flag_60days,
            "has_dsa_90_days": dsa_flag_90days,
            "has_dsa_6mo": dsa_flag_6mo,
            "has_delivery_services_ever": delivery_services_flag_ever,
            "has_delivery_services_30_days": delivery_services_flag_30days,
            "has_delivery_services_60_days": delivery_services_flag_60days,
            "has_delivery_services_90_days": delivery_services_flag_90days,
            "has_delivery_services_6mo": delivery_services_flag_6mo,
            "appl_entry_dt": this_iteration["APPL_ENTRY_DT"][0],
        }
        # Append the dictionary to create a list of flags for all the applicants in this file.
        all_flags.append(flags_dict)

    return all_flags
