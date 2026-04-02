import polars as pl
import json
import s3fs
import logging
from itertools import chain
import gc
import re
from .write_to_database import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

BNPL_LOOKUP_LIST = [
    "AFFIRM",
    "KLARNA",
    "AFTERPAY",
    "SEZZLE",
    "ZIP",
    "PAYPAL PAY LATER",
    "SHOP PAY",
    "SPLITIT",
    "SUNBIT",
    "UPLIFT",
]
GAMBLING_LOOKUP_LIST = [
    "DRAFTKINGS",
    "BETFAIR",
    "BETMGM",
    "FANDUEL",
    "CAESARS PALACE ONLINE",
    "BET365",
    "CROWN COINS",
    "STAKE.US",
    "IGNITION",
    "BETONLINE",
    "BETRIVERS",
]
RIDESHARE_LOOKUP_LIST = [
    "UBER",
    "LYFT",
    "VIA",
    "REVEL",
    "ALTO",
    "HOPSKIPDRIVE",
    "ZTRIP",
    "WINGZ",
]
P2P_PLATFORM_LOOKUP_LIST = [
    "ZELLE",
    "CASH APP",
    "PAYPAL",
    "VENMO",
    "GOOGLE PAY",
    "APPLE PAY CASH",
    "REVOLUT",
    "CHIME",
    "WISE",
    "REMITLY",
]
PAYDAY_LOAN_LOOKUP_LIST = [
    "EARNIN",
    "DAVE",
    "MONEYLION",
    "BRIGIT",
    "FLOATME",
    "KLOVER",
    "EMPOWER",
    "CLEO",
    "ALBERT",
    "POSSIBLE FINANCE",
]
DSA_LOOKUP_LIST = [
    "NATIONAL DEBT RELIEF",
    "FREEDOM DEBT RELIEF",
    "ACCORDED",
    "AMERICOR",
    "BEYOND FINANCE",
    "CLEARONE ADVANTAGE",
    "PACIFIC DEBT RELIEF",
    "STRATEGIC TAX DEBT",
    "NEW ERA DEBT SOLUTIONS",
    "GUARDIAN DEBT RELIEF",
]

pattern_bnpl = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in BNPL_LOOKUP_LIST) + r")\b",
    re.IGNORECASE,
)
pattern_gambling = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in GAMBLING_LOOKUP_LIST) + r")\b",
    re.IGNORECASE,
)
pattern_rideshare = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in RIDESHARE_LOOKUP_LIST) + r")\b",
    re.IGNORECASE,
)
pattern_p2p = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in P2P_PLATFORM_LOOKUP_LIST) + r")\b",
    re.IGNORECASE,
)
pattern_payday = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in PAYDAY_LOAN_LOOKUP_LIST) + r")\b",
    re.IGNORECASE,
)
pattern_dsa = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in DSA_LOOKUP_LIST) + r")\b",
    re.IGNORECASE,
)


def process_chunk(chunk):
    chunk_id, files = chunk

    flags = list()

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
    save_to_snowflake(pl.DataFrame(flags))


def get_flags(df):

    rows = df.partition_by("APPL_KEY", include_key=True)

    all_flags = []
    for this_iteration in rows:
        accounts = this_iteration["DATA"][0]["report"]["items"][0]
        bnpl_flag = gambling_flag = rideshare_flag = p2p_flag = payday_flag = (
            dsa_flag
        ) = False
        for account in accounts["accounts"]:
            if not bnpl_flag:
                bnpl_flag = any(
                    keyword in item["original_description"].upper()
                    for item in account["transactions"]
                    for keyword in BNPL_LOOKUP_LIST
                )
            if not gambling_flag:
                gambling_flag = any(
                    keyword in item["original_description"].upper()
                    for item in account["transactions"]
                    for keyword in GAMBLING_LOOKUP_LIST
                )
            if not rideshare_flag:
                count = sum(
                    (item["original_description"].upper().count(keyword))
                    for item in account["transactions"]
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                if count >= 4:
                    rideshare_flag = True

            if not p2p_flag:
                p2p_flag = any(
                    keyword in item["original_description"].upper()
                    for item in account["transactions"]
                    for keyword in P2P_PLATFORM_LOOKUP_LIST
                )
            if not payday_flag:
                payday_flag = any(
                    keyword in item["original_description"].upper()
                    for item in account["transactions"]
                    for keyword in PAYDAY_LOAN_LOOKUP_LIST
                )
            if not dsa_flag:
                dsa_flag = any(
                    keyword in item["original_description"].upper()
                    for item in account["transactions"]
                    for keyword in DSA_LOOKUP_LIST
                )

        flags_dict = {
            "appl_key": this_iteration["APPL_KEY"][0],
            "acap_key": this_iteration["ACAP_REFR_ID"][0],
            "has_bnpl_ever": bnpl_flag,
            "has_gambling_ever": gambling_flag,
            "has_rideshare_ever": rideshare_flag,
            "has_p2p_ever": p2p_flag,
            "has_payday_ever": payday_flag,
            "has_dsa_ever": dsa_flag,
            "appl_entry_dt": this_iteration["APPL_ENTRY_DT"][0],
        }
        all_flags.append(flags_dict)

    return all_flags
