import re
import pandas as pd
from typing import Dict
from cleanflow.utils.logger import get_logger

logger = get_logger(__name__)


# =========================
# 🔹 NUMERIC CLEANING (COLUMN LEVEL)
# =========================
def clean_numeric_column(df, col, rules, tracker):
    if not rules:
        return df

    original = df[col].copy()

    series = df[col].astype(str)

    if rules.get("strip"):
        series = series.str.strip()

    if rules.get("remove_special_chars"):

        series = series.apply(
            lambda x: re.sub(r"[^\d\.\-]", "", x) if x else x
        )

    df[col] = series

    if tracker:
        changed = (original.astype(str) != df[col]).sum()
        if changed > 0:
            tracker.log(col, "Numeric cleaned", int(changed))

    return df


def apply_dtype_column(df, col, dtype, rules, tracker):
    try:
        original = df[col].copy()

        if dtype in ["int", "float"]:
            numeric_rules = rules.get("numeric_cleaning")
            df = clean_numeric_column(df, col, numeric_rules, tracker)

        if dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")

        elif dtype == "str":
            df[col] = df[col].astype(str)

        elif dtype == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")

        elif dtype == "bool":
            df[col] = df[col].astype("boolean")

        else:
            logger.warning(f"Unsupported dtype '{dtype}'")
            return df

        if tracker:
            changed = (original != df[col]).sum()
            if changed > 0:
                tracker.log(col, f"Converted to {dtype}", int(changed))

        return df

    except Exception as e:
        logger.error(f"Failed to convert column '{col}' to {dtype}: {e}")
        return df


def auto_detect_column(df, col, tracker):
    original = df[col].copy()

    numeric = pd.to_numeric(df[col], errors="coerce")
    if numeric.notna().sum() >= 0.8 * len(df):
        df[col] = numeric

        if tracker:
            changed = (original != df[col]).sum()
            if changed > 0:
                tracker.log(col, "Auto type: numeric", int(changed))

        return df

    datetime = pd.to_datetime(df[col], errors="coerce")
    if datetime.notna().sum() >= 0.8 * len(df):
        df[col] = datetime

        if tracker:
            changed = (original != df[col]).sum()
            if changed > 0:
                tracker.log(col, "Auto type: datetime", int(changed))

        return df

    return df



def fix_types(df: pd.DataFrame, config: Dict, tracker) -> pd.DataFrame:
    columns_config = config.get("columns", {})

    logger.info("Starting type fixing...")

    for col, rules in columns_config.items():
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in DataFrame")
            continue

        dtype = rules.get("dtype")

        if dtype:
            logger.info(f"Applying dtype '{dtype}' to column '{col}'")
            df = apply_dtype_column(df, col, dtype, rules, tracker)

    for col in df.columns:
        if col in columns_config and "dtype" in columns_config[col]:
            continue

        df = auto_detect_column(df, col, tracker)

    logger.info("Type fixing completed.")

    return df