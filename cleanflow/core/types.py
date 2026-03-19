import re
import pandas as pd
from typing import Dict
from cleanflow.utils.logger import get_logger

logger = get_logger(__name__)


def clean_numeric(series, rules):
    if not rules:
        return series

    series = series.astype(str)

    if rules.get("strip"):
        series = series.str.strip()

    if rules.get("remove_special_chars"):
        series = series.apply(
            lambda x: re.sub(r"[^\d\.]", "", x) if x else x
        )

    return series


def apply_dtype(series: pd.Series, dtype: str, rules: Dict) -> pd.Series:
    try:
        # apply numeric cleaning BEFORE conversion
        if dtype in ["int", "float"]:
            numeric_rules = rules.get("numeric_cleaning")
            series = clean_numeric(series, numeric_rules)

        if dtype == "int":
            return pd.to_numeric(series, errors="coerce").astype("Int64")

        elif dtype == "float":
            return pd.to_numeric(series, errors="coerce")

        elif dtype == "str":
            return series.astype(str)

        elif dtype == "datetime":
            return pd.to_datetime(series, errors="coerce")

        elif dtype == "bool":
            return series.astype("boolean")

        else:
            logger.warning(f"Unsupported dtype '{dtype}'")
            return series

    except Exception as e:
        logger.error(f"Failed to convert column '{series.name}' to {dtype}: {e}")
        return series


def auto_detect_type(series: pd.Series) -> pd.Series:
    # try numeric
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() >= 0.8 * len(series):
        logger.info(f"Auto-detected numeric column: {series.name}")
        return numeric

    # try datetime
    datetime = pd.to_datetime(series, errors="coerce")
    if datetime.notna().sum() >= 0.8 * len(series):
        logger.info(f"Auto-detected datetime column: {series.name}")
        return datetime

    return series


def fix_types(df: pd.DataFrame, config: Dict, tracker) -> pd.DataFrame:
    columns_config = config.get("columns", {})

    logger.info("Starting type fixing...")

    # apply user-defined schema
    for col, rules in columns_config.items():
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in DataFrame")
            continue

        dtype = rules.get("dtype")

        if dtype:
            logger.info(f"Applying dtype '{dtype}' to column '{col}'")
            df[col] = apply_dtype(df[col], dtype, rules)

    # auto-detect remaining columns
    for col in df.columns:
        if col in columns_config and "dtype" in columns_config[col]:
            continue

        df[col] = auto_detect_type(df[col])

    logger.info("Type fixing completed.")
    return df