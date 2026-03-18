# import lib
import pandas as pd
from typing import Dict
from cleanflow.utils.logger import get_logger


logger = get_logger()


def apply_dtype(series: pd.Series, dtype: str) -> pd.Series:
    # to safely apply the specified dtype, with error handling
    try:
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
    # attempt to auto-detect the type of a pandas Series
    original_dtype = series.dtype

    # try numeric
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() > 0 and numeric.notna().sum() >= 0.8 * len(series):
        logger.info(f"Auto-detected numeric column: {series.name}")
        return numeric

    # try datetime
    datetime = pd.to_datetime(series, errors="coerce")
    if datetime.notna().sum() > 0 and datetime.notna().sum() >= 0.8 * len(series):
        logger.info(f"Auto-detected datetime column: {series.name}")
        return datetime

    return series  # fallback (no change)


def fix_types(df: pd.DataFrame, config: Dict, tracker) -> pd.DataFrame:
    columns_config = config.get("columns", {})

    logger.info("Starting type fixing...")

    # STEP 1: Apply user-defined schema
    for col, rules in columns_config.items():
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found in DataFrame")
            continue

        dtype = rules.get("dtype")
        if dtype:
            logger.info(f"Applying dtype '{dtype}' to column '{col}'")
            df[col] = apply_dtype(df[col], dtype)

    # STEP 2: Auto-detect remaining columns
    for col in df.columns:
        if col in columns_config and "dtype" in columns_config[col]:
            continue  # already handled

        df[col] = auto_detect_type(df[col])

    logger.info("Type fixing completed.")
    return df