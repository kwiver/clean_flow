# imoport necessary libs
from typing import Dict
import pandas as pd
from cleanflow.utils.logger import get_logger

logger = get_logger()

SUPPORTED_DTYPES = {"int", "float", "str", "datetime", "bool"}


def validate_config(config: Dict):
    # validate the structure and content of the config dictionary
    if "columns" not in config:
        raise ValueError("Config must contain 'columns' section")

    for col, rules in config["columns"].items():
        if not isinstance(rules, dict):
            raise ValueError(f"Rules for column '{col}' must be a dictionary")

        dtype = rules.get("dtype")
        if dtype and dtype not in SUPPORTED_DTYPES:
            raise ValueError(
                f"Invalid dtype '{dtype}' for column '{col}'. "
                f"Supported: {SUPPORTED_DTYPES}"
            )


def validate_dataframe(df: pd.DataFrame, config: Dict, tracker=None) -> pd.DataFrame:
    # validate the DataFrame against the config rules
    
    strict_mode = config.get("general", {}).get("strict_mode", False)
    columns_config = config.get("columns", {})

    logger.info("Starting validation...")

    missing_cols = []
    for col, rules in columns_config.items():
        required = rules.get("required", False)

        if required and col not in df.columns:
            msg = f"Required column '{col}' is missing in DataFrame"
            missing_cols.append(col)
            if strict_mode:
                logger.error(msg)
            else:
                logger.warning(msg)

            # Track missing column
            if tracker:
                tracker.log(col, "Missing required column", 0)

    if strict_mode and missing_cols:
        raise ValueError(f"Missing required columns in strict mode: {missing_cols}")

    logger.info("Validation completed.")
    return df