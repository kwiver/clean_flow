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


def validate_dataframe(df: pd.DataFrame, config: Dict):
    # validate the DataFrame against the config rules
    columns_config = config.get("columns", {})

    for col, rules in columns_config.items():
        if rules.get("required", False) and col not in df.columns:
            raise ValueError(f"Missing required column: '{col}'")

        if col not in df.columns:
            logger.warning(f"Column '{col}' defined in config but not found in data")