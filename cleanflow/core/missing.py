# import necessary libs
import pandas as pd
import numpy as np

def standardize_missing(df: pd.DataFrame, missing_values=None, tracker=None):
    if missing_values is None:
        missing_values = ["?", "unknown", "Unknown", "NA", "N/A", "nan", "NaN", ""]
    
    original_counts = df.isna().sum().to_dict()
    df.replace(to_replace=missing_values, value=np.nan, inplace=True)
    
    if tracker:
        for col, orig_count in original_counts.items():
            new_count = df[col].isna().sum()
            changed = new_count - orig_count
            if changed > 0:
                tracker.log(col, "Standardized missing values", changed)
    
    return df


def handle_missing(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    for col, rules in config.get("columns", {}).items():
        if col not in df.columns:
            continue

        method = rules.get("missing")
        if not method:
            continue

        original_nulls = df[col].isna().sum()

        if method == "mean":
            df[col] = df[col].fillna(df[col].mean())

        elif method == "median":
            df[col] = df[col].fillna(df[col].median())

        elif method == "mode":
            df[col] = df[col].fillna(df[col].mode()[0])

        elif method == "drop":
            before = len(df)
            df = df[df[col].notna()]
            removed = before - len(df)

            if tracker and removed > 0:
                tracker.log(col, "Rows dropped (missing)", int(removed))
            continue

        new_nulls = df[col].isna().sum()
        filled = original_nulls - new_nulls

        if tracker and filled > 0:
            tracker.log(col, "Missing filled", int(filled))

    return df