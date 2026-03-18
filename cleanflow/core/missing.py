# import necessary libs
import pandas as pd


def handle_missing(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    for col, rules in config.get("columns", {}).items():
        if col not in df.columns:
            continue

        if "missing" not in rules:
            continue

        before = df[col].isna().sum()
        strategy = rules["missing"]

        if strategy == "mean":
            df[col] = df[col].fillna(df[col].mean())

        elif strategy == "median":
            df[col] = df[col].fillna(df[col].median())

        elif strategy == "mode":
            df[col] = df[col].fillna(df[col].mode().iloc[0])

        elif strategy == "drop":
            df = df.dropna(subset=[col])

        elif strategy == "zero":
            df[col] = df[col].fillna(0)

        else:
            continue

        after = df[col].isna().sum()

        tracker.log(
            f"{col}: missing handled using '{strategy}' ({before - after} values affected)"
        )

    return df