# import necessary libs
import pandas as pd


def remove_duplicates(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    if not config.get("general", {}).get("drop_duplicates", False):
        return df

    before = len(df)

    df = df.drop_duplicates()

    removed = before - len(df)

    tracker.log_global("duplicates removed", removed)

    return df