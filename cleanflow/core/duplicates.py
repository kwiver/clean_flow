# import necessary libs
import pandas as pd


def remove_duplicates(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    if not config.get("general", {}).get("drop_duplicates", False):
        return df

    before = len(df)

    df = df.drop_duplicates()

    after = len(df)

    tracker.log(f"duplicates: removed {before - after} rows")

    return df