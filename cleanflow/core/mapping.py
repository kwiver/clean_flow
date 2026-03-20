import pandas as pd

def map_columns(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    column_map = {}

    for standard_col, rules in config.get("columns", {}).items():
        aliases = rules.get("aliases", [])

        for alias in aliases:
            if alias in df.columns:
                column_map[alias] = standard_col

    df = df.rename(columns=column_map)

    
    if tracker:
        for old, new in column_map.items():
            tracker.log(new, "Column mapped", 1)

    return df