import pandas as pd

def map_columns(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    column_config = config.get("columns", {})

    rename_dict = {}

    for standard_name, rules in column_config.items():
        aliases = rules.get("aliases", [])

        # check if standard name already exists
        if standard_name in df.columns:
            continue

        # look for alias in dataset
        for alias in aliases:
            if alias in df.columns:
                rename_dict[alias] = standard_name
                tracker.log(f"Column '{alias}' mapped to '{standard_name}'")
                break

    # apply renaming
    df = df.rename(columns=rename_dict)

    return df