import pandas as pd

def clean_text(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    for col, rules in config.get("columns", {}).items():
        if col not in df.columns:
            continue

        text_rules = rules.get("text_cleaning", {})
        if not text_rules:
            continue

        if not pd.api.types.is_string_dtype(df[col]):
            continue

        original = df[col].copy()

        # basic cleaning
        if text_rules.get("strip"):
            df[col] = df[col].str.strip()

        if text_rules.get("remove_spaces"):
            df[col] = df[col].str.replace(" ", "", regex=False)

        # case handling
        if text_rules.get("lower"):
            df[col] = df[col].str.lower()

        elif text_rules.get("upper"):
            df[col] = df[col].str.upper()

        elif text_rules.get("title"):
            df[col] = df[col].str.title()

        # tracking
        if tracker:
            changed = (original != df[col]).sum()
            if changed > 0:
                tracker.log(col, "Text cleaned", int(changed))

    return df