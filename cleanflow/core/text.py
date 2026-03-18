# import necessary libs
import pandas as pd


def clean_text(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    for col, rules in config.get("columns", {}).items():
        if col not in df.columns:
            continue

        text_rules = rules.get("text_cleaning", {})

        if not text_rules:
            continue

        if df[col].dtype != "object":
            continue

        original = df[col].copy()

        if text_rules.get("lowercase"):
            df[col] = df[col].str.lower()

        if text_rules.get("uppercase"):
            df[col] = df[col].str.upper()

        if text_rules.get("strip"):
            df[col] = df[col].str.strip()

        if text_rules.get("remove_spaces"):
            df[col] = df[col].str.replace(" ", "", regex=False)
            
        if text_rules.get("capitalize"):
            df[col] = df[col].str.capitalize()

        if text_rules.get("title"):
            df[col] = df[col].str.title()


        changes = (original != df[col]).sum()

        tracker.log(f"{col}: text cleaned ({changes} values modified)")

    return df