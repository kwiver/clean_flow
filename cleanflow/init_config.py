import pandas as pd
import yaml
import os

def load_data(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext in [".xls", ".xlsx"]:
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format")


def generate_config(df):
    print("\nDetected columns:")
    for i, col in enumerate(df.columns):
        print(f"{i+1}. {col}")

    print("\nMap each column and define cleaning rules.\n")

    config = {
        "general": {"strict_mode": False},
        "columns": {},
        "duplicates": {"drop": False},
    }

    for col in df.columns:
        print(f"\nColumn: '{col}'")

        standard_name = input("Standard name (Enter to skip): ").strip()
        if not standard_name:
            continue

        dtype = input("dtype (int, float, str, datetime, bool) [str]: ").strip() or "str"

        column_rules = {
            "aliases": [col],
            "dtype": dtype,
        }

        missing = input("Missing (mean/median/mode/drop/skip) [skip]: ").strip().lower()
        if missing and missing != "skip":
            column_rules["missing"] = missing

        if dtype == "str":
            print("Text cleaning options:")

            strip = input(" - Strip spaces? (y/n): ").lower() == "y"
            capitalize = input(" - Capitalize? (y/n): ").lower() == "y"
            lower = input(" - Convert to lowercase? (y/n): ").lower() == "y"
            upper = input(" - Convert to uppercase? (y/n): ").lower() == "y"

            if strip or capitalize or lower or upper:
                column_rules["text_cleaning"] = {
                    "strip": strip,
                    "capitalize": capitalize,
                    "lower": lower,
                    "upper": upper,
                }

        if dtype in ["int", "float"]:
            print("Numeric cleaning options:")

            strip = input(" - Strip spaces? (y/n): ").lower() == "y"
            remove_special = input(" - Remove special characters ($, commas, %, etc)? (y/n): ").lower() == "y"

            if strip or remove_special:
                column_rules["numeric_cleaning"] = {
                    "strip": strip,
                    "remove_special_chars": remove_special,
                }

            print("Outlier handling options:")
            outlier = input(" - Choose (cap/remove/skip) [skip]: ").strip().lower()

            if outlier == "cap":
                column_rules["outliers"] = {
                    "method": "cap_percentile",
                    "lower": 0.10,
                    "upper": 0.90,
                }

            elif outlier == "remove":
                column_rules["outliers"] = {
                    "method": "remove",
                }

        config["columns"][standard_name] = column_rules

    dup = input("\nDrop duplicate rows? (y/n): ").lower()
    config["duplicates"]["drop"] = dup == "y"

    return config


def save_config(config, path="configs/generated.yaml"):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        yaml.dump(config, f, sort_keys=False)

    print(f"\n✅ Config saved to {path}")