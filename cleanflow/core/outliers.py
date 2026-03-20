# import necessary libs
import pandas as pd


def handle_outliers(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    for col, rules in config.get("columns", {}).items():
        if col not in df.columns:
            continue

        outlier_rules = rules.get("outliers")
        if not outlier_rules:
            continue

        method = outlier_rules.get("method")

        if method == "cap_percentile":
            lower = df[col].quantile(outlier_rules.get("lower", 0.10))
            upper = df[col].quantile(outlier_rules.get("upper", 0.90))

            original = df[col].copy()
            df[col] = df[col].clip(lower, upper)

            changed = (original != df[col]).sum()

            if tracker and changed > 0:
                tracker.log(col, "Outliers capped", int(changed))

        elif method == "remove":
            before = len(df)
            lower = df[col].quantile(0.10)
            upper = df[col].quantile(0.90)

            df = df[(df[col] >= lower) & (df[col] <= upper)]
            removed = before - len(df)

            if tracker and removed > 0:
                tracker.log(col, "Outliers removed", int(removed))

    return df