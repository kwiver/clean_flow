# import necessary libs
import pandas as pd


def handle_outliers(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    for col, rules in config.get("columns", {}).items():
        if col not in df.columns:
            continue

        strategy = rules.get("outliers")

        if not strategy:
            continue

        if not pd.api.types.is_numeric_dtype(df[col]):
            continue

        before = df[col].copy()
        
        # 75th percentile capping (IQR-based)
        if strategy == "cap":
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1

            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR

            df[col] = df[col].clip(lower, upper)
            
        # 90th percentile capping
        elif strategy == "cap_percentile":
            lower = df[col].quantile(0.10)
            upper = df[col].quantile(0.90)

            df[col] = df[col].clip(lower, upper)

        # Remove Outliers (IQR-based)
        elif strategy == "remove":
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1

            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR

            df = df[(df[col] >= lower) & (df[col] <= upper)]

        else:
            continue

        changes = (before != df[col]).sum()

        tracker.log(
            f"{col}: outliers handled using '{strategy}' ({changes} values affected)"
        )

    return df