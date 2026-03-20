import pandas as pd
def apply_value_mapping(df: pd.DataFrame, config: dict, tracker) -> pd.DataFrame:
    for col, rules in config.get("columns", {}).items():
        if col not in df.columns:
            continue

        mapping = rules.get("value_mapping")
        if not mapping:
            continue

        original = df[col].copy()

        # reverse map
        reverse_map = {}
        for standard, values in mapping.items():
            for v in values:
                reverse_map[str(v).lower()] = standard

        df[col] = df[col].astype(str).apply(
            lambda x: reverse_map.get(x.lower(), x)
        )

        # tracking
        if tracker:
            changed = (original != df[col]).sum()
            if changed > 0:
                tracker.log(col, "Value mapped", int(changed))

    return df