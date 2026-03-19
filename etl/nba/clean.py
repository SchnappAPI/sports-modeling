# etl/clean.py
import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.where(pd.notna(df), None)

    for col in df.select_dtypes(include=["int64", "float64"]).columns:
        df[col] = df[col].apply(
            lambda x: None if x is None else
            int(x) if float(x).is_integer() else float(x)
        )

    return df
