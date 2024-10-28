import io
import pandas as pd
from flask import flash


def validate_missing_columns(df, required_columns):
    """Ensure all required columns are present in the DataFrame."""
    df_columns = [col.lower() for col in df.columns]
    missing_columns = [col for col in required_columns if col.lower() not in df_columns]

    if missing_columns:
        flash(f"Missing columns: {', '.join(missing_columns)}")
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")


def validate_null_columns(df, nullable_columns):
    """Ensure that only the specified columns are allowed to be null."""
    nullable_columns = [col.lower() for col in nullable_columns]
    not_null_columns = [col for col in df.columns if col.lower() not in nullable_columns]
    null_columns = [col for col in not_null_columns if df[col].isnull().any()]

    if null_columns:
        flash(f"These columns cannot be null: {', '.join(null_columns)}")
        raise ValueError(f"Empty columns detected: {', '.join(null_columns)}")

def read_csv_file(file, required_columns=None, nullable_columns=None):
    df = pd.read_csv(file)

    validate_missing_columns(df, required_columns or [])
    validate_null_columns(df, nullable_columns or [])

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    return csv_buffer