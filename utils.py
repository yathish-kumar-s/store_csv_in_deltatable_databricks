import io
import pandas as pd
from flask import flash


def read_csv_file(file):
    df = pd.read_csv(file)

    # df_columns = [col.lower() for col in df.columns]
    # missing_columns = [col for col in required_columns if col not in df_columns]
    # if missing_columns:
    #     flash(f"Missing columns: {', '.join(missing_columns)}")
    #     raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    return csv_buffer