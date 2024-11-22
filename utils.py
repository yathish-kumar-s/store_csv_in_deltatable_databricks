import io
import os
import pandas as pd
from flask import flash
from databricks import sql

from database_connector import db_connector


def validate_missing_columns(df, required_columns):
    """Ensure all required columns are present in the DataFrame."""
    df_columns = [col.lower() for col in df.columns]
    missing_columns = [col for col in required_columns if col.lower() not in df_columns]

    if missing_columns:
        flash(f"Missing columns: {', '.join(missing_columns)}",  category='error')
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")


def validate_null_columns(df, nullable_columns):
    """Ensure that only the specified columns are allowed to be null."""

    if df.empty:
        flash(f"The file doesn't contain data please check again", category='error')
        raise ValueError(f"Dataframe is empty")
    nullable_columns = [col.lower() for col in nullable_columns]
    not_null_columns = [col for col in df.columns if col.lower() not in nullable_columns]
    null_columns = [
        col for col in not_null_columns
        if df[col].isnull().any() or (df[col].astype(str).str.strip() == '').any()
    ]

    if null_columns:
        flash(f"These columns cannot be null: {', '.join(null_columns)}", category='error')
        raise ValueError(f"Empty columns detected: {', '.join(null_columns)}")


def validate_single_selection(row):
    """
        Validates that exactly one of the 'good', 'better', 'best', or 'ultra_premium' columns
        in the given row is marked with an 'x', while the others must either be NaN or empty.
        Returns:
        bool: True if exactly one column is marked with 'x' and the others are NaN or empty,
              False otherwise.
        """
    values = row[['good', 'better', 'best', 'ultra_premium']]
    if not all([value == 'x' or pd.isna(value) or ' ' for value in values]):
        return False

    count_x = (values == 'x').sum()
    return count_x == 1


def validate_single_selection_good_better_best_ultra_premium_columns(df):
    """
    Validates the 'good', 'better', 'best', and 'ultra_premium' columns in the provided DataFrame.
    It ensures that each row has exactly one of the columns marked with 'x',
    and the others are either NaN or empty.
    """
    df[['good', 'better', 'best', 'ultra_premium']] = df[['good', 'better', 'best', 'ultra_premium']].apply(
        lambda col: col.astype(str).str.strip().str.lower())
    df['is_valid'] = df.apply(validate_single_selection, axis=1)
    invalid_rows = df[df['is_valid'] == False]

    if not invalid_rows.empty:
        invalid_parts = [str(part) for part in invalid_rows['part'].tolist()]
        flash(f"Invalid data in rows with part no: {', '.join(invalid_parts)}", category='error')
        raise ValueError(f"Invalid data in rows with part no: {', '.join(invalid_parts)}")


def validate_single_selection_custom_attributes(row, value_columns):
    values = row[value_columns]
    if not all([value == 'x' or pd.isna(value) or ' ' for value in values]):
        return False

    count_x = (values == 'x').sum()
    return count_x == 1


def validate_single_selection_custom_part_attribute_columns(df):
    """
    Validates the 'good', 'better', 'best', and 'ultra_premium' columns in the provided DataFrame.
    It ensures that each row has exactly one of the columns marked with 'x',
    and the others are either NaN or empty.
    """
    cols = ['partid', 'lc', 'part']
    value_columns = [c for c in df.columns if c not in cols]
    df[value_columns] = df[value_columns].apply(
        lambda col: col.astype(str).str.strip().str.lower())
    df['is_valid'] = df.apply(validate_single_selection_custom_attributes,  axis=1, value_columns=value_columns)
    invalid_rows = df[df['is_valid'] == False]

    if not invalid_rows.empty:
        invalid_parts = [str(part) for part in invalid_rows['part'].tolist()]
        flash(f"Invalid data in rows with part no: {', '.join(invalid_parts)}", category='error')
        raise ValueError(f"Invalid data in rows with part no: {', '.join(invalid_parts)}")


def validate_good_better_best(df, required_columns, nullable_columns):
    """
    Validation for good better and best dataframe
    """
    validate_missing_columns(df, required_columns)
    validate_null_columns(df, nullable_columns)
    validate_single_selection_good_better_best_ultra_premium_columns(df)
    df = df.drop('is_valid', axis=1)
    return df


def validate_part_terms(df, required_columns, nullable_columns):
    """
    Validation for part terms dataframe
    """
    validate_missing_columns(df, required_columns)
    validate_null_columns(df, nullable_columns)


def validate_custom_part_attributes(df, required_columns, nullable_columns):
    """
    Validation for custom part attributes dataframe
    """
    if not nullable_columns:
        nullable_columns = [col for col in df.columns if col not in required_columns]
    validate_missing_columns(df, required_columns)
    validate_null_columns(df, nullable_columns)
    validate_single_selection_custom_part_attribute_columns(df)
    df = df.drop('is_valid', axis=1)
    return df


def validate_load_sku_list(df, required_columns, only_part_number):
    df_columns = [col.lower() for col in df.columns]

    missing_columns = [col for col in required_columns if col.lower() not in df_columns]

    if missing_columns:
        if only_part_number and 'partnumber' in missing_columns:
            flash(f"Missing columns: {', '.join(missing_columns)}", category='error')
            raise ValueError(f"Missing columns: {', '.join(missing_columns)}")
        if 'sku' in df_columns:
            return
        flash(f"Missing columns: {', '.join(missing_columns)}", category='error')
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")
    # validate_null_columns(df, nullable_columns)


def get_csv_buffer(df):
    """
     Converts a DataFrame into a CSV buffer.
    """
    csv_buffer = io.BytesIO()
    df.columns = df.columns.str.lower()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    return csv_buffer



def read_csv_file(file, required_columns=None, nullable_columns=None):
    df = pd.read_excel(file)
    df.columns = df.columns.str.lower()
    validate_part_terms(df, required_columns, nullable_columns)
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    return csv_buffer


def create_templates_df_csv_buffer(columns):
    """
     Creates a DataFrame with the specified columns and returns it as a CSV buffer.
    """
    df = pd.DataFrame(columns=columns)
    csv_stream = io.BytesIO()
    df.to_excel(csv_stream, index=False)
    csv_stream.seek(0)
    return csv_stream

def create_templates_df_cpa(columns, part_id):
    """
     Creates a DataFrame with the specified columns and returns it as a CSV buffer.
    """
    df = pd.DataFrame(columns=columns)
    df.loc[len(df)] = [part_id] + [None]*(len(columns)-1)
    csv_stream = io.BytesIO()
    df.to_excel(csv_stream, index=False)
    csv_stream.seek(0)
    return csv_stream



def create_templates_df_cpa_prefilled_sku(default_columns,columns, part_id, cpa_type, cpa_category):
    """
     Creates a DataFrame with the specified columns and returns it as a CSV buffer.
    """

    with db_connector() as connection:
        with connection.cursor() as cursor:
            attribute_cols = [column for column in columns if column not in default_columns]
            partterm_id = part_id

            select_attr_cols = ''
            for item in attribute_cols:
                select_attr_cols = select_attr_cols + ", '' as " + f"`{item}`"

            query = f"""
                  select a.partterm PartId,s.linecode LC,s.partnumber Part, '{cpa_type}' as `PartAttributeType`
                  , '{cpa_category}' as `PartAttributeCategory` {select_attr_cols}
                  from catalogdata.silver.ptfinal a
                  join dst.gold.skumaster s on a.sku = s.sku where partterm = {partterm_id}
                """

            rows = cursor.execute(query).fetchall()

    data_dict = [row.asDict() for row in rows]
    df = pd.DataFrame(data_dict)
    csv_stream = io.BytesIO()
    df.to_excel(csv_stream, index=False)
    csv_stream.seek(0)
    return csv_stream

