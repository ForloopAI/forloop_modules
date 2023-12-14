import inspect
from typing import Any

import pandas as pd

from forloop_modules.utils.definitions import JSON_SERIALIZABLE_TYPES, REDIS_STORED_TYPES


def is_value_serializable(value) -> bool:
    return type(value) in JSON_SERIALIZABLE_TYPES


def is_value_redis_compatible(value) -> bool:
    return (
        type(value) in REDIS_STORED_TYPES or inspect.isfunction(value) or inspect.isclass(value)
    )


def serialize_dataframe_to_api(value_df: pd.DataFrame) -> dict:
    """Serialize a DF into the dict format used by API."""
    return {"columns": list(value_df.columns), "values": value_df.values.tolist()}


def parse_if_dataframe_from_db(row: pd.Series) -> Any:
    """Parse a DF from a dict format used in the DB."""
    if row['type'] == 'DataFrame':
        df_dict = row["value"]
        df = pd.DataFrame(df_dict["data"], index=df_dict["index"], columns=df_dict["columns"])
        df.attrs = df_dict["attrs"]
        row["value"] = df
    return row


def serialize_if_dataframe_to_db(row: pd.Series) -> JSON_SERIALIZABLE_TYPES:
    """Cast a DF to a dict format compatible with the DB."""
    if row['type'] == 'DataFrame':
        df = row["value"]
        df_dict = df.to_dict(orient="split")
        df_dict["attrs"] = df.attrs
        row["value"] = df_dict
    return row
