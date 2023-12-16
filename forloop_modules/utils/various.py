import inspect
from typing import Any

import pandas as pd

from forloop_modules.utils.definitions import JSON_SERIALIZABLE_TYPES, REDIS_STORED_TYPES


def is_value_serializable(value) -> bool:
    is_value_serializable = type(value) in JSON_SERIALIZABLE_TYPES
    return is_value_serializable


def is_value_redis_compatible(value) -> bool:
    is_value_callable = inspect.isfunction(value)
    is_value_class = inspect.isclass(value)
    is_value_redis_compatible = type(value) in REDIS_STORED_TYPES
    return is_value_redis_compatible or is_value_callable or is_value_class


def is_list_of_strings(var) -> bool:
    """Check for list[str] variable type."""
    return isinstance(var, list) and not all(isinstance(v, str) for v in var)


def is_value_redis_compatible(value) -> bool:
    return (
        type(value) in REDIS_STORED_TYPES or inspect.isfunction(value) or inspect.isclass(value)
    )


def serialize_if_dataframe_to_api(variable_df: pd.Series) -> Any:
    """Cast a DF into a dict format used by the API if the input Variable/Result is of type 'DataFrame'."""
    if variable_df['type'] == 'DataFrame':
        variable_df["value"] = serialize_dataframe_to_api(variable_df["value"])
    return variable_df


def serialize_dataframe_to_api(variable_value_df: pd.DataFrame) -> dict:
    """Serialize a DF into the dict format used by API."""
    return {"columns": list(variable_value_df.columns), "values": variable_value_df.values.tolist()}


def parse_if_dataframe_from_db(variable_df: pd.Series) -> Any:
    """Parse a DF from a dict format used in the DB if the input Variable/Result is of type 'DataFrame'."""
    if variable_df['type'] == 'DataFrame':
        df_dict = variable_df["value"]
        df = pd.DataFrame(df_dict["data"], index=df_dict["index"], columns=df_dict["columns"])
        df.attrs = df_dict["attrs"]
        variable_df["value"] = df
    return variable_df


def serialize_if_dataframe_to_db(variable_df: pd.Series) -> Any:
    """Cast a DF into a dict format used in the DB if the input Variable/Result is of type 'DataFrame'."""
    if variable_df['type'] == 'DataFrame':
        df = variable_df["value"]
        df_dict = df.to_dict(orient="split")
        df_dict["attrs"] = df.attrs
        variable_df["value"] = df_dict
    return variable_df
