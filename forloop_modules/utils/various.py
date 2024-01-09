import inspect
from typing import Any

import numpy as np
import pandas as pd

from forloop_modules.utils.definitions import JSON_SERIALIZABLE_TYPES, REDIS_STORED_TYPES


def is_value_serializable(value) -> bool:
    is_value_serializable = type(value) in JSON_SERIALIZABLE_TYPES
    return is_value_serializable


def is_value_redis_compatible(value) -> bool:
    is_value_redis_compatible = type(value) in REDIS_STORED_TYPES
    is_value_callable = inspect.isfunction(value)
    is_value_class = inspect.isclass(value)
    return is_value_redis_compatible or is_value_callable or is_value_class


def is_list_of_strings(var) -> bool:
    """Check for list[str] variable type."""
    return isinstance(var, list) and all(isinstance(v, str) for v in var)


def serialize_if_dataframe_to_api(variable_series: pd.Series) -> Any:
    """
    Cast a DF into a dict format used by the API if the input Variable/Result is of type 'DataFrame'
    This functions is to be used only with pd.DataFrame.apply() method, hence the input is a
    pd.Series.

    :param variable_series: pd.Series holding a Variable
    :type variable_series: pd.Series
    :return: modified pd.Series holding a Variable
    :rtype: Any
    """
    if variable_series['type'] == 'DataFrame':
        variable_series["value"] = serialize_dataframe_to_api(variable_series["value"])
    return variable_series


def serialize_dataframe_to_api(variable_value_df: pd.DataFrame) -> dict:
    """
    Serialize a DF into the dict format used by API.

    :param variable_value_df: a Variable's value attribute as a DataFrame
    :type variable_value_df: pd.DataFrame
    :return: Variable's value serialized as a dict
    :rtype: dict
    """
    df = variable_value_df.copy()
    df = df.replace(np.nan, None)
    return {"columns": list(df.columns), "values": df.values.tolist()}


def parse_if_dataframe_from_db(variable_series: pd.Series) -> Any:
    """
    Parse a DF from a dict format used in the DB if the input Variable/Result is of type 'DataFrame'
    This functions is to be used only with pd.DataFrame.apply() method, hence the input is a
    pd.Series.

    :param variable_series: pd.Series holding a Variable
    :type variable_series: pd.Series
    :return: modified pd.Series holding a Variable
    :rtype: Any
    """
    if variable_series['type'] == 'DataFrame':
        df_dict = variable_series["value"]
        df = pd.DataFrame(df_dict["data"], index=df_dict["index"], columns=df_dict["columns"])
        df.attrs = df_dict["attrs"]
        variable_series["value"] = df
    return variable_series


def serialize_if_dataframe_to_db(variable_series: pd.Series) -> Any:
    """
    Cast a DF into a dict format used in the DB if the input Variable/Result is of type 'DataFrame'.
    This functions is to be used only with pd.DataFrame.apply() method, hence the input is a
    pd.Series.

    :param variable_series: pd.Series holding a Variable
    :type variable_series: pd.Series
    :return: modified pd.Series holding a Variable
    :rtype: Any
    """
    if variable_series['type'] == 'DataFrame':
        df = variable_series["value"]
        df_dict = df.to_dict(orient="split")
        df_dict["attrs"] = df.attrs
        variable_series["value"] = df_dict
    return variable_series
