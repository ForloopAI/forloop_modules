from collections.abc import Hashable
from typing import Any

import numpy as np
import pandas as pd

import forloop_modules.flog as flog
from forloop_modules.errors.errors import CriticalPipelineError


def validate_hashable_dict_key(key: Any):
    """
    Ensures the provided key is hashable. Raises a CriticalPipelineError if the key is unhashable.

    Args:
        key (Any): The key to check.

    Raises:
        CriticalPipelineError: If the key is not hashable.
    """
    if not isinstance(key, Hashable):
        raise CriticalPipelineError(f"provided dictionary key is unhashable: {key}")

def validate_input_data_types(df):
    """
    Validate numerical dataframe columns
    If any column has zero decimal number sum it can be retyped to integer columns
    (By default pandas treats integers with Nones as floats
    """
    try:
        df = df.replace({np.nan: pd.NA, "None": pd.NA})

        # this has to be done twice as replacing nans casts columns to objects and integers than fail on .str accessor
        df = df.apply(pd.to_numeric, errors='ignore').convert_dtypes(convert_string=False,
                                                                     convert_boolean=False)

        # remove spaces from cells with numbers and spaces only
        object_columns = df.select_dtypes(include=object).columns
        for object_column in object_columns:
            # without astype(str) condition ignores cells which are of integer type
            if df[object_column].astype(str).str.match(r"^ *[-]?[0-9][0-9 ]*([,.][0-9]+)*$").sum() == (~df[object_column].isna()).sum():
                df[object_column] = df[object_column].replace({" ": ""}, regex=True)

        df[object_columns] = df[object_columns].apply(pd.to_numeric, errors='ignore').convert_dtypes(convert_string=False,
                                                                     convert_boolean=False)

        # may be redundant with the next rows
        columns_containing_date = df.columns[df.columns.str.contains("date")]
        df[columns_containing_date] = df[columns_containing_date].apply(lambda x: pd.to_datetime(x, errors='ignore',
                                                                                                 infer_datetime_format=True))

        # cast empty columns to object
        empty_columns = df.columns[df.isna().mean() == 1.]
        df[empty_columns] = df[empty_columns].astype(object)
    except Exception as e:
        flog.error(message=f"input dtype validation crashed with {e}")

    return df
