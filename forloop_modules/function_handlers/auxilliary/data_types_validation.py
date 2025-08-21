import pandas as pd
import numpy as np
import forloop_modules.flog as flog



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
        ## VVVV Messed up date format in dataframe explorer on desktop app VVVV
        df[columns_containing_date] = df[columns_containing_date].apply(lambda x: pd.to_datetime(x, errors='ignore', infer_datetime_format=True))

        # After conversion attempt, format each date column individually to ISO string
        for col in columns_containing_date:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d')
        ## ^^^^ Messed up date format in dataframe explorer on desktop app ^^^^

        # cast empty columns to object
        empty_columns = df.columns[df.isna().mean() == 1.]
        df[empty_columns] = df[empty_columns].astype(object)
    except Exception as e:
        flog.error(message=f"input dtype validation crashed with {e}")

    return df
