import logging
from pyspark.sql import DataFrame
from typing import List, Union

log = logging.getLogger(__name__)

def validate_columns(df: DataFrame, columns: Union[List[str], str] = None) -> List[str]:
    if isinstance(columns, str):
        columns = [columns]
    elif columns == [] or columns:
        pass
    else:
        columns = df.columns

    for col in columns:
        if col not in df.columns:
            raise ValueError(f'`{col}` not a column of the dataframe.')

    return columns