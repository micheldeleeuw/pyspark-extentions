import logging
from pyspark.sql import DataFrame
from typing import List

log = logging.getLogger(__name__)

def normalize_column_names(
        df: DataFrame,
        columns: List[str] = None
    ):

    columns = df.columns if not columns else columns
    columns = [columns] if isinstance(columns, str) else columns

    new_columns = []
    for col in df.columns:
        if col in columns:
            col = col.lower().translate({
                32:95, 33:95, 34:95, 35:95, 36:95, 37:95, 38:95, 39:95, 40:95, 41:95, 42:95, 43:95, 44:95, 45:95, 46:95, 47:95
            })
        col = col[0:-1] if col.endswith('_') else col

        while col in new_columns:
            col = col + '_1'
        new_columns.append(col)

    return df.toDF(*new_columns)

