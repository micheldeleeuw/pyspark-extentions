from pyspark.sql import DataFrame
from typing import List, Union


def round(df: DataFrame, precision: int = 2, columns:Union[str, List[str]] = None):
    '''round some or all columns in a dataframe'''

    import pyspark.sql.functions as F

    columns = [columns] if isinstance(columns, str) else columns
    columns = [dtype[0] for dtype in df.dtypes if dtype[1] == 'double'] if not columns else columns

    for col in columns:
        df = df.withColumn(col, F.round(F.col(col), precision))

    return df

