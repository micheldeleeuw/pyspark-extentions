from pyspark.sql import DataFrame
from typing import List, Union
import pyspark.sql.functions as F


def round(df: DataFrame, precision: int = 2, columns:Union[str, List[str]] = None):

    columns = [dtype[0] for dtype in df.dtypes if dtype[1] == 'double'] if not columns else columns
    columns = df.eValidateColumns(columns)

    for col in columns:
        df = df.withColumn(col, F.round(F.col(col), precision))

    return df

