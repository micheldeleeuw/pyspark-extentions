from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Union, List
from .validate_columns import validate_columns

def to_string(
        df: DataFrame,
        by: str = None,
        padding_left: str = '',
        padding_right: str = '',
        n: int = 1000,
        truncate: Union[bool, int] = False,
    ):

    if isinstance(truncate, bool) and truncate:
        # set truncate to 20 when truncate is a boolean
        truncate = 20

    if by:
        # we cache because we will get query it multiple times
        df.cache().count()
        by_values = [row[0] for row in df.select(by).distinct().orderBy(by).collect()]
        columns = [col for col in df.columns if col != by]
    else:
        by_values = [None]
        columns = df.columns

    result = ''

    for by_value in by_values:
        if by:
            result += '\n' + (f'{by}: {by_value}')

        result += (
            df
            .filter('true' if not by else F.col(by)==by_value)
            .drop(by if by else '')
            # .muPrintable(precision=precision, percentage_precision=percentage_precision)

            # add spaces to the data
            # .selectExpr([f'concat("{padding_character}", cast(`{col}` as string), "{padding_character}") as `{col}`' for col in cols])

            # ._jdf.showString(n, int(truncate))
            .show()
        )

    df.unpersist()
    return result


