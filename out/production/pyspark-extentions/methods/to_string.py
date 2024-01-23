from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Union, List
from tabulate import tabulate

def to_string(
        df: DataFrame,
        by: str = None,
        n: int = 1000,
        precision: int = None,
        percentage_precision: int = None,
        k: bool = False,
        k_label = '',
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

    # determine the column alignment
    colalign = []
    for col in columns:
        if col in df.eColumnsNummeric + df.eColumnsInteger:
            colalign.append('right')
        else:
            colalign.append('left')

    # make the data printable
    df = df.ePrintable(
        precision = precision,
        percentage_precision = percentage_precision,
        k = k,
        k_label = k_label,
    )

    # create the result by looping through the by values and make the
    # per by vqlue
    result = ''
    for by_value in by_values:
        if by:
            title = f'{by}: {by_value}'
            result += f'{title}\n' + '-' * len(title) + '\n'

        df_selection = (
            df
            .filter('true' if not by else F.col(by)==by_value)
            .select(columns)
            .limit(n)
        )

        data = df_selection.eToList('D')
        # add separation lines before the total columns

        if '_group' in df_selection.columns:
            # columns = [col for col in df_selection.columns if col not in ('_group', '_seq')]
            group_column_number = df_selection.columns.index('_group')
            seq_column_number = df_selection.columns.index('_seq')
            _data = data
            records = data.co
            data = []
            for row in _data:
                if row[group_column_number] in ['2', '3']:
                    # "\001" is the tabulate internal code for separator line
                    data.append("\001")
                    # the totals line
                    data.append(row)
                    # an empty line
                    data.append([])
                else:
                    data.append(row)

        # Print the table and add it to the result
        result += tabulate(
            data,
            headers=df_selection.columns,
            colalign=colalign
        ) + '\n'

        result = result + '\n' if by else result

    df.unpersist()
    return result


