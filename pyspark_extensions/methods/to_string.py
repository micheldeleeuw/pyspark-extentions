from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Union, List
from tabulate import tabulate
from textwrap import wrap

def to_string(
        df: DataFrame,
        by: str = None,
        n: int = 1000,
        precision: int = None,
        percentage_precision: int = None,
        k: bool = False,
        k_label = '',
        title: str = None,
        truncate: Union[bool, int] = False,
        show_index: bool = False,
        format: str = None, # see https://pypi.org/project/tabulate/ for possible values
        columns_widths: List[Union[None, int]] = None,
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
    for col in [col for col in columns if col != '_group']:
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
    # per by vqlue.
    result = title + '\n' if title else ''

    for by_value in by_values:
        if by:
            title = f'{by}: {by_value}'
            result += f'{title}\n'
            if not format or format.find('outline') == -1:
                result += '-' * len(title) + '\n'

        df_selection = (
            df
            .filter('true' if not by else F.col(by)==by_value)
            .select(columns)
            .limit(n)
        )

        # Get the data in a 2-dimensional array
        data = [
            list(row.asDict().values())
            for row in df_selection.collect()
        ]

        # add separation lines before the total columns
        if '_group' in df_selection.columns:
            group_column_number = df_selection.columns.index('_group')
            _data = data
            records = len(data)
            data = []
            for i, row in enumerate(_data):
                if row[group_column_number] in ['2', '3']:
                    if format in ['simple', 'rst'] or format == None:
                        # "\001" is the tabulate internal code for separator line
                        data.append("\001")
                    # the totals line
                    del row[group_column_number]
                    data.append(row)
                    # an empty line, except for the last total
                    if i < records-1:
                        data.append([])
                else:
                    del row[group_column_number]
                    data.append(row)

        # Apply column widths. Default tabulate functionality has a bug
        if columns_widths:
            for row, row_data in enumerate(data):
                for col, width in enumerate(columns_widths):
                    if width and isinstance(row_data, List) and isinstance(row_data[col], str):
                        data[row][col] = '\n'.join(wrap(data[row][col], width))

        # Print the table and add it to the result
        if data:
            result += tabulate(
                data,
                headers=[col for col in df_selection.columns if col != '_group'],
                colalign=colalign,
                showindex=show_index,
                tablefmt=format,
            ) + '\n'
            result = result + '\n' if by else result
        else:
            result += 'No data'

    df.unpersist()
    return result.rstrip()


