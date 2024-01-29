from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Union, List
from textwrap import wrap

class ToString():

    @staticmethod
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
            table_format: str = 'default',
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
        col_align = []
        for col in [col for col in columns if col != '_group']:
            if col in df.eColumnsNummeric + df.eColumnsInteger:
                col_align.append('right')
            else:
                col_align.append('left')

        # make the data printable
        df = df.ePrintable(
            precision = precision,
            percentage_precision = percentage_precision,
            k = k,
            k_label = k_label,
        )

        # if _group is one of the columns, make sure it is the last one
        if '_group' in df.columns:
            df = df.select(
                *[col for col in df.columns if col != '_group'], '_group')
            group_column = True
        else:
            group_column = False

        # create the result by looping through the by values and make the table
        # per by value.
        result = title + '\n' if title else ''

        for by_value in by_values:
            result += ToString._to_string_for_single_by_value(
                df=df, by=by, by_value=by_value, n=n, columns=columns,
                columns_widths=columns_widths, group_column=group_column,
                col_align=col_align, show_index=show_index, table_format=table_format
            )

        df.unpersist()
        return result.rstrip()

    @staticmethod
    def _to_string_for_single_by_value(
            df, by, by_value, n, columns,
            columns_widths, group_column,
            col_align, show_index, table_format
        ):
        result = ''
        if by:
            title = f'{by}: {by_value}'
            result += f'{title}\n'
            if not table_format or table_format.find('outline') == -1:
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

        # Apply column widths to data and header
        headers = df_selection.columns
        if columns_widths:
            # headers
            for col, width in enumerate(columns_widths):
                if width:
                    headers[col] = '\n'.join(wrap(headers[col], width))
            # data
            for row, row_data in enumerate(data):
                for col, width in enumerate(columns_widths):
                    if width and isinstance(row_data[col], str):
                        data[row][col] = '\n'.join(wrap(data[row][col], width))

        # Print the table and add it to the result
        if data:
            result += ToString._array_to_string(
                data=data,
                headers=headers,
                group_column=group_column,
                col_align=col_align,
                show_index=show_index,
                table_format=table_format,
            )
            result += '\n'
            result = result + '\n' if by else result
        else:
            result += 'No data'

        return result

    @staticmethod
    def _array_to_string(
            data: List[List],
            headers: List[str],
            group_column: bool,
            col_align: List[str],
            show_index: bool,
            table_format: str,
        ):

        # Step 1, do some preprocessing
        # Make each element a string
        data = [[' ' if not col else str(col) for col in row] for row in data]
        headers_count = len(headers) -1 if group_column else len(headers)

        # transform the headers and data to enable multi line
        headers_raw = headers
        headers = [list(reversed(header.split('\n'))) for header in headers]
        header_rows = max([len(header) for header in headers])
        headers = list(reversed([
            [
                header[row] if row < len(header) else ''
                for header in headers
            ] for row in range(header_rows)
        ]))

        _data = data
        data = []
        for row_data in _data:
            row_data = [value.split('\n') for value in row_data]
            row_data_rows = max([len(data_row) for data_row in row_data])
            row_data = [[value[row] if row < len(value) else '' for value in row_data] for row in range(row_data_rows)]
            data.extend(row_data)

        # Step 2 determine columns widths, excl. separator
        widths = [1] * len(headers_raw)
        for row, row_data in enumerate(data + headers):
            for col, value in enumerate(row_data):
                widths[col] = max(widths[col], len(value))
        widths = widths[0:headers_count]
        headers = headers[0:headers_count]

        # step 3, determine which rows are totals rows and which are data rows
        if group_column:
            data_group = [row_data[-1] for row_data in data]
            total_rows = [i for i in range(len(data_group)) if data_group[i] in ('2', '3')]
            data_rows = [i for i in range(len(data_group)) if data_group[i] not in ('2', '3')]
        else:
            total_rows = []
            data_rows = range(len(data))

            # step 4, create characters we will use from the table format
        table_settings = ToString._table_settings(table_format=table_format)

        # step 5, create some elements we will use
        start_header = ToString._make_lines('start_header', table_settings, widths)
        header_lines = ToString._make_lines('header_line', table_settings, widths, headers, col_align)
        header_data = ToString._make_lines('header_data', table_settings, widths)
        data_lines = ToString._make_lines('data_line', table_settings, widths, data, col_align)
        data_totals = ToString._make_lines('data_totals', table_settings, widths)
        totals_lines = ToString._make_lines('totals_line', table_settings, widths, data, col_align)
        totals_data = ToString._make_lines('totals_data', table_settings, widths)
        totals_totals = ToString._make_lines('totals_totals', table_settings, widths)
        data_end = ToString._make_lines('data_end', table_settings, widths)
        totals_end = ToString._make_lines('totals_end', table_settings, widths)

        # Step 6, bring it together
        result = [
            [start_header],
            *[header_lines],
            [header_data],
            *[
                # To understand what is happening here: a row is either a data row or a totals row. Depending
                # on that fact layout lines are added to the result.
                ([data_lines[row]] if row in data_rows else []) +
                ([totals_lines[row]] if row in total_rows else []) +

                ([data_totals] if row in data_rows and row+1 in total_rows else []) +
                ([totals_data] if row in total_rows and row+1 in data_rows else []) +
                ([totals_totals] if row in total_rows and row+1 in total_rows else []) +
                ([data_end] if row in data_rows and row == len(data)-1 else []) +
                ([totals_end] if row in total_rows and row == len(data)-1 else [])
                for row in range(len(data))
            ],
        ]

        result = [line for line_array in result for line in line_array if line != '']
        return '\n'.join(result)


    @staticmethod
    def _make_lines(line_type, table_settings, widths, data=None, col_align=None):
        open_str = table_settings[f'{line_type}__open']
        separate_str = table_settings[f'{line_type}__separate']
        close_str = table_settings[f'{line_type}__close']
        fill_str = table_settings[f'{line_type}__fill'] if f'{line_type}__fill' in table_settings.keys() else ''
        postfix_str = table_settings[f'{line_type}__postfix'] if f'{line_type}__postfix' in table_settings.keys() else ''

        if not data:
            return (
                    open_str +
                    separate_str.join([
                        fill_str * width for width in widths
                ]) +
                close_str +
                postfix_str
            )
        else:
            return [
                open_str +
                separate_str.join([
                    value.ljust(widths[col])
                    if col_align[col] == 'left'
                    else value.rjust(widths[col])
                    for col, value in enumerate(data_line[0:len(widths)])
                ]) + close_str + postfix_str
                for data_line in data
            ]


    @staticmethod
    def _table_settings(table_format):
        # set all settings to '' so each table format only needs to define the exceptions
        table_settings = dict(
            start_header__open = '', start_header__separate = '', start_header__close = '', start_header__fill = '',
            header_line__open = '', header_line__separate = '', header_line__close = '',
            header_data__open = '', header_data__separate = '', header_data__close = '', header_data__fill = '',
            data_line__open = '', data_line__separate = '', data_line__close = '',
            data_totals__open = '', data_totals__separate = '', data_totals__close = '', data_totals__fill = '', data_totals__postfix = '',
            totals_line__open = '', totals_line__separate = '', totals_line__close = '', totals_line__post_fix = '',
            totals_data__open = '', totals_data__separate = '', totals_data__close = '', totals_data__fill = '',
            totals_totals__open = '', totals_totals__separate = '', totals_totals__close = '', totals_totals__fill = '', totals_totals__postfix = '',
            data_end__open = '', data_end__separate = '', data_end__close = '',
            totals_end__open = '', totals_end__separate = '', totals_end__close = '',
        )

        if table_format == 'default':
            table_settings.update(
                start_header__open = '+', start_header__separate = '+', start_header__close = '', start_header__fill = '-',
                header_line__open = '|', header_line__separate = '|', header_line__close = '|',
                header_data__open = '+', header_data__separate = '+', header_data__close = '+', header_data__fill = '-',
                data_line__open = '|', data_line__separate = '|', data_line__close = '|',
                totals_line__open = '', totals_line__separate = '', totals_line__close = '', totals_line__post_fix = '',
                data_totals__open = '', data_totals__separate = '', data_totals__close = '', data_totals__fill = '', data_totals__postfix = '',
                totals_data__open = '', totals_data__separate = '', totals_data__close = '', totals_data__fill = '',
                totals_totals__open = '', totals_totals__separate = '', totals_totals__close = '', totals_totals__fill = '', totals_totals__postfix = '',
                data_end__open = '+', data_end__separate = '+', data_end__close = '+',
                totals_end__open = '+', totals_end__separate = '+', totals_end__close = '+',
            )
        elif table_format == 'compact':
            table_settings.update(
                header_line__separate = ' ', header_data__separate = ' ', header_data__fill = '-',
                data_line__separate = ' ', totals_line__separate = ' ',
                data_totals__separate = ' ', data_totals__close = '', data_totals__fill = '-', data_totals__postfix = ' +',
                totals_data__open = ' ',
                totals_totals__separate = ' ', totals_totals__fill = '-', totals_totals__postfix = ' ++',
            )
        else:
            raise Exception(f'Unkwown table format {table_format}.')

        return table_settings