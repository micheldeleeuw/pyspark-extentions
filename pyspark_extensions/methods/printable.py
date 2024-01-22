import logging

from pyspark.sql import DataFrame
from typing import List, Union, Tuple
import pyspark.sql.functions as F


class Printable():

    @staticmethod
    def printable(
            df: DataFrame,
            precision: int = None,
            percentage_precision: int = None,
            k: bool = False,
            k_label = '',
            columns: Union[str, List[str]] = None,
            align_right: bool = True,
        ) -> DataFrame:

        if k:
            precision = precision if precision else 0
            percentage_precision = percentage_precision if percentage_precision else 1
        else:
            precision = precision if precision else 2
            percentage_precision = percentage_precision if percentage_precision else 1

        if precision == -1:
            # round 0 decimals and >1000 with k
            k = True
            k_label = 'k' if k_label == '' else k_label

        columns = df.eValidateColumns(columns)
        columns_nummeric = df.eColumnsNummeric
        columns_int = df.eColumnsInteger
        columns_percentage = df.eColumnsPercentage

        columns_transforms = []
        for column in df.columns:
            if column not in columns:
                columns_transforms.append(f'`{column}`')
            if column in columns_percentage:
                columns_transforms.append(
                    Printable._transform_column(column, percentage_precision)
                )
            elif column in columns_int and k:
                columns_transforms.append(
                    Printable._transform_column(column, precision, k, k_label)
                )
            elif column in columns_int:
                columns_transforms.append(
                    Printable._transform_column(column, 0, k, k_label)
                )
            elif column in columns_nummeric:
                columns_transforms.append(
                    Printable._transform_column(column, precision, k, k_label)
                )
            else:
                columns_transforms.append(f'`{column}`')

        result = df.selectExpr(*columns_transforms)

        if align_right:
            result = Printable._align_right(result, [col for col in columns_nummeric if col in columns])

        return result

    @staticmethod
    def _transform_column(column, precision, k=False, k_label=''):
        # pre process if thousands (1.200 -> 1k) are needed
        unforced_k = True if k and precision == -1 else False
        if unforced_k:
            # only make k if > 1000 or < 1000
            column_expression = f'if(abs(`{column}`) >= 1000, `{column}` / 1000, `{column}`)'
            precision = 0
        elif k:
            # always make k
            column_expression = f'`{column}` / 1000'
        else:
            # no k needed
            column_expression = f'`{column}`'

        # round to the needed precision and format with thousands separator
        column_expression = f'''
            case
            when {column_expression} is null then ""
            else
                replace(
                  replace(
                    replace(
                      format_number({column_expression}, {precision}),
                    ",", "xxx"),
                  ".", ","),
                "xxx", ".")
              end
        '''

        if unforced_k:
            column_expression = f'if(abs(`{column}`) >= 1000, concat({column_expression}, "{k_label}"), {column_expression})'
        elif k:
            column_expression = f'concat({column_expression}, "{k_label}")'

        column_expression = f'{column_expression} as `{column}`'

        return column_expression


    @staticmethod
    def _align_right(df: DataFrame, columns: List[str]):
        # lpad spaces to right align all columns that were previously nummeric

        columns = [col for col in columns if not col.startswith('_')]

        max_lenghts = (
            df
            .selectExpr([
                f'greatest(max(length(`{col}`)), length("{col}")) as `{col}`'
                for col in columns
            ])
        )

        return (
            df
            .withColumn('_seq', F.monotonically_increasing_id()).alias('r')
            .join(max_lenghts.alias('l'))
            .selectExpr([
                f'r.`{col}` as `{col}`'
                if col not in columns
                else f'lpad(r.`{col}`, l.`{col}`, " ") as `{col}`'
                for col in df.columns
            ])
            .orderBy('_seq')
            .drop('_seq')
        )
