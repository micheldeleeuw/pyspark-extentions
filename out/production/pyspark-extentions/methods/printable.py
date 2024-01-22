from pyspark.sql import DataFrame
from typing import List, Union
import pyspark.sql.functions as F


class Printable():

    @staticmethod
    def printable(df: DataFrame, precision: int = 2, percentage_precision: int = None,
            columns: Union[str, List[str]] = None) -> DataFrame:

        return Printable(df=df, precision=precision, percentage_precision=percentage_precision,
            columns=columns).make_printable()

    def __init__(self, df: DataFrame, precision: int = 2, percentage_precision: int = None,
            columns: Union[str, List[str]] = None):

        self.columns_printable = [] if not columns else columns
        self.columns_printable = [self.columns_printable] if isinstance(self.columns_printable, str) else var
        self.df = df
        self.precision = precision
        self.percentage_precision = percentage_precision if percentage_precision else precision

    def make_printable(self):

        if self.precision == -3:
            # precision -3 means we want the values to be shown as thousands
            for col in self.columns_nummeric_not_percentage:
                self.df = self.df.withColumn(col, F.expr(f'`{col}` / 1000'))
            self.precision = 0
            self.percentage_precision = 1

        columns_expr = []
        for col in self.columns:
            if col not in self.columns_printable:
                columns_expr.append(f'`{col}`')

            elif col in self.columns_integer:
                columns_expr.append(f'''
                      case
                      when `{col}` is null then ""
                      else
                        replace(
                          replace(
                            replace(
                              format_number(`{col}`, 0), 
                            ",", "xxx"), 
                          ".", ","), 
                        "xxx", ".")
                      end
                      as `{col}`
                ''')

            elif col in self.columns_nummeric_not_percentage:
                columns_expr.append(f'''
                      case
                      when `{col}` is null then ""
                      else
                        replace(
                          replace(
                            replace(
                              format_number(`{col}`, {self.precision}), 
                            ",", "xxx"), 
                          ".", ","), 
                        "xxx", ".")
                      end
                      as `{col}`
                ''')

            elif col in self.columns_percentage:
                columns_expr.append(f'''
                      case
                      when `{col}` is null then ""
                      else
                        replace(
                          replace(
                            replace(
                              format_number(`{col}`, {self.percentage_precision})
                              , 
                            ",", "xxx"), 
                          ".", ","), 
                        "xxx", ".") || '%'
                      end
                      as `{col}`
                ''')

            else:
                columns_expr.append(f'coalesce(cast(`{col}` as string), " ") as `{col}`')

        result = self.df.selectExpr(columns_expr)

        # lpad spaces to right align
        max_lenghts = (
            result.selectExpr([f'greatest(max(length(`{col}`)), length("{col}")) as `{col}`' for col in self.columns]))

        return (result.withColumn('_seq', F.monotonically_increasing_id()).alias('r').join(
            max_lenghts.alias('l')).selectExpr([f'r.`{col}` as `{col}`' if col not in self.columns_printable or (
                col not in self.columns_integer and col not in self.columns_nummeric and col not in self.columns_percentage) else f'lpad(r.`{col}`, l.`{col}`, " ") as `{col}`'
            for col in self.columns]).orderBy('_seq').drop('_seq'))
