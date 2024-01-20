import logging
from pyspark.sql import DataFrame
from typing import List

log = logging.getLogger(__name__)

def transpose(
        df: DataFrame,
        order_by: List[str] = [],
        max_records: int = 100,
        low_case_columns: bool = False,
        add_data_type: bool = False,
        first_col_gives_column_name: bool = False,
        column_prefix: str = None,
):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    if max_records > 10000:
        raise Exception('max_records may not be bigger than 10.000, as the result set will get to many columns.')

    w = Window().orderBy(F.lit('') if order_by == [] else order_by)

    if first_col_gives_column_name:
        column_prefix = '' if not column_prefix else column_prefix
        rec_name_expr = F.expr(f'concat("{column_prefix}", cast(`{df.columns[0]}` as string))')
    else:
        column_prefix = 'record_' if not column_prefix else column_prefix
        rec_name_expr = F.expr(f'concat("{column_prefix}", format_string("%04d", _rec_id))')

    tran = (df
            .limit(max_records)
            .withColumn('_rec_id', F.row_number().over(w))
            .filter(f'_rec_id <= {max_records}')
            .withColumn('_rec_name', rec_name_expr)
            .select(
                '_rec_id',
                '_rec_name',
                F.array([
                    F.struct(
                        F.lit(i).alias('column_id'),
                        F.lit(col.lower() if low_case_columns else col).alias('column'),
                        F.lit(data_type).alias('data_type'),
                        F.col(col).cast('string').alias('value')
                    )
                    for i, (col, data_type) in enumerate(df.dtypes)
                ]).alias('_record'))
            .select('_rec_name', F.explode('_record').alias('_col'))
            .select('_rec_name', '_col.*')
            .groupBy('column_id', 'column', 'data_type')
            .pivot('_rec_name')
            .agg(F.first('value'))
            .fillna(' ')
            .orderBy('column_id')
            )

    if first_col_gives_column_name:
        tran = tran.filter('column_id != "0"')

    if not add_data_type:
        tran = tran.drop('data_type')

    return tran

