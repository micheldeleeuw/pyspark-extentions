# global DataFrame
# from pyspark.sql import DataFrame
# from ..functions import *
# from .muCE import *


def group(
        df,
        group_columns=None,
        agg=None,
        pivot_column=None,
        alias=False,
        prefix_pivot_column='',
        pivot_column_sort_by_measure=True
):

    import pyspark.sql.functions as F
    from typing import List
    from pyspark.sql.column import Column

    df = self

    group_columns = [] if not group_columns else group_columns
    cols = [col for col in df.columns if col not in group_columns and col != pivot_column]
    cols_nummeric = [dtype[0] for dtype in df.dtypes if dtype[1] in
                     ('double', 'integer', 'int', 'short', 'long', 'float', 'bigint') or dtype[1].find('decimal')>-1
                     ]

    if isinstance(agg, Column):
        agg = [agg]
    elif not agg:
        agg = [F.count('*').alias('count')]
    elif not isinstance(agg, List):
        agg = [agg]

    agg_old = agg
    agg = []
    for i, ag_el in enumerate(agg_old):
        if isinstance(ag_el, str) and ag_el in ('max', 'min', 'avg', 'count', 'count_distinct', 'sum', 'avg_null', 'sum_null',
                                                'count_null', 'count_not_null', 'first', 'collect_set', 'collect_list'):
            for j, col in enumerate(cols):
                agg_name = f"{ag_el}_{col}" if alias else col
                if col in cols_nummeric and ag_el in ('avg_null'):
                    ag_el_adjused = ag_el.replace('_null', '')
                    agg.append([2, i, j, f'{ag_el_adjused}(nvl({col}, 0)) as {agg_name}'])
                elif ag_el == 'count_distinct':
                    agg.append([2, i, j, f'count(distinct({col})) as {agg_name}'])
                elif ag_el == 'count_null':
                    agg.append([2, i, j, f'sum(case when {col} is null then 1 else 0 end) as {agg_name}'])
                elif ag_el == 'count_not_null':
                    agg.append([2, i, j, f'sum(case when {col} is not null then 1 else 0 end) as {agg_name}'])
                elif col in cols_nummeric or ag_el in ('max', 'min', 'count', 'first', 'collect_set', 'collect_list'):
                    agg.append([2, i, j, f'{ag_el}({col}) as {agg_name}'])
        elif isinstance(ag_el, str):
            ag_el = f'"{", ".join(sorted(df.columns))}" as columns' if ag_el == 'sorted_columns()' else ag_el
            ag_el = ag_el.replace('sorted_columns()', f'"{", ".join(sorted(df.columns))}"')
            ag_el = f'"{", ".join(df.columns)}" as columns' if ag_el == 'columns()' else ag_el
            ag_el = ag_el.replace('columns()', f'"{", ".join(df.columns)}"')
            agg.append([1, i, 0, ag_el])
        else:
            agg.append([1, i, 0, ag_el])

    agg = sorted(agg, key = lambda x: x[0]*100000 + x[2]*1000 + x[1])

    agg = [el[3] for el in agg]
    agg = [element if isinstance(element, Column) else F.expr(element) for element in agg]

    df = df.muCE(f'concat("{prefix_pivot_column}", {pivot_column}) as __pivot_col') if pivot_column else df
    df = df.groupBy() if not group_columns else df.groupBy(group_columns)
    df = df.pivot('__pivot_col') if pivot_column else df
    df = df.agg(*agg)
    df = df.orderBy(group_columns) if group_columns else df

    return df.muNormalizeColumnNames()

# DataFrame.muGroup = muGroup