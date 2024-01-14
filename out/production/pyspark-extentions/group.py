import logging
from typing import List
from pyspark.sql.column import Column
import pyspark.sql.functions as F

from .pyspark_extentions_tools import PysparkExtentionsTools

log = logging.getLogger(__name__)

class Group(PysparkExtentionsTools):

    @staticmethod
    def group(df, *by):
        return Group(df, *by)

            # .pivot(column, prefix='', sort_by_measure=True),
            # .totalsBy(label='Subtotal', type='sum')
            # .totals(label='Total', type='sum')
            # .agg(agg=None, alias=False)


    def __init__(self, df, *by):
        # check and register some characteristics
        print(by)
        self.df = df
        self.add_dataframe_properties_to_self(df)
        self.by = self.validate_columns_parameters(by)
        self.pivot_column = None


    def pivot(self, column, prefix='', sort_by_measure=True):
        self.pivot_column = column
        self.pivot_prefix = prefix
        self.pivot_sort_by_measure = sort_by_measure

        return self


    def totalsBy(self, by=None, label='Subtotal', type='sum'):
        # when by is not specified, we assume by is on first group by
        if not by and len(self.by) < 2:
            raise Exception('totalsBy only possible with two or more by columns.')
        elif not by:
            self.totals_by = [self.by[0]]
        else:
            self.totals_by = self.validate_columns_parameters(by)

        self.totals_by_label = label
        self.totals_by_type = type

        return self


    def totals(self, label='Total', type='sum'):
        if len(self.by) == 0:
            raise Exception('totals only possible with one or more by columns.')

        self.totals = True
        self.totals_label = label
        self.totals_type = type

        return self


    def agg(self, *args, **kwargs):
        # do the actual aggregation

        # transform generic parameters to named local variables
        agg = kwargs['agg'] if 'agg' in kwargs.keys() else args
        alias = kwargs['alias'] if 'alias' in kwargs.keys() else False

        self.columns_aggregable = [
            col for col in self.columns if col not in self.by and col != self.pivot_column
        ]

        # default aggregate and scalar to list
        if not agg or agg == []:
            self.agg = [F.count('*').alias('count')]
        elif isinstance(agg, Column):
            self.agg = [agg]
        elif not isinstance(agg, List):
            self.agg = [agg]
        else:
            self.agg = agg

        
        self.alias = alias
        self._build_aggregate()

        from pprint import pprint
        pprint(vars(self))

        if self.pivot_column:
            result = (
                self.df
                .selectExpr('*', f'concat("{self.pivot_prefix}", {self.pivot_column}) as __pivot_col')
                .groupBy(*self.by)
                .pivot('__pivot_col')
            )
        else:
            result = self.df.groupBy(*self.by)

        result = result.agg(*self.agg)
        # df_agg = df_agg.muNormalizeColumnNames()
        # df_agg = df_agg.withColumn('_group', F.lit(1))

        return result


    def _build_aggregate(self):
        # We (re)build the aggregation expressions.
        # 1. If it is string with a single aggregation function without parameters
        #    then that function is applied to all columns for which that function
        #    makes sense.
        # 2. Otherwise, if it is a string, we assume is in SQL expression that can
        #    be applied directly. The onlu thing we do is that we implement sorted_columns()
        #    and columns() functionality.
        # 3. Otherwise, we assume the expression is already a column expression that can
        #    be applied directly

        # Note that we implement each aggregate result columns as an array as we want
        # to sort the columns after their determination.

        agg = []
        for i, ag_el in enumerate(self.agg):

            # 1. single aggregation function
            if isinstance(ag_el, str) and ag_el in self.single_aggregation_function():
                for j, col in enumerate(self.columns_aggregable):
                    result_expression = None

                    if col in self.columns_nummeric and ag_el == 'avg_null':
                        result_expression = f'avg(coalesce({col}, 0))'
                    elif ag_el == 'count_distinct':
                        result_expression = f'count(distinct({col}))'
                    elif ag_el == 'count_null':
                        result_expression = f'sum(case when {col} is null then 1 else 0 end)'
                    elif ag_el == 'count_not_null':
                        result_expression = f'sum(case when {col} is not null then 1 else 0 end)'
                    elif col in self.columns_nummeric or ag_el in (
                            'max', 'min', 'count', 'first', 'collect_set', 'collect_list'):
                        result_expression = f'{ag_el}({col})'
                    else:
                        # nummeric aggregation on non nummeric columns are not included
                        pass

                    if result_expression:
                        result_column_name = f"{ag_el}_{col}" if self.alias else col
                        agg.append([2, i, j, f'{result_expression} as `{result_column_name}`'])

            # 2. SQL expression (string), add sorted_columns() and columns() functionality.
            elif isinstance(ag_el, str):
                ag_el = f'"{", ".join(sorted(self.columns))}" as columns' if ag_el == 'sorted_columns()' else ag_el
                ag_el = ag_el.replace('sorted_columns()', f'"{", ".join(sorted(self.columns))}"')
                ag_el = f'"{", ".join(self.columns)}" as columns' if ag_el == 'columns()' else ag_el
                ag_el = ag_el.replace('columns()', f'"{", ".join(self.columns)}"')
                agg.append([1, i, 0, ag_el])

            # 3. Column expression
            else:
                agg.append([1, i, 0, ag_el])

        agg = sorted(agg, key = lambda x: x[0]*100000 + x[2]*1000 + x[1])
        agg = [el[3] for el in agg]
        agg = [element if isinstance(element, Column) else F.expr(element) for element in agg]
        self.agg = agg


    def single_aggregation_function(self):
        return (
            'max',
            'min',
            'sum',
            'avg',
            'avg_null',
            'count',
            'count_distinct',
            'count_null',
            'count_not_null',
            'first',
            'last',
            'collect_set',
            'collect_list'
        )


'''


    df_agg = df.muCE(f'concat("{prefix_pivot_column}", {pivot_column}) as __pivot_col') if pivot_column else df
    df_agg = df_agg.groupBy(*by)
    df_agg = df_agg.pivot('__pivot_col') if pivot_column else df_agg
    df_agg = df_agg.agg(*agg)
    df_agg = df_agg.muNormalizeColumnNames()
    df_agg = df_agg.withColumn('_group', F.lit(1))

    if totals_by != []:
        df_agg = (
            df_agg
            .selectExpr(f'cast({by[0]} as string) as {by[0]}', f'* except({by[0]})')
            .unionByName(
                df
                .muGroup(
                    by=totals_by,
                    agg=agg,
                    pivot_column=pivot_column,
                    alias=alias,
                    prefix_pivot_column=prefix_pivot_column,
                    pivot_column_sort_by_measure=pivot_column_sort_by_measure,
                )
                .selectExpr(
                    f'"{totals_label}" as {[col for col in by if col not in totals_by][0]}',
                    '*'
                )
                .withColumn('_group', F.lit(2)),
                allowMissingColumns=True
            )
        )

    if totals:
        df_agg = (
            df_agg
            .selectExpr(f'cast({by[0]} as string) as {by[0]}', f'* except({by[0]})')
            .unionByName(
                df
                .muGroup(
                    by=[],
                    agg=agg,
                    pivot_column=pivot_column,
                    alias=alias,
                    prefix_pivot_column=prefix_pivot_column,
                    pivot_column_sort_by_measure=pivot_column_sort_by_measure,
                )
                .selectExpr(f'"{totals_label}" as {by[0]}', '*')
                .withColumn('_group', F.lit(3)),
                allowMissingColumns=True
            )
        )

    df_agg = df_agg.orderBy(
        # grand totals at bottom
        F.expr('if(_group in (1,2), 1, 2)'),
        # then each total group
        *totals_by,
        # totals at bottom of group
        '_group',
        # then sorting of the rows
        *by,
    )

    return df_agg.drop('_group')

DataFrame.muGroup = muGroup

# o.muGroup([], ['sum(customer_demand_amt)'], totals=True).muShow(0)
o.muGroup(['fascia_id', 'order_date'], ['sum(customer_demand_amt)'], totals=True, totals_by='fascia_id', totals_label="0").muShow(0)
o.muGroup(['order_date'], ['sum(customer_demand_amt)'], pivot_column='fascia_id', totals=True).muShow(0)

'''