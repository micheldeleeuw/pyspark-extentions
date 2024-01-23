import logging
from typing import List, Union, Dict
from typing_extensions import Self
from pyspark.sql.column import Column
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .validate_columns import validate_columns


log = logging.getLogger(__name__)

class Group():

    @staticmethod
    def group(df: DataFrame, *by:List[str]) -> Self:
        return Group(df, *by)


    def __init__(self, df: DataFrame, *by: List[str]):
        # check and register some characteristics
        self.df = df
        self.by = [] if not by else by
        self.by = validate_columns(df, self.by)
        self.columns_aggregable = [col for col in self.df.columns if col not in self.by]
        self.pivot_column = None
        self.totals_by = []
        self.totals_indicator = False
        self.keep_group_column = False


    def pivot(self, column: str, prefix: str = '') -> Self:
        self.pivot_column = column
        self.pivot_prefix = prefix
        self.columns_aggregable = [col for col in self.columns_aggregable if col != self.pivot_column]

        return self


    def totalsBy(self, by: List[str] = None, label: str = 'Subtotal',
                 keep_group_column: bool = False):
        # when by is not specified, we assume by is on first group by
        if isinstance(by, str):
            by = [by]

        if len(self.by) < 2:
            raise Exception('totalsBy only possible with two or more by columns.')
        elif not by:
            self.totals_by = [self.by[0]]
        else:
            self.totals_by = validate_columns(self.df, by)

        self.totals_by_label = label
        self.keep_group_column = keep_group_column

        return self


    def totals(self, label: str = 'Total',
               keep_group_column: bool = False) -> Self:
        if len(self.by) == 0:
            raise Exception('totals only possible with one or more by columns.')

        self.totals_indicator = True
        self.totals_label = label
        self.keep_group_column = keep_group_column

        return self


    def agg(self, *args: List[Union[str, Column]], **kwargs: Dict) -> DataFrame:
        ''' Do the actual aggregation
        
        :param args: the aggregation expressions and columns
        :param kwargs:  holds the alias parameter
        :return: DataFrame with the aggregation
        '''

        # transform generic parameters to named local variables
        agg = list(kwargs['agg']) if 'agg' in kwargs.keys() else list(args)
        alias = kwargs['alias'] if 'alias' in kwargs.keys() else False
        normalize_column_names = kwargs['normalize_column_names'] if 'normalize_column_names' in kwargs.keys() else True

        # default aggregate and scalar to list
        if isinstance(agg, List) and len(agg) > 0 and isinstance(agg[0], List):
            agg = agg[0]

        if not agg or agg == []:
            self.agg_exprs = [F.count('*').alias('count')]
        elif isinstance(agg, Column):
            self.agg_exprs = [agg]
        elif not isinstance(agg, List):
            self.agg_exprs = [agg]
        else:
            self.agg_exprs = agg

        # Force alias when more than one single aggregation function is requested
        if len([
            value for value in agg
            if isinstance(value, str) and value in self.single_aggregation_functions()
        ]) > 1:
            alias = True

        self.alias = alias
        self.normalize_aggregate_column_names = normalize_column_names
        self._build_aggregate()

        if self.pivot_column:
            result = (
                self.df
                .selectExpr('*', f'concat("{self.pivot_prefix}", {self.pivot_column}) as __pivot_col')
                .groupBy(*self.by)
                .pivot('__pivot_col')
            )
        else:
            result = self.df.groupBy(*self.by)

        result = result.agg(*self.agg_exprs)
        result = result.withColumn('_group', F.lit(1))
        self.result = result

        if self.totals_by != []:
            self._totals_by()

        if self.totals_indicator:
            self._totals()

        # sort the result
        self.result =(
            self.result
            .orderBy(
                # grand totals at bottom
                F.expr('if(_group in (1,2), 1, 2)'),
                # then each totals by group
                *self.totals_by,
                # by totals at bottom of by group
                '_group',
                # then sorting within the by group
                *self.by,
            )
        )

        if not self.keep_group_column:
            self.result = self.result.drop('_group')
        else:
            self.result = self.result.withColumn('_seq', F.monotonically_increasing_id())

        if self.normalize_aggregate_column_names:
            self._normalize_aggregate_column_names()

        return self.result


    def _normalize_aggregate_column_names(self):
        self.result = (
            self.result
            .eNormalizeColumnNames(columns=[col for col in self.result.columns if col not in self.by])
        )


    def _totals_by(self):
        # first column must be able to hold totals label
        self._first_column_to_string()

        # group the original data, but without by
        subtotals = Group(self.df, *self.totals_by)

        if self.pivot_column:
            subtotals = subtotals.pivot(self.pivot_column, self.pivot_prefix)

        subtotals = (
            subtotals
            .agg(agg=self.agg_exprs, alias=self.alias)
            .selectExpr(f'"{self.totals_by_label}" as {[col for col in self.by if col not in self.totals_by][0]}', '*')
            .withColumn('_group', F.lit(2))
        )

        self.result = self.result.unionByName(subtotals, allowMissingColumns=True)


    def _totals(self):
        # first column must be able to hold totals label
        self._first_column_to_string()

        # group the original data, but without by
        totals = Group(self.df)

        if self.pivot_column:
            totals = totals.pivot(self.pivot_column, self.pivot_prefix)

        totals = (
            totals
            .agg(agg=self.agg_exprs, alias=self.alias)
            .selectExpr(f'"{self.totals_label}" as {self.by[0]}', '*')
            .withColumn('_group', F.lit(3))
        )

        self.result = self.result.unionByName(totals, allowMissingColumns=True)


    def _first_column_to_string(self):
        # (Sub)totals label in column 1 means it must be string
        result_columns = self.result.columns
        self.result = (
            self.result
            .selectExpr(
                f'cast(`{result_columns[0]}` as string) as `{result_columns[0]}`',
                *[f'`{col}`' for col in result_columns[1:]]
            )
        )


    def _build_aggregate(self) -> None:
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
        for i, ag_el in enumerate(self.agg_exprs):

            # 1. single aggregation function
            if isinstance(ag_el, str) and ag_el in self.single_aggregation_functions():
                for j, col in enumerate(self.columns_aggregable):
                    result_expression = None

                    if col in self.df.eColumnsNummeric and ag_el == 'avg_null':
                        result_expression = f'avg(coalesce(`{col}`, 0))'
                    elif ag_el == 'count_distinct':
                        result_expression = f'count(distinct(`{col}`))'
                    elif ag_el == 'count_null':
                        result_expression = f'sum(case when `{col}` is null then 1 else 0 end)'
                    elif ag_el == 'count_not_null':
                        result_expression = f'sum(case when `{col}` is not null then 1 else 0 end)'
                    elif col in self.df.eColumnsNummeric or ag_el in (
                            'max', 'min', 'count', 'first', 'collect_set', 'collect_list'):
                        result_expression = f'{ag_el}(`{col}`)'
                    else:
                        # nummeric aggregation on non nummeric columns are not included
                        pass

                    if result_expression:
                        result_column_name = f"{ag_el}_{col}" if self.alias else col
                        agg.append([2, i, j, f'{result_expression} as `{result_column_name}`'])

            # 2. SQL expression (string), add sorted_columns() and columns() functionality.
            elif isinstance(ag_el, str):
                ag_el = f'"{", ".join(sorted(self.df.columns))}" as columns' if ag_el == 'sorted_columns()' else ag_el
                ag_el = ag_el.replace('sorted_columns()', f'"{", ".join(sorted(self.df.columns))}"')
                ag_el = f'"{", ".join(self.df.columns)}" as columns' if ag_el == 'columns()' else ag_el
                ag_el = ag_el.replace('columns()', f'"{", ".join(self.df.columns)}"')
                agg.append([1, i, 0, ag_el])

            # 3. Column expression
            else:
                agg.append([1, i, 0, ag_el])

        agg = sorted(agg, key = lambda x: x[0]*100000 + x[2]*1000 + x[1])
        agg = [el[3] for el in agg]
        agg = [element if isinstance(element, Column) else F.expr(element) for element in agg]
        self.agg_exprs = agg

    ## define shortcuts for commonly used agg usages
    def count(self) -> DataFrame:
        return self.agg()


    def min(self) -> DataFrame:
        return self.agg('min')


    def max(self) -> DataFrame:
        return self.agg('max')


    def sum(self) -> DataFrame:
        return self.agg('sum')


    def avg(self) -> DataFrame:
        return self.agg('avg')


    def avg_null(self) -> DataFrame:
        return self.agg('avg_null')


    def count_distinct(self) -> DataFrame:
        return self.agg('count_distinct')


    def count_null(self) -> DataFrame:
        return self.agg('count_null')


    def count_not_null(self) -> DataFrame:
        return self.agg('count_not_null')


    def first(self) -> DataFrame:
        return self.agg('first')


    def last(self) -> DataFrame:
        return self.agg('last')


    def collect_set(self) -> DataFrame:
        return self.agg('collect_set')


    def collect_list(self) -> DataFrame:
        return self.agg('collect_list')


    def single_aggregation_functions(self) -> List[str]:
        return (
            'min',
            'max',
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
