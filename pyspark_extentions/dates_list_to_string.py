import logging
from pyspark.sql import DataFrame

log = logging.getLogger(__name__)

def dates_list_to_string(
        df: DataFrame,
        column: str = None,
) -> DataFrame:

    import pyspark.sql.functions as F

    if column:
        columns = [column]
    else:
        columns = [col for col, type in df.dtypes if type == 'array<date>']

    for col in columns:
        print(col)
        df = (
            df
            .withColumn(col, F.expr(f'''
                substr(aggregate(
                    sort_array(array_distinct(`{col}`)),
                    struct(
                        '' as result, 
                        cast(null as date) as period_start,
                        cast(null as date) as period_end
                    ),
                    (s, x) -> 
                        case
                            when s.period_start is null
                            then struct(
                                s.result, 
                                x as period_start,
                                x as period_end
                            )
                            when x = s.period_end + 1
                            then struct(
                                s.result as result, 
                                s.period_start as period_start,
                                x as period_end
                            )
                            else struct(
                                concat(
                                    s.result, ', ',
                                    if(
                                        s.period_start = s.period_end, 
                                        cast(s.period_start as string), 
                                        concat(
                                            cast(s.period_start as string), ' to ',
                                            cast(s.period_end as string)
                                        )
                                    )
                                ) as result, 
                                x as period_start,
                                x as period_end
                            )
                        end,
                    s -> concat(
                            s.result, ', ',
                            if(
                                s.period_start = s.period_end, 
                                cast(s.period_start as string), 
                                concat(
                                    cast(s.period_start as string), ' to ',
                                    cast(s.period_end as string)
                                )
                            )
                        )
                ), 3)
            '''))
        )

    return (
        df
    )




    return df


