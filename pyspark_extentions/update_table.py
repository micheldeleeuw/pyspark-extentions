import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from delta.tables import *
from typing import List

log = logging.getLogger(__name__)

def update_table(
        updates_df: DataFrame,
        target_table: str,
        primary_key: List[str],
        audit_columns=['created_dtime', 'updated_dtime'],
        skip_technical_updates=True,
):

    spark = SparkSession.builder.getOrCreate()

    primary_key = [primary_key] if isinstance(primary_key, str) else primary_key
    audit_columns = [] if not audit_columns else audit_columns

    assert isinstance(updates_df, DataFrame)
    assert isinstance(target_table, str)
    assert isinstance(primary_key, List)

    try:
        spark.sql(f'describe table {target_table}')
        target_exists = True
    except Exception as e:
        target_exists = False
        if str(e).find('cannot be found') > 0:
            pass
        else:
            raise

    if not target_exists:
        if audit_columns != []:
            updates_df = (
                updates_df
                .withColumn(audit_columns[0], F.expr('current_timestamp()'))
                .withColumn(audit_columns[1], F.expr('current_timestamp()'))
            )

        updates_df.write.saveAsTable(target_table)
        log.info(f'Table {target_table} created.')

        return

    # target exists; merge the data
    delta_table_target = DeltaTable.forName(sparkSession=spark, tableOrViewName=target_table)
    match_condition = ' and '.join([f'target.{col} <=> updates.{col}' for col in primary_key])
    insert_set = dict([(col, f'updates.{col}') for col in updates_df.columns])
    update_set = dict([(col, f'updates.{col}') for col in updates_df.columns if col not in primary_key])

    if audit_columns:
        insert_set[audit_columns[0]] = 'current_timestamp()'
        insert_set[audit_columns[1]] = 'current_timestamp()'
        update_set[audit_columns[1]] = 'current_timestamp()'

    if skip_technical_updates:
        target_cols = spark.table(target_table).columns
        common_cols = [
            col for col in updates_df.columns
            if col not in primary_key
               and col not in audit_columns
        ]

        # if there are new columns, we process all data
        new_columns = [col for col in common_cols if col not in target_cols]
        if len(new_columns) > 0:
            update_condition = 'true'
            log.info(f'New column(s) [{", ".join(new_columns)}] found in update set, all records are processed.')

        else:
            update_condition = (
                    "not (" +
                    " and ".join([f'(target.`{col}` <=> updates.`{col}`)' for col in common_cols]) +
                    ")"
            )

    else:
        update_condition = 'true'

    # do the update, first set some Spark options
    spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")
    spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
    spark.sql("set spark.databricks.delta.schema.autoMerge.enabled = true")

    (
        delta_table_target.alias('target')
        .merge(updates_df.alias('updates'), match_condition)
        .whenMatchedUpdate(set=update_set, condition=update_condition)
        .whenNotMatchedInsert(values = insert_set)
        .execute()
    )
