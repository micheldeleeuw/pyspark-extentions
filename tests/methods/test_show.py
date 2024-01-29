from tests.conftest import *
import pyspark_extensions

def _show(df, table_format):
    print()
    df.eShow(table_format=table_format, title=f'table format: {table_format}')


def test_show(test_set_1):
    df = test_set_1.filter('customer_id in ("5001", "5321")')

    data_no_group = (
        df
        .eGroup('customer_id')
        .agg('sum')
    )
    data_totals = (
        df
        .eGroup('customer_id')
        .totals(keep_group_column=True)
        .agg('sum')
    )
    data_sub_totals = (
        df
        .eGroup('customer_id', 'line_id')
        .totalsBy('customer_id', keep_group_column=True)
        .agg('sum')
    )
    data_totals_and_sub_totals = (
        df
        .eGroup('customer_id', 'line_id')
        .totalsBy('customer_id', keep_group_column=True)
        .totals(keep_group_column=True)
        .agg('sum')
    )



    for table_format in ['default', 'compact']:
        print(f'table format: {table_format}')
        print()
        data_no_group.eShow(table_format=table_format)
        print()
        data_totals.eShow(table_format=table_format)
        print()
        data_sub_totals.eShow(table_format=table_format)
        print()
        data_totals_and_sub_totals.eShow(table_format=table_format)
        print()


    return

    assert test_set_1.filter('false').eShow() == None


    for table_format in ['default', 'compact']:

        print('========== data 1')
        print('==========')
        print(
            ToString._array_to_string(
                data,
                ['cust\nomer_\nid', 'line_id', 'article\n_description', 'sales_amount', '_group'],
                True,
                ['left', 'left', 'left', 'right'],
                False,
                table_format
            )
        )

        print('========== data 2')
        print(
            ToString._array_to_string(
                data2,
                ['cust\nomer_\nid', 'line_id', 'article\n_description', 'sales_amount', '_group'],
                True,
                ['left', 'left', 'left', 'right'],
                False,
                table_format,
            )
        )
        print('========== data 3')
        print(
            ToString._array_to_string(
                data3,
                ['cust\nomer_\nid', 'line_id', 'article\n_description', 'sales_amount', '_group'],
                True,
                ['left', 'left', 'left', 'right'],
                False,
                table_format,
            )
        )
        print('========== data 4')
        print(
            ToString._array_to_string(
                data4,
                ['cust\nomer_\nid', 'line_id', 'article\n_description', 'sales_amount', '_group'],
                True,
                ['left', 'left', 'left', 'right'],
                False,
                table_format,
            )
        )
        print('==========')