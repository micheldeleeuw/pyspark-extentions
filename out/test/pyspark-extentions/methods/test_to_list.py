from tests.conftest import *
import pyspark_extensions
import pyspark.sql.functions as F

def test_to_list(test_set_1):
    data = (
        test_set_1
        .eGroup('customer_id')
        .agg('sum')
        .eRound(2)
    )
    assert data.eToList() == ['5001', '5321', '7232', '7233']
    assert data.eToList('C') == ['5001', '5321', '7232', '7233']
    assert data.eToList('R') == ['5001', 40010, 576.97]
    assert data.eToList('D') == [
        ('5001', 40010, 576.97),
        ('5321', 20019, 115.09),
        ('7232', 70098, 609.16),
        ('7233', 60063, 230.16)
    ]
