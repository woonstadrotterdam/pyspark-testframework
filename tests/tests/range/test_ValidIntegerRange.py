import pytest
from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
    StructField,
    StructType,
)
from testframework.tests.range import ValidIntegerRange


@pytest.mark.parametrize(
    "primary_key, value, min_value, max_value, expected_result",
    [
        (1, 10, 0, 50, True),
        (2, 20, 0, 50, True),
        (3, 30, 0, 50, True),
        (4, None, 0, 50, True),
        (5, 50, 0, 50, True),
        (6, -10, 0, 50, False),
        (7, 100, 0, 50, False),
        (1, 0, 0, 50, True),
        (2, 50, 0, 50, True),
        (3, -1, 0, 50, False),
        (4, 51, 0, 50, False),
        (5, None, 0, 50, True),
    ],
)
@pytest.mark.parametrize("col_type", [IntegerType, LongType, ShortType, ByteType])
def test_valprimary_key_integer_range(
    spark, primary_key, value, min_value, max_value, expected_result, col_type
):
    schema = StructType(
        [
            StructField("primary_key", IntegerType(), False),
            StructField("value", col_type(), True),
        ]
    )
    data = [(primary_key, value)]
    df = spark.createDataFrame(data, schema)

    test = ValidIntegerRange(min_value=min_value, max_value=max_value)
    result_df = test.test(df, "value", "primary_key", nullable=True)

    actual_result = result_df.collect()[0]["value__ValidIntegerRange"]
    assert actual_result == expected_result
