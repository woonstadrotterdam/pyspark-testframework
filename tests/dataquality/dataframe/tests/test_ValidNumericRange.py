import pytest
from pyspark.sql.types import (
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StructField,
    StructType,
)
from testframework.dataquality.tests import ValidNumericRange


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
def test_ValidNumericRange_IntegerTypes(
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

    test = ValidNumericRange(min_value=min_value, max_value=max_value)
    result_df = test.test(df, "value", "primary_key", nullable=True)

    actual_result = result_df.collect()[0]["value__ValidNumericRange"]
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "primary_key, value, min_value, max_value, expected_result",
    [
        (1, 10.5, 0.0, 50.0, True),
        (2, 20.25, 0.0, 50.0, True),
        (3, 30.75, 0.0, 50.0, True),
        (4, None, 0.0, 50.0, True),
        (5, 50.0, 0.0, 50.0, True),
        (6, -10.5, 0.0, 50.0, False),
        (7, 100.1, 0.0, 50.0, False),
        (8, 0.0, 0.0, 50.0, True),
        (9, 50.5, 0.0, 50.5, True),
        (10, -1.0, 0.0, 50.0, False),
        (11, 51.0, 0.0, 50.0, False),
        (12, None, 0.0, 50.0, True),
        (13, 45.5, 30.5, 50.5, True),
        (14, 30.0, 30.0, 30.0, True),
        (15, 30.1, 30.0, 30.0, False),
        (16, 29.9, 30.0, 30.0, False),
    ],
)
@pytest.mark.parametrize("col_type", [FloatType, DoubleType])
def test_ValidNumericRange_FloatTypes(
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

    test = ValidNumericRange(min_value=min_value, max_value=max_value)
    result_df = test.test(df, "value", "primary_key", nullable=True)

    actual_result = result_df.collect()[0]["value__ValidNumericRange"]
    assert actual_result == expected_result


def test_ValidIntegerRange_raises_ValueError(spark):
    with pytest.raises(ValueError):
        ValidNumericRange()  # both min_value and max_value not defined
