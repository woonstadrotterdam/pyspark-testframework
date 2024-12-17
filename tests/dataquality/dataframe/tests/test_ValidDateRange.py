from datetime import date, datetime

import pytest
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from testframework.dataquality.tests import ValidDateRange


@pytest.mark.parametrize(
    "primary_key, value, min_date, max_date, expected_result",
    [
        (1, "2023-01-01", "2022-01-01", "2024-01-01", True),
        (2, "2022-01-01", "2022-01-01", "2024-01-01", True),
        (3, "2024-01-01", "2022-01-01", "2024-01-01", True),
        (4, None, "2022-01-01", "2024-01-01", True),
        (5, "2021-12-31", "2022-01-01", "2024-01-01", False),
        (6, "2024-01-02", "2022-01-01", "2024-01-01", False),
        # Test with only min_date
        (7, "2023-01-01", "2022-01-01", None, True),
        (8, "2021-12-31", "2022-01-01", None, False),
        # Test with only max_date
        (9, "2023-01-01", None, "2024-01-01", True),
        (10, "2024-01-02", None, "2024-01-01", False),
    ],
)
@pytest.mark.parametrize("col_type", [DateType, TimestampType, StringType])
def test_ValidDateRange(
    spark, primary_key, value, min_date, max_date, expected_result, col_type
):
    schema = StructType(
        [
            StructField("primary_key", IntegerType(), False),
            StructField("value", col_type(), True),
        ]
    )

    # Convert string dates to datetime objects based on column type
    if value is not None:
        if isinstance(col_type(), DateType):
            value = datetime.strptime(value, "%Y-%m-%d").date()
        elif isinstance(col_type(), TimestampType):
            value = datetime.strptime(value, "%Y-%m-%d")
        # For StringType, keep as string

    data = [(primary_key, value)]
    df = spark.createDataFrame(data, schema)

    test = ValidDateRange(min_date=min_date, max_date=max_date)
    result_df = test.test(df, "value", "primary_key", nullable=True)

    actual_result = result_df.collect()[0]["value__ValidDateRange"]
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "date_format, value, min_date, max_date, expected_result",
    [
        ("dd-MM-yyyy", "01-01-2023", "01-01-2022", "01-01-2024", True),
        ("MM/dd/yyyy", "01/01/2023", "01/01/2022", "01/01/2024", True),
        ("yyyy.MM.dd", "2023.01.01", "2022.01.01", "2024.01.01", True),
        ("dd-MM-yyyy", "01-01-2021", "01-01-2022", "01-01-2024", False),
    ],
)
def test_ValidDateRange_different_formats(
    spark, date_format, value, min_date, max_date, expected_result
):
    schema = StructType(
        [
            StructField("primary_key", IntegerType(), False),
            StructField("value", StringType(), True),
        ]
    )
    data = [(1, value)]
    df = spark.createDataFrame(data, schema)

    test = ValidDateRange(min_date=min_date, max_date=max_date, date_format=date_format)
    result_df = test.test(df, "value", "primary_key", nullable=True)

    actual_result = result_df.collect()[0]["value__ValidDateRange"]
    assert actual_result == expected_result


def test_ValidDateRange_raises_ValueError(spark):
    with pytest.raises(ValueError):
        ValidDateRange()  # both min_date and max_date not defined


@pytest.mark.parametrize(
    "min_date, max_date",
    [
        (datetime(2022, 1, 1), "2024-01-01"),
        ("2022-01-01", datetime(2024, 1, 1)),
        (datetime(2022, 1, 1), datetime(2024, 1, 1)),
    ],
)
def test_ValidDateRange_datetime(spark, min_date, max_date):
    schema = StructType(
        [
            StructField("primary_key", IntegerType(), False),
            StructField("value", DateType(), True),
        ]
    )
    data = [(1, datetime(2023, 1, 1))]
    df = spark.createDataFrame(data, schema)

    test = ValidDateRange(min_date=min_date, max_date=max_date)
    result_df = test.test(df, "value", "primary_key", nullable=True)

    actual_result = result_df.collect()[0]["value__ValidDateRange"]
    assert actual_result


@pytest.mark.parametrize(
    "min_date, max_date",
    [
        (date(2022, 1, 1), "2024-01-01"),
        ("2022-01-01", date(2024, 1, 1)),
        (date(2022, 1, 1), date(2024, 1, 1)),
    ],
)
def test_ValidDateRange_date(spark, min_date, max_date):
    schema = StructType(
        [
            StructField("primary_key", IntegerType(), False),
            StructField("value", DateType(), True),
        ]
    )
    data = [(1, date(2023, 1, 1))]
    df = spark.createDataFrame(data, schema)

    test = ValidDateRange(min_date=min_date, max_date=max_date)
    result_df = test.test(df, "value", "primary_key", nullable=True)

    actual_result = result_df.collect()[0]["value__ValidDateRange"]
    assert actual_result
