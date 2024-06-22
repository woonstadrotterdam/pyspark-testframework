import pytest
from pyspark.sql import DataFrame, SparkSession
from testframework.dataquality.tests import (
    CorrectValue,
)


# Sample DataFrame creation helper function
def create_dataframe(spark: SparkSession, data: list, schema: list) -> DataFrame:
    return spark.createDataFrame(data, schema)


# Test cases
def test_correct_value_with_integers(spark):
    data = [(1, 100), (2, 200), (3, 300)]
    schema = ["id", "value"]
    df = create_dataframe(spark, data, schema)

    test = CorrectValue(correct_value=200)
    result_df = test.test(df, col="value", primary_key="id", nullable=False)
    result = result_df.collect()

    expected = [(1, 100, False), (2, 200, True), (3, 300, False)]
    assert result == expected


def test_correct_value_with_strings(spark):
    data = [("a", "apple"), ("b", "banana"), ("c", "cherry")]
    schema = ["id", "value"]
    df = create_dataframe(spark, data, schema)

    test = CorrectValue(correct_value="banana")
    result_df = test.test(df, col="value", primary_key="id", nullable=False)
    result = result_df.collect()

    expected = [("a", "apple", False), ("b", "banana", True), ("c", "cherry", False)]
    assert result == expected


def test_correct_value_nullable_column(spark):
    data = [(1, "apple"), (2, None), (3, "cherry")]
    schema = ["id", "value"]
    df = create_dataframe(spark, data, schema)

    test = CorrectValue(correct_value=None)
    result_df = test.test(df, col="value", primary_key="id", nullable=True)
    result = result_df.collect()

    expected = [(1, "apple", False), (2, None, True), (3, "cherry", False)]
    assert result == expected


def test_correct_value_with_floats(spark):
    data = [(1, 1.1), (2, 2.2), (3, 3.3)]
    schema = ["id", "value"]
    df = create_dataframe(spark, data, schema)

    test = CorrectValue(correct_value=2.2)
    result_df = test.test(df, col="value", primary_key="id", nullable=False)
    result = result_df.collect()

    expected = [(1, 1.1, False), (2, 2.2, True), (3, 3.3, False)]
    assert result == expected


def test_correct_value_nullable_false_with_none_correct_value(spark):
    data = [(1, 100), (2, 200), (3, None)]
    schema = ["id", "value"]
    df = create_dataframe(spark, data, schema)

    test = CorrectValue(correct_value=None)

    with pytest.raises(
        ValueError,
        match="Nullable is False doesn't make sense if correct_value=None, please use nullable=True",
    ):
        test.test(df, col="value", primary_key="id", nullable=False)


def test_correct_value_custom_result_column_name(spark):
    data = [(1, "apple"), (2, "banana"), (3, "cherry")]
    schema = ["id", "value"]
    df = create_dataframe(spark, data, schema)

    test = CorrectValue(correct_value="banana")
    result_df = test.test(
        df, col="value", primary_key="id", nullable=False, result_col="custom_result"
    )
    result = result_df.collect()

    expected = [(1, "apple", False), (2, "banana", True), (3, "cherry", False)]
    assert result == expected
