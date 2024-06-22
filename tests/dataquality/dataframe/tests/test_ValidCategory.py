import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from testframework.dataquality.tests import ValidCategory


@pytest.fixture
def sample_df(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("category", StringType(), True),
        ]
    )
    data = [
        Row(id=1, category="A"),
        Row(id=2, category="B"),
        Row(id=3, category="C"),
        Row(id=4, category="D"),
        Row(id=5, category=None),
    ]
    return spark.createDataFrame(data, schema)


def test_valid_category_all_valid(spark, sample_df):
    valid_categories = {"A", "B", "C", "D"}
    test = ValidCategory(categories=valid_categories)
    result_df = test.test(sample_df, "category", primary_key="id", nullable=False)

    expected_results = [
        True,
        True,
        True,
        True,
        False,
    ]  # None is not in valid categories and nullable is False

    assert result_df.count() == 5
    result = [row.category__ValidCategory for row in result_df.collect()]
    assert result == expected_results


def test_valid_category_with_null(spark, sample_df):
    valid_categories = {"A", "B", "C", "D"}
    test = ValidCategory(categories=valid_categories)
    result_df = test.test(sample_df, "category", primary_key="id", nullable=True)

    expected_results = [True, True, True, True, True]  # Nullable is True

    assert result_df.count() == 5
    result = [row.category__ValidCategory for row in result_df.collect()]
    assert result == expected_results


def test_valid_category_non_nullable(spark, sample_df):
    valid_categories = {"A", "B", "C", "D", None}
    test = ValidCategory(categories=valid_categories)
    with pytest.raises(ValueError):
        test.test(sample_df, "category", primary_key="id", nullable=False)


def test_valid_category_custom_result_column(spark, sample_df):
    valid_categories = {"A", "B", "C", "D"}
    test = ValidCategory(categories=valid_categories)
    result_df = test.test(
        sample_df, "category", primary_key="id", nullable=False, result_col="is_valid"
    )

    expected_results = [
        True,
        True,
        True,
        True,
        False,
    ]  # None is not in valid categories

    assert result_df.count() == 5
    result = [row.is_valid for row in result_df.collect()]
    assert result == expected_results


def test_valid_category_invalid_column_type(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("category", IntegerType(), True),
        ]
    )
    data = [Row(id=1, category=1), Row(id=2, category=2), Row(id=3, category=3)]
    df = spark.createDataFrame(data, schema)

    valid_categories = {"1", "2", "3"}
    test = ValidCategory(categories=valid_categories)
    with pytest.raises(TypeError):
        test.test(df, "category", primary_key="id", nullable=True)


def test_valid_category_empty_categories(spark, sample_df):
    valid_categories = set()
    test = ValidCategory(categories=valid_categories)
    result_df = test.test(sample_df, "category", primary_key="id", nullable=True)

    expected_results = [
        False,
        False,
        False,
        False,
        True,
    ]  # No valid categories, but nullable is True

    assert result_df.count() == 5
    result = [row.category__ValidCategory for row in result_df.collect()]
    assert result == expected_results


def test_valid_category_partial_match(spark, sample_df):
    valid_categories = {"A", "B"}
    test = ValidCategory(categories=valid_categories)
    result_df = test.test(sample_df, "category", primary_key="id", nullable=False)

    expected_results = [True, True, False, False, False]  # Only A and B are valid

    assert result_df.count() == 5
    result = [row.category__ValidCategory for row in result_df.collect()]
    assert result == expected_results
