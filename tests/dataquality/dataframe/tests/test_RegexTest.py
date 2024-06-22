from pyspark.sql.types import StringType, StructField, StructType
from testframework.dataquality.tests import (
    RegexTest as RegexTst,  # to prevent pytest confusion
)


def test_regex_test_method(spark):
    # Schema and data for the test
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "test@example.com"),
        ("2", "invalid-email"),
        ("3", None),
        ("4", "another@test.com"),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    # Instantiate RegexTst
    regex_test = RegexTst(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")

    # Apply the test
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=True)

    # Collect results
    results = result_df.select("id", "value", "value__email_test").collect()

    # Check the results
    expected_results = [
        ("1", "test@example.com", True),  # Matches the regex
        ("2", "invalid-email", False),  # Does not match the regex
        ("3", None, True),  # Null value, should be True due to nullable=True
        ("4", "another@test.com", True),  # Matches the regex
    ]

    assert results == expected_results


def test_regex_test_no_match(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "no-email-here"),
        ("2", "still-no-email"),
        ("3", None),
    ]

    df = spark.createDataFrame(data, schema)

    regex_test = RegexTst(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")

    result_df = regex_test.test(df, col="value", primary_key="id", nullable=False)

    results = result_df.select("id", "value", "value__email_test").collect()

    expected_results = [
        ("1", "no-email-here", False),  # Does not match the regex
        ("2", "still-no-email", False),  # Does not match the regex
        ("3", None, False),  # Null value, should be False due to nullable=False
    ]

    assert results == expected_results


def test_regex_test_partial_match(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "test1@example.com"),
        ("2", "example@test.org"),
        ("3", "not-an-email"),
        ("4", "example@domain.com"),
    ]

    df = spark.createDataFrame(data, schema)

    regex_test = RegexTst(name="domain_test", pattern=r"@example\.com$")

    result_df = regex_test.test(df, col="value", primary_key="id", nullable=False)

    results = result_df.select("id", "value", "value__domain_test").collect()

    expected_results = [
        ("1", "test1@example.com", True),  # Matches the regex
        ("2", "example@test.org", False),  # Does not match the regex
        ("3", "not-an-email", False),  # Does not match the regex
        ("4", "example@domain.com", False),  # Does not match the regex
    ]

    assert results == expected_results


def test_regex_test_empty_string(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", ""),
        ("2", " "),
        ("3", None),
        ("4", "not-empty"),
    ]

    df = spark.createDataFrame(data, schema)

    regex_test = RegexTst(name="non_empty_test", pattern=r"^.+$")

    result_df = regex_test.test(df, col="value", primary_key="id", nullable=True)

    results = result_df.select("id", "value", "value__non_empty_test").collect()

    expected_results = [
        ("1", "", False),  # Empty string, does not match regex
        ("2", " ", True),  # Space, matches regex
        ("3", None, True),  # Null value, should be True due to nullable=True
        ("4", "not-empty", True),  # Non-empty, matches regex
    ]

    assert results == expected_results


def test_regex_test_special_characters(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "test@example.com"),
        ("2", "special@char$.com"),
        ("3", None),
        ("4", "test@domain.net"),
    ]

    df = spark.createDataFrame(data, schema)

    regex_test = RegexTst(name="special_char_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")

    result_df = regex_test.test(df, col="value", primary_key="id", nullable=True)

    results = result_df.select("id", "value", "value__special_char_test").collect()

    expected_results = [
        ("1", "test@example.com", True),  # Matches the regex
        ("2", "special@char$.com", False),  # Special character $, does not match regex
        ("3", None, True),  # Null value, should be True due to nullable=True
        ("4", "test@domain.net", True),  # Matches the regex
    ]

    assert results == expected_results
