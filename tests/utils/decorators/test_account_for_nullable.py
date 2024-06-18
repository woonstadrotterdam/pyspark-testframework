from pyspark.sql.types import BooleanType, StringType, StructField, StructType
from testframework.dataquality.tests.regex_tst import RegexTest


def test_regex_test_method_nullable_false(spark):
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
    df = spark.createDataFrame(data, schema)

    regex_test = RegexTest(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=False)
    results = result_df.select("id", "value", "value__email_test").collect()

    expected_results = [
        ("1", "test@example.com", True),  # Matches the regex
        ("2", "invalid-email", False),  # Does not match the regex
        ("3", None, False),  # Null value, should be False due to nullable=False
        ("4", "another@test.com", True),  # Matches the regex
    ]

    assert [
        (row.id, row.value, row.value__email_test) for row in results
    ] == expected_results


def test_regex_test_method_without_nulls_nullable_true(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "test@example.com"),
        ("2", "invalid-email"),
        ("3", "no-email"),
        ("4", "another@test.com"),
    ]
    df = spark.createDataFrame(data, schema)

    regex_test = RegexTest(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=True)
    results = result_df.select("id", "value", "value__email_test").collect()

    expected_results = [
        ("1", "test@example.com", True),  # Matches the regex
        ("2", "invalid-email", False),  # Does not match the regex
        ("3", "no-email", False),  # Does not match the regex
        ("4", "another@test.com", True),  # Matches the regex
    ]

    assert [
        (row.id, row.value, row.value__email_test) for row in results
    ] == expected_results


def test_regex_test_method_without_nulls_nullable_false(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "test@example.com"),
        ("2", "invalid-email"),
        ("3", "no-email"),
        ("4", "another@test.com"),
    ]
    df = spark.createDataFrame(data, schema)

    regex_test = RegexTest(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=False)
    results = result_df.select("id", "value", "value__email_test").collect()

    expected_results = [
        ("1", "test@example.com", True),  # Matches the regex
        ("2", "invalid-email", False),  # Does not match the regex
        ("3", "no-email", False),  # Does not match the regex
        ("4", "another@test.com", True),  # Matches the regex
    ]

    assert [
        (row.id, row.value, row.value__email_test) for row in results
    ] == expected_results


def test_regex_test_method_all_nulls_nullable_true(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [("1", None), ("2", None)]
    df = spark.createDataFrame(data, schema)

    regex_test = RegexTest(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=True)
    results = result_df.select("id", "value", "value__email_test").collect()

    expected_results = [
        ("1", None, True),  # Null value, should be True due to nullable=True
        ("2", None, True),  # Null value, should be True due to nullable=True
    ]

    assert [
        (row.id, row.value, row.value__email_test) for row in results
    ] == expected_results


def test_regex_test_method_all_nulls_nullable_false(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [("1", None), ("2", None)]
    df = spark.createDataFrame(data, schema)

    regex_test = RegexTest(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=False)
    results = result_df.select("id", "value", "value__email_test").collect()

    expected_results = [
        ("1", None, False),  # Null value, should be False due to nullable=False
        ("2", None, False),  # Null value, should be False due to nullable=False
    ]

    assert [
        (row.id, row.value, row.value__email_test) for row in results
    ] == expected_results


def test_regex_test_method_no_nulls_and_boolean_output(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "test@example.com"),
        ("2", "invalid-email"),
        ("3", "no-email"),
        ("4", "another@test.com"),
    ]
    df = spark.createDataFrame(data, schema)

    regex_test = RegexTest(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=True)
    assert result_df.schema["value__email_test"].dataType == BooleanType()


def test_regex_test_method_mixed_data(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [
        ("1", "test@example.com"),
        ("2", "invalid-email"),
        ("3", "no-email"),
        ("4", None),
        ("5", "another@test.com"),
    ]
    df = spark.createDataFrame(data, schema)

    regex_test = RegexTest(name="email_test", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    result_df = regex_test.test(df, col="value", primary_key="id", nullable=True)
    results = result_df.select("id", "value", "value__email_test").collect()

    expected_results = [
        ("1", "test@example.com", True),  # Matches the regex
        ("2", "invalid-email", False),  # Does not match the regex
        ("3", "no-email", False),  # Does not match the regex
        ("4", None, True),  # Null value, should be True due to nullable=True
        ("5", "another@test.com", True),  # Matches the regex
    ]

    assert [
        (row.id, row.value, row.value__email_test) for row in results
    ] == expected_results
