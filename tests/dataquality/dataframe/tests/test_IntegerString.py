import pytest
from pyspark.sql import types as T
from testframework.dataquality.tests import IntegerString

test_data = [
    ("01", False, "pk1"),  # Leading zero
    ("1", True, "pk2"),  # Positive integer
    ("-1", True, "pk3"),  # Negative integer
    ("1.1", False, "pk4"),  # Decimal number
    ("1.0", True, "pk5"),  # Integer with .0
    ("0", True, "pk6"),  # Zero
    ("-0", True, "pk7"),  # Negative zero
    ("10.00", True, "pk8"),  # Integer with .00
    ("-10.0", True, "pk9"),  # Negative integer with .0
    ("-10.01", False, "pk10"),  # Negative decimal number
    ("abc", False, "pk11"),  # Non-numeric string
    ("", False, "pk12"),  # Empty string
    (None, False, "pk13"),  # Null value
]


@pytest.mark.parametrize("value, expected, primary_key", test_data)
def test_IntegerString(spark, value, expected, primary_key):
    schema = T.StructType(
        [
            T.StructField("primary_key", T.StringType(), nullable=False),
            T.StructField("value", T.StringType(), nullable=True),
        ]
    )

    df = spark.createDataFrame([(primary_key, value)], schema)
    is_integer = IntegerString()
    result_df = df.withColumn(
        "result", is_integer._test_impl(df, "value", nullable=False)
    )
    result = result_df.collect()[0]["result"]
    assert (
        result == expected
    ), f"Failed for value: {value}, Expected: {expected}, Got: {result}"
