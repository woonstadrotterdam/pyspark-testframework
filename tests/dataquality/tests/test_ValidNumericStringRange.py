import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)
from testframework.dataquality.tests import ValidNumericStringRange


@pytest.fixture
def mock_df(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", FloatType(), True),  # Column to be tested
            StructField("value_str", StringType(), True),
        ]
    )
    data = [
        ("1", 10.5, "10.5"),
        ("2", 100.0, "100.0"),
        ("3", -20.0, "-20.0"),
        ("4", 0.0, "0.0"),
        ("5", 50.0, "50.0"),
        ("6", None, None),
        ("7", None, "unexpected_type"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.mark.parametrize("nullable", [True, False])
@pytest.mark.parametrize(
    "min_value, max_value, floats_allowed, expected_valid_count",
    [
        (None, 100, True, 5),
        (None, 100, False, 4),
        (0, None, True, 4),
        (0, None, False, 3),
        (-50, 50, True, 4),
        (-50, 50, False, 3),
    ],
)
def test_ValidNumericStringRange(
    mock_df,
    min_value,
    max_value,
    floats_allowed,
    expected_valid_count,
    nullable,
):
    test_instance = ValidNumericStringRange(
        min_value=min_value, max_value=max_value, floats_allowed=floats_allowed
    )
    # Assuming the ValidNumericStringRange test method returns a DataFrame indicating whether each row is valid
    result_df = test_instance.test(mock_df, "value_str", "id", nullable=nullable)

    col = test_instance.generate_result_col_name("value_str")

    # Count the number of rows considered valid
    valid_rows = result_df.filter(F.col(col)).count()
    # if nullable, the None value should be considered valid, thus add the amount of Null rows to the expected rows
    expected_valid_count += (
        int(nullable) * mock_df.filter(F.col("value_str").isNull()).count()
    )
    assert (
        valid_rows == expected_valid_count
    ), f"Expected {expected_valid_count} valid rows, but got {valid_rows}"


def test_ValidNumericStringRange_error():
    with pytest.raises(ValueError):
        ValidNumericStringRange(floats_allowed=False)
