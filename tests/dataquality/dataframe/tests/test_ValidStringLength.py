import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from testframework.dataquality.tests import ValidStringLength


@pytest.fixture
def mock_df(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("text", StringType(), True),
        ]
    )
    data = [
        ("1", "short"),  # 5 chars
        ("2", "medium text"),  # 11 chars
        ("3", "this is a longer text"),  # 21 chars
        ("4", ""),  # 0 chars
        ("5", None),  # null value
        ("6", "x" * 100),  # 100 chars
    ]
    return spark.createDataFrame(data, schema)


@pytest.mark.parametrize("nullable", [True, False])
@pytest.mark.parametrize(
    "min_value, max_value, expected_valid_count",
    [
        (None, 10, 2),  # short, medium text, empty string
        (0, None, 5),  # all non-null values (all lengths >= 0)
        (5, 20, 2),  # short, medium text (longer text is 19 chars)
        (10, 50, 2),  # medium text, longer text
        (0, 5, 2),  # empty string, short
        (100, 200, 1),  # only the 100-char string
    ],
)
def test_ValidStringLength(
    mock_df,
    min_value,
    max_value,
    expected_valid_count,
    nullable,
):
    test_instance = ValidStringLength(
        min_value=min_value,
        max_value=max_value,
    )
    result_df = test_instance.test(mock_df, "text", "id", nullable=nullable)

    col = test_instance.generate_result_col_name("text")

    # Count the number of rows considered valid
    valid_rows = result_df.filter(F.col(col)).count()
    # if nullable, the None value should be considered valid, thus add 1 to expected count
    expected_valid_count += int(nullable)
    assert (
        valid_rows == expected_valid_count
    ), f"Expected {expected_valid_count} valid rows, but got {valid_rows}"


def test_ValidStringLength_error():
    with pytest.raises(ValueError):
        ValidStringLength()  # both min_value and max_value not defined
