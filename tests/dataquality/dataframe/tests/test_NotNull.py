import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from testframework.dataquality.tests import (
    NotNull,
)


@pytest.fixture
def sample_df(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
        ]
    )
    data = [
        Row(id=1, name="Alice"),
        Row(id=2, name="Bob"),
        Row(id=3, name=None),
        Row(id=4, name="Charlie"),
    ]
    return spark.createDataFrame(data, schema)


def test_notnull(spark, sample_df):
    test = NotNull()
    result_df = test.test(sample_df, "name", primary_key="id", nullable=False)

    expected_results = [
        True,
        True,
        False,  # None value should return False
        True,
    ]

    assert result_df.count() == 4
    result = [row.name__NotNull for row in result_df.collect()]
    assert result == expected_results


def test_notnull_nullable_true_raises_error(spark, sample_df):
    test = NotNull()
    with pytest.raises(
        ValueError,
        match="Nullable is True doesn't make sense for NotNull test. Please use nullable=False",
    ):
        test.test(sample_df, "name", primary_key="id", nullable=True)
