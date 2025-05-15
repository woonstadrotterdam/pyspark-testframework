import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from testframework.dataquality.tests.valid_email_address import ValidEmailAddress


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.appName("pytest_valid_email_address").getOrCreate()


def test_valid_email_address(spark: SparkSession) -> None:
    test = ValidEmailAddress()
    schema = StructType([StructField("email", StringType(), True)])
    data = [
        ("test@example.com",),
        ("invalid-email",),
        ("another.test@sub.example.co.uk",),
        (None,),
        ("test@example",),
        ("test@.com",),
        ("@example.com",),
        ("test@example..com",),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result_df = df.withColumn("is_valid_email", test.run(df, "email"))

    expected_data = [
        ("test@example.com", True),
        ("invalid-email", False),
        ("another.test@sub.example.co.uk", True),
        (None, None),  # Null input should result in null output due to @account_for_nullable
        ("test@example", False),
        ("test@.com", False),
        ("@example.com", False),
        ("test@example..com", False),
    ]
    expected_df = spark.createDataFrame(
        expected_data, schema=StructType(df.schema.fields + [StructField("is_valid_email", StringType(), True)]) #type: ignore
    ).withColumn("is_valid_email", F.col("is_valid_email").cast("boolean"))


    assert result_df.collect() == expected_df.collect()


def test_valid_email_address_custom_name(spark: SparkSession) -> None:
    test = ValidEmailAddress(name="CustomEmailCheck")
    assert str(test) == "CustomEmailCheck()"
    assert repr(test) == "CustomEmailCheck()"


def test_valid_email_address_nullable_false(spark: SparkSession) -> None:
    test = ValidEmailAddress()
    schema = StructType([StructField("email", StringType(), False)]) # Nullable set to False
    data = [
        ("test@example.com",),
        ("invalid-email",),
        ("another.test@sub.example.co.uk",),
        # (None,), # This would raise an error if DataFrame is created with non-nullable schema and null data
        ("test@example",),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result_df = df.withColumn("is_valid_email", test.run(df, "email", nullable=False))

    expected_data = [
        ("test@example.com", True),
        ("invalid-email", False),
        ("another.test@sub.example.co.uk", True),
        ("test@example", False),
    ]
    expected_df = spark.createDataFrame(
        expected_data, schema=StructType(df.schema.fields + [StructField("is_valid_email", StringType(), True)]) # type: ignore
    ).withColumn("is_valid_email", F.col("is_valid_email").cast("boolean"))

    assert result_df.collect() == expected_df.collect() 