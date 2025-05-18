import pytest
from pyspark.sql import types as T
from testframework.dataquality.tests import ValidEmailAddress

test_data = [
    ("test@example.com", True),  # Standard valid email
    ("user.name+tag@example.co.uk", True),  # Valid with special chars and subdomain
    ("invalid-email", False),  # Missing @ and domain
    ("@missing-local-part.com", False),  # Missing local part
    ("no-at-sign.com", False),  # Missing @
    ("no-domain@", False),  # Missing domain
    ("spaces in@email.com", False),  # Contains spaces
    ("double@@at.com", False),  # Double @
    ("test@example", False),  # Missing TLD
    ("test.email@example.com", True),  # Valid with dot in local part
    ("test@sub.example.com", True),  # Valid with subdomain
    ("", False),  # Empty string
    (None, False),  # Null value
    ("user+tag@domain.com", True),  # Valid with plus sign
    ("very.common@example.com", True),  # Valid with dots
    ("disposable.style.email.with+symbol@example.com", True),  # Complex valid email
    ("other.email-with-hyphen@example.com", True),  # Valid with hyphen
    ("fully-qualified-domain@example.com", True),  # Valid with hyphen in local part
    ("example@s.example", True),  # Invalid - TLD too short
    ("example@example..com", False),  # Invalid - consecutive dots
]


@pytest.mark.parametrize("value, expected", test_data)
def test_ValidEmailAddress(spark, value, expected):
    schema = T.StructType(
        [
            T.StructField("value", T.StringType(), nullable=True),
        ]
    )

    df = spark.createDataFrame([(value,)], schema)
    email_validator = ValidEmailAddress()
    result_df = df.withColumn(
        "result", email_validator._test_impl(df, "value", nullable=False)
    )
    result = result_df.collect()[0]["result"]
    assert (
        result == expected
    ), f"Failed for value: {value}, Expected: {expected}, Got: {result}"
