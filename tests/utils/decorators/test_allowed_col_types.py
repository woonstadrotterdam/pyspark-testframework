import pytest
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from testframework.dataquality._base import (
    Test as Tst,  # to prevent pytest from collecting this class
)
from testframework.utils.decorators import allowed_col_types


def test_allowed_col_types_with_valid_column_type(spark):
    class ColumnTypeTest(Tst):
        def __init__(self, name: str) -> None:
            super().__init__(name=name)

        @allowed_col_types([StringType])
        def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
            return lit(True)  # Dummy implementation

        def __str__(self) -> str:
            return f"{self.name}"

        def __repr__(self) -> str:
            return f"{self.name}"

    schema = StructType([StructField("test_column", StringType(), True)])
    data = [("abc",), ("def",), (None,)]
    df = spark.createDataFrame(data, schema)

    test = ColumnTypeTest(name="test")
    result_df = test.test(
        df, col="test_column", primary_key="test_column", nullable=True
    )
    assert result_df is not None  # Check if DataFrame is returned


def test_allowed_col_types_with_invalid_column_type(spark):
    class ColumnTypeTest(Tst):
        def __init__(self, name: str) -> None:
            super().__init__(name=name)

        @allowed_col_types([StringType])
        def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
            return lit(True)  # Dummy implementation

        def __str__(self) -> str:
            return f"{self.name}"

        def __repr__(self) -> str:
            return f"{self.name}"

    schema = StructType([StructField("test_column", IntegerType(), True)])
    data = [(1,), (2,), (None,)]
    df = spark.createDataFrame(data, schema)

    test = ColumnTypeTest(name="test")
    with pytest.raises(TypeError) as excinfo:
        test.test(df, col="test_column", primary_key="test_column", nullable=True)

    assert "Column 'test_column' is not of expected types" in str(excinfo.value)


def test_allowed_col_types_with_multiple_valid_types(spark):
    class MultiTypeTest(Tst):
        def __init__(self, name: str) -> None:
            super().__init__(name=name)

        @allowed_col_types([StringType, IntegerType])
        def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
            return lit(True)  # Dummy implementation

        def __str__(self) -> str:
            return f"{self.name}"

        def __repr__(self) -> str:
            return f"{self.name}"

    schema = StructType([StructField("test_column", IntegerType(), True)])
    data = [(1,), (2,), (None,)]
    df = spark.createDataFrame(data, schema)

    test = MultiTypeTest(name="test")
    result_df = test.test(
        df, col="test_column", primary_key="test_column", nullable=True
    )
    assert result_df is not None  # Check if DataFrame is returned


def test_allowed_col_types_with_string_and_integer_columns(spark):
    class MixedTypeTest(Tst):
        def __init__(self, name: str) -> None:
            super().__init__(name=name)

        @allowed_col_types([StringType, IntegerType])
        def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
            return lit(True)  # Dummy implementation

        def __str__(self) -> str:
            return f"{self.name}"

        def __repr__(self) -> str:
            return f"{self.name}"

    schema = StructType(
        [
            StructField("string_column", StringType(), True),
            StructField("int_column", IntegerType(), True),
        ]
    )
    data = [("abc", 1), ("def", 2), (None, 3)]
    df = spark.createDataFrame(data, schema)

    test = MixedTypeTest(name="test")
    result_df_string = test.test(
        df, col="string_column", primary_key="string_column", nullable=True
    )
    result_df_int = test.test(
        df, col="int_column", primary_key="int_column", nullable=True
    )

    assert result_df_string is not None  # Check if DataFrame is returned
    assert result_df_int is not None  # Check if DataFrame is returned


def test_allowed_col_types_with_no_matching_column_type(spark):
    class NoMatchingTypeTest(Tst):
        def __init__(self, name: str) -> None:
            super().__init__(name=name)

        @allowed_col_types([BooleanType])
        def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
            return lit(True)  # Dummy implementation

        def __str__(self) -> str:
            return f"{self.name}"

        def __repr__(self) -> str:
            return f"{self.name}"

    schema = StructType([StructField("test_column", StringType(), True)])
    data = [("abc",), ("def",), (None,)]
    df = spark.createDataFrame(data, schema)

    test = NoMatchingTypeTest(name="test")
    with pytest.raises(TypeError) as excinfo:
        test.test(df, col="test_column", primary_key="test_column", nullable=True)

    assert "Column 'test_column' is not of expected types" in str(excinfo.value)
