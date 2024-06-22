from functools import wraps
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import DataType

from testframework.dataquality._base import Test


def account_for_nullable(
    func: Callable[[Test, DataFrame, str, bool], Column],
) -> Column:
    """
    A decorator for test methods that adjusts the return value based on the column's nullability

    This decorator wraps a test method to handle nullable columns appropriately. If a column
    contains null values, those values will be converted to True or False, depending on the `nullable` parameter
    passed to the test method. The decorator ensures that the final result is a boolean column,
    with True for values matching the test criteria, False for non-matching values.
    """

    @wraps(func)
    def wrapper(test: Test, df: DataFrame, column: str, nullable: bool) -> Column:
        # Call the original test implementation
        test_result = func(test, df, column, nullable)

        # Adjust the result based on the nullable flag and null values
        if nullable:
            return (
                F.when(F.col(column).isNull(), F.lit(True))
                .otherwise(test_result)
                .cast("boolean")
            )
        else:
            return (
                F.when(F.col(column).isNull(), F.lit(False))
                .otherwise(test_result)
                .cast("boolean")
            )

    return wrapper


def allowed_col_types(
    expected_types: list[DataType],
) -> Callable[
    [Callable[[Test, DataFrame, str, bool], DataFrame]],
    Callable[[Test, DataFrame, str, bool], DataFrame],
]:
    expected_types_tuple = tuple(
        expected_types
    )  # Convert list to tuple once outside the wrapper

    def decorator(
        func: Callable[[Test, DataFrame, str, bool], DataFrame],
    ) -> Callable[[Test, DataFrame, str, bool], DataFrame]:
        @wraps(func)
        def wrapper(
            test: Test, df: DataFrame, column: str, nullable: bool
        ) -> DataFrame:
            col_type = df.schema[column].dataType
            if not isinstance(col_type, expected_types_tuple):
                raise TypeError(
                    f"Column '{column}' is not of expected types {expected_types}. Found type: {col_type}"
                )
            return func(test, df, column, nullable)

        return wrapper

    return decorator
