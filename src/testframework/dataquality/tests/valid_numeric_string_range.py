from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from testframework.dataquality._base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class ValidNumericStringRange(Test):
    def __init__(
        self,
        *,
        name: str = "ValidNumericStringRange",
        min_value: Union[float, str, None] = None,
        max_value: Union[float, str, None] = None,
        floats_allowed: bool,
    ) -> None:
        super().__init__(name=name)
        if min_value is None and max_value is None:
            raise ValueError(
                "At least one of 'min_value' or 'max_value' must be provided"
            )
        self.min_value = float("-inf") if min_value is None else float(min_value)
        self.max_value = float("inf") if max_value is None else float(max_value)
        self.floats_allowed = floats_allowed

    @account_for_nullable
    @allowed_col_types([StringType])
    def _test_impl(self, df: DataFrame, column: str, nullable: bool) -> Column:
        if self.floats_allowed:
            return (F.col(column).cast("double") >= self.min_value) & (
                F.col(column).cast("double") <= self.max_value
            )
        if not self.floats_allowed:
            return (
                (
                    F.col(column).cast("double") == F.col(column).cast("int")
                )  # Check if the value is an integer
                & (F.col(column) >= self.min_value)
                & (F.col(column) <= self.max_value)
            )

    def __str__(self) -> str:
        return f"{self.name}(min_value={self.min_value}, max_value={self.max_value})"

    def __repr__(self) -> str:
        return f"{self.name}(min_value={self.min_value}, max_value={self.max_value})"
