from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from testframework.dataquality._base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class ValidStringLength(Test):
    def __init__(
        self,
        *,
        name: str = "ValidStringLength",
        min_value: Union[float, str, None] = None,
        max_value: Union[float, str, None] = None,
    ) -> None:
        super().__init__(name=name)
        if min_value is None and max_value is None:
            raise ValueError("At least one of 'min' or 'max' must be provided")
        self.min_value = 0 if min_value is None else float(min_value)
        self.max_value = float("inf") if max_value is None else float(max_value)

    @account_for_nullable
    @allowed_col_types([StringType])
    def _test_impl(self, df: DataFrame, column: str, nullable: bool) -> Column:
        return (F.length(F.col(column)) >= F.lit(self.min_value)) & (
            F.length(F.col(column)) <= F.lit(self.max_value)
        )

    def __str__(self) -> str:
        return f"{self.name}(min_value={self.min_value}, max_value={self.max_value})"

    def __repr__(self) -> str:
        return f"{self.name}(min_value={self.min_value}, max_value={self.max_value})"
