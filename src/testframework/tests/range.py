from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
)

from testframework.base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class ValidIntegerRange(Test):
    def __init__(
        self,
        name: str = "ValidIntegerRange",
        min_value: Union[int, str, None] = None,
        max_value: Union[int, str, None] = None,
    ) -> None:
        super().__init__(name=name)
        if min_value is None and max_value is None:
            raise ValueError(
                "At least one of 'min_value' or 'max_value' must be provided"
            )
        self.min_value = float("-inf") if min_value is None else int(min_value)
        self.max_value = float("inf") if max_value is None else int(max_value)

    @account_for_nullable
    @allowed_col_types([IntegerType, LongType, ShortType, ByteType])
    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
        return (F.col(col) >= self.min_value) & (F.col(col) <= self.max_value)
