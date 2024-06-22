from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from testframework.dataquality._base import Test
from testframework.utils.decorators import account_for_nullable


class CorrectValue(Test):
    """
    Returns True if a value is the specified (correct) value.
    """

    def __init__(self, *, name: str = "CorrectValue", correct_value: Any):
        super().__init__(name=name)
        self.correct_value = correct_value

    @account_for_nullable
    def _test_impl(self, df: DataFrame, column: str, nullable: bool) -> Column:
        if nullable is False and self.correct_value is None:
            raise ValueError(
                "Nullable is False doesn't make sense if correct_value=None, please use nullable=True"
            )

        if self.correct_value is None:
            return F.col(column).isNull()

        return F.col(column) == self.correct_value

    def __str__(self) -> str:
        return f"{self.name}(correct_value={self.correct_value})"

    def __repr__(self) -> str:
        return f"{self.name}(correct_value={self.correct_value})"
