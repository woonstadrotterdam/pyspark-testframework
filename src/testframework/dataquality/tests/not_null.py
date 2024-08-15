from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from testframework.dataquality._base import Test


class NotNull(Test):
    """
    Test to check if a column's values are not null.
    """

    def __init__(self, *, name: str = "NotNull") -> None:
        super().__init__(name=name)

    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
        if nullable is True:
            raise ValueError(
                "Nullable is True doesn't make sense for NotNull test. Please use nullable=False"
            )

        return F.col(col).isNotNull()

    def __str__(self) -> str:
        return f"{self.name}"

    def __repr__(self) -> str:
        return f"{self.name}"
