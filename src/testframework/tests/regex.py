from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from testframework.base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class RegexTest(Test):
    """
    Test to check if a column's values match a regular expression pattern.
    """

    def __init__(self, *, name: str, pattern: str) -> None:
        super().__init__(name=name)
        self.pattern = pattern

    @account_for_nullable
    @allowed_col_types([StringType])
    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
        return F.regexp_extract(F.col(col), self.pattern, 0) != ""

    def __str__(self) -> str:
        return f"{self.name}(pattern={self.pattern})"

    def __repr__(self) -> str:
        return f"{self.name}(pattern={self.pattern})"


class IntegerString(RegexTest):
    """
    Returns True when a string can be converted to an integer without losing information.
    01 -> False
    1 -> True
    -1 -> True
    1.1 -> False
    1.0 -> True
    """

    def __init__(self, *, name: str = "IntegerString") -> None:
        super().__init__(name=name, pattern=r"^-?(0|[1-9]\d*)(\.0+)?$")
