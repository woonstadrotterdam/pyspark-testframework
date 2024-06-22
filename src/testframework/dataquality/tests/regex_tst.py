# this file is called regex_tst instead of regex_test due to pytest confusion otherwise

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from testframework.dataquality._base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class RegexTest(Test):
    """
    Test to check if a column's values matches a regular expression pattern.
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
