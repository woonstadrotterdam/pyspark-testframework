from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from testframework.dataquality._base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class ValidEmailAddress(Test):
    def __init__(self, *, name: str = "ValidEmailAddress") -> None:
        super().__init__(name=name)
        # Basic regex for email validation, can be adjusted for more complex needs
        self.email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$"

    @account_for_nullable
    @allowed_col_types([StringType])
    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
        return F.col(col).rlike(self.email_regex)

    def __str__(self) -> str:
        return f"{self.name}()"

    def __repr__(self) -> str:
        return f"{self.name}()"