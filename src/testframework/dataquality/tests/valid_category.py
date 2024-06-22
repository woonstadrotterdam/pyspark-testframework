from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from testframework.dataquality._base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class ValidCategory(Test):
    """
    Returns True if the value is one of the specified categorical values.
    """

    def __init__(self, *, categories: set[str], name: str = "ValidCategory"):
        super().__init__(name=name)
        if isinstance(categories, list):
            categories = set(categories)
        if not isinstance(categories, set):
            raise TypeError(f"Categories must be a set of strings. {categories = }")
        self.categories = categories

    @account_for_nullable
    @allowed_col_types([StringType])
    def _test_impl(self, df: DataFrame, column: str, nullable: bool) -> Column:
        if nullable is False and None in self.categories:
            raise ValueError(
                "Nullable is False doesn't make sense if correct_values contains None, please use nullable=True"
            )
        return F.col(column).isin(self.categories)

    def __str__(self) -> str:
        return f"{self.name}(categories={self.categories})"

    def __repr__(self) -> str:
        return f"{self.name}(categories={self.categories})"
