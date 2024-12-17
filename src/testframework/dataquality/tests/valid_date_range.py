from datetime import date, datetime
from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, TimestampType

from testframework.dataquality._base import Test
from testframework.utils.decorators import account_for_nullable, allowed_col_types


class ValidDateRange(Test):
    def __init__(
        self,
        *,
        name: str = "ValidDateRange",
        min_date: Union[str, datetime, date, None] = None,
        max_date: Union[str, datetime, date, None] = None,
        date_format: str = "yyyy-MM-dd",
    ) -> None:
        super().__init__(name=name)
        if min_date is None and max_date is None:
            raise ValueError(
                "At least one of 'min_date' or 'max_date' must be provided"
            )

        # Convert string dates to datetime objects
        if isinstance(min_date, str):
            min_date = datetime.strptime(
                min_date,
                date_format.replace("yyyy", "%Y")
                .replace("MM", "%m")
                .replace("dd", "%d"),
            )
        if isinstance(max_date, str):
            max_date = datetime.strptime(
                max_date,
                date_format.replace("yyyy", "%Y")
                .replace("MM", "%m")
                .replace("dd", "%d"),
            )

        self.min_date = min_date
        self.max_date = max_date
        self.date_format = date_format

    @account_for_nullable
    @allowed_col_types([DateType, TimestampType, StringType])
    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
        # Convert string column to date if needed
        col_type = df.schema[col].dataType
        date_col = (
            F.to_date(F.col(col), self.date_format)
            if isinstance(col_type, StringType)
            else F.col(col)
        )

        # Build and combine conditions
        conditions = []
        if self.min_date is not None:
            conditions.append(date_col >= F.lit(self.min_date))
        if self.max_date is not None:
            conditions.append(date_col <= F.lit(self.max_date))

        return conditions[0] if len(conditions) == 1 else conditions[0] & conditions[1]

    def __str__(self) -> str:
        return f"{self.name}(min_date={self.min_date}, max_date={self.max_date})"

    def __repr__(self) -> str:
        return f"{self.name}(min_date={self.min_date}, max_date={self.max_date})"
