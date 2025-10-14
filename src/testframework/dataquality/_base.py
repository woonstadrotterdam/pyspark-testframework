from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


class DataQualityTest(ABC):
    def __init__(self, name: str, description: Optional[str] = None):
        self.name = name
        self.description = description

    @abstractmethod
    def test(
        self,
        df: DataFrame,
        col: str,
        primary_key: str,
        nullable: bool,
        test_name: Optional[str] = None,
    ) -> DataFrame:
        pass

    @abstractmethod
    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def __repr__(self) -> str:
        pass


class Test(DataQualityTest):
    """
    Abstract base class for implementing various tests on DataFrame columns.
    """

    @abstractmethod
    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
        """
        Abstract method for implementing the test logic on a specified column.

        Args:
            df (DataFrame): The DataFrame containing the column to test.
            col (str): The name of the column to test.
            nullable (bool): Flag indicating whether the column can contain null values.

        Returns:
            Column: The result of the test as a Spark SQL Column.
        """
        pass

    def test(
        self,
        df: DataFrame,
        col: str,
        primary_key: str,
        nullable: bool,
        test_name: Optional[str] = None,
    ) -> DataFrame:
        """
        Applies the test to the specified column of the DataFrame and returns results in long-format.

        Args:
            df (DataFrame): The DataFrame to test.
            col (str): The name of the column to test.
            primary_key (str): The column name of the primary key.
            nullable (bool): Flag indicating whether the column is allowed to have Null values.
            test_name (Optional[str]): Optional explicit test name. Used both as the internal
                temporary result column name and as the emitted test_name in long-format. Defaults to "{col}__{self.name}".

        Returns:
            DataFrame: A DataFrame with the test results in long-format.
        """
        test_function = self._test_impl(df, col, nullable)
        result_col_name = test_name if test_name else f"{col}__{self.name}"

        # Apply the test result to the DataFrame
        test_result_df = df.withColumn(result_col_name, test_function)

        # Convert to long-format
        pk_expr = F.col(primary_key)

        select_exprs = [
            pk_expr.alias("primary_key"),
            F.lit(result_col_name).alias("test_name"),
            F.lit(col).alias("test_col"),
            F.col(col).cast(StringType()).alias("test_value"),
            F.col(result_col_name).alias("test_result"),
            F.lit(self.description if self.description else self.name).alias(
                "test_description"
            ),
        ]

        return test_result_df.select(*select_exprs)
