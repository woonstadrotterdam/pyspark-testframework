from abc import ABC, abstractmethod
from typing import Optional, Union

from pyspark.sql import Column, DataFrame


class DataQualityTest(ABC):
    def __init__(self, name: str, description: Optional[str] = None):
        self.name = name
        self.description = description

    @abstractmethod
    def test(
        self,
        df: DataFrame,
        col: str,
        primary_key: Union[str, list[str]],
        nullable: bool,
        result_col: Optional[str] = None,
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

    def generate_result_col_name(self, col: str) -> str:
        """
        Generates a standardized name for the test result column.

        Args:
            col (str): The name of the column being tested.

        Returns:
            str: The name of the test result column.
        """
        return f"{col}__{self.name}"

    def test(
        self,
        df: DataFrame,
        col: str,
        primary_key: Union[str, list[str]],
        nullable: bool,
        result_col: Optional[str] = None,
    ) -> DataFrame:
        """
        Applies the test to the specified column of the DataFrame.

        Args:
            df (DataFrame): The DataFrame to test.
            col (str): The name of the column to test.
            primary_key (Union[str, list[str]]): The column name(s) of the primary key(s).
            nullable (bool): Flag indicating whether the column is allowed to have Null values.
            result_col (Optional[str]): The name of the column to store the test result. By default None. If None, a default name will be generated.

        Returns:
            DataFrame: A DataFrame with the test results for the specified column.
        """
        primary_key = [primary_key] if isinstance(primary_key, str) else primary_key
        test_function = self._test_impl(df, col, nullable)
        result_col = result_col if result_col else self.generate_result_col_name(col)

        # Apply the test result to the DataFrame
        return df.withColumn(result_col, test_function).select(
            primary_key + [col, result_col]
        )
