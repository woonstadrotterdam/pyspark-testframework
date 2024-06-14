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
        business_key: Union[str, list[str]],
        nullable: bool,
    ) -> DataFrame:
        pass

    @abstractmethod
    def _test_impl(self, df: DataFrame, col: str, nullable: bool) -> Column:
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

    def generate_test_col_name(self, col: str) -> str:
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
        business_key: Union[str, list[str]],
        nullable: bool,
    ) -> DataFrame:
        """
        Applies the test to the specified column of the DataFrame.

        Args:
            df (DataFrame): The DataFrame to test.
            col (str): The name of the column to test.
            business_key (Union[str, list[str]]): The name(s) of the business key(s).
            nullable (bool): Flag indicating whether the column can contain null values.

        Returns:
            DataFrame: A DataFrame with the test results for the specified column.
        """
        business_key = [business_key] if isinstance(business_key, str) else business_key
        test_function = self._test_impl(df, col, nullable)
        test_name = self.generate_test_col_name(col)

        # Apply the test result to the DataFrame
        return df.withColumn(test_name, test_function).select(
            business_key + [col, test_name]
        )
