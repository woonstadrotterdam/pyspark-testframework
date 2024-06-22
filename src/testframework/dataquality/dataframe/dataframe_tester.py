import logging
from typing import Any, Optional, Union

from pyspark.sql import Column, DataFrame, SparkSession

from testframework.dataquality._base import Test

logger = logging.getLogger(__name__)


class DataFrameTester:
    """
    DataFrameTester class which helps with testing a single DataFrame.
    Add configurable tests from testframework.dataquality.tests using .test()
    Add custom tests using .add_custom_test_result()

    Args:
        df (DataFrame): The pyspark DataFrame to test.
        primary_key (Union[str, list[str]]): The name of the column(s) used as a primary key. The column should contain only unique values. Rows of which all primary keys are null are deleted.
        spark (SparkSession): The SparkSession to use for the tests.
    """

    def __init__(
        self,
        df: DataFrame,
        primary_key: Union[str, list[str]],
        spark: SparkSession,
    ) -> None:
        self.primary_key = (
            [primary_key] if isinstance(primary_key, str) else primary_key
        )
        self.df = self._check_primary_key(df)
        self.results: DataFrame = self.df.select(self.primary_key)
        self.spark = spark
        self.descriptions: dict[str, str] = {}

    @classmethod
    def unique_columns(cls, df: DataFrame) -> DataFrame:
        """
        Loads input DataFrame with only columns for possible primary keys.

        Args:
            df (DataFrame): The input DataFrame to test.

        Returns:
            DataFrame: The loaded Spark DataFrame.
        """
        return df.select(cls.potential_primary_keys(df))

    def _check_primary_key(self, df: DataFrame) -> DataFrame:
        """
        Extracts the input DataFrame from the Analytics class

        Args:
            df (DataFrame): The input DataFrame to test.

        Returns:
            DataFrame: The loaded Spark DataFrame.

        Raises:
            KeyError: If the primary key column is not an existing column in the DataFrame.
            ValueError: If the primary key column does not contain unique values.
        """
        for key in self.primary_key:
            if key not in df.columns:
                unique_primary_keys = self.potential_primary_keys(df)
                raise KeyError(
                    f"Primary key column '{key}' is not an existing column in this DataFrame.",
                    f"Unique primary keys in this DataFrame: {unique_primary_keys}. You can also use multiple columns as composite primary key",
                )

        df_filtered = df.dropna(how="all", subset=self.primary_key)
        null_count = df.count() - df_filtered.count()
        if null_count > 0:
            logger.warning(
                f"Primary key '{self.primary_key}' contains null values, {null_count} rows are excluded."
            )

        self.assert_primary_key_unique(df_filtered, self.primary_key)
        return df_filtered

    @classmethod
    def assert_primary_key_unique(
        cls, df: DataFrame, primary_key: Union[str, list[str]]
    ) -> None:
        """
        Checks if the specified primary key column contains unique values.

        Args:
            df (DataFrame): The DataFrame to check.
            primary_key (Union[str, list[str]]): The primary key column name.

        Raises:
            ValueError: If the primary key column does not contain unique values.
        """
        total_count = df.count()
        distinct_count = df.select(primary_key).distinct().count()
        if total_count != distinct_count:
            logger.error(
                f"Primary key(s) {primary_key} is not unique ({total_count = }, {distinct_count = }). Determining potential primary keys.."
            )
            unique_primary_keys = cls.potential_primary_keys(
                df, total_count=total_count
            )
            raise ValueError(
                f"Primary key '{primary_key}' is not unique",
                f"Unique primary keys in this DataFrame: {unique_primary_keys}. You can also use multiple columns as composite primary key",
            )

    @staticmethod
    def potential_primary_keys(
        df: DataFrame, total_count: Optional[int] = None
    ) -> list[str]:
        if total_count is None:
            total_count = df.count()
        return [
            col_name
            for col_name in df.columns
            if df.select(col_name).distinct().count() == total_count
        ]

    def test(
        self,
        col: str,
        test: Test,
        nullable: bool,
        description: Optional[str] = None,
        filter_rows: Optional[Column] = None,
        return_extra_cols: Optional[list[str]] = None,
        dummy_run: bool = False,
    ) -> DataFrame:
        """
        Executes a specific test on a given column of the DataFrame.

        Args:
            col (str): The column to test.
            test (Test): The test to apply.
            nullable (bool): Indicates if the column to test is allowed to contain null-values.
            description (Optional[str]): Description of the test for reporting purposes.
            filter_rows (Optional[Column]): Uses df.filter(filter_rows) to filter rows for which the test doesn't apply.
            return_extra_cols (Optional[list[str]]): Return extra columns from the original dataframe. Defaults to None.
            dummy_run (bool): If True, perform a dummy run without saving results. Defaults to False.

        Returns:
            DataFrame: The test results as a DataFrame.

        Raises:
            TypeError: If the test is not an instance of Test or its subclass.
            TypeError: If filter_rows is not a pyspark Column or None.
            TypeError: If description is not of type string or None.
        """
        if not isinstance(test, Test):
            raise TypeError("test must be an instance of Test or its subclass")

        if not isinstance(filter_rows, (Column, type(None))):
            raise TypeError(
                f"filter_rows must be a pyspark Column or None, but is: {type(filter_rows)}"
            )

        if not isinstance(description, (str, type(None))):
            raise TypeError(
                f"description must be of type string or None but is {type(description)}"
            )

        # Apply the filter if provided
        filtered_df = (
            self.df.filter(filter_rows) if filter_rows is not None else self.df
        )

        test_result = test.test(filtered_df, col, self.primary_key, nullable)

        if not dummy_run:
            test_name = test.generate_result_col_name(col)

            self.descriptions[test_name] = (
                description if description else f"{col}__{test}"
            )

            new_test_results = self.results.drop(test_name)
            new_test_results = new_test_results.join(
                test_result.select(self.primary_key + [test_name]),
                on=self.primary_key,
                how="left",
            )
            self.results = new_test_results

        if return_extra_cols:
            return test_result.join(
                self.df.select(*self.primary_key, *return_extra_cols),
                on=self.primary_key,
                how="left",
            )

        return test_result

    @property
    def description_df(self) -> DataFrame:
        """
        Creates a DataFrame from the descriptions dictionary.

        Returns:
            DataFrame: A DataFrame containing the test names and their descriptions.
        """
        return self.spark.createDataFrame(
            [(k, v) for k, v in self.descriptions.items()], ["test", "description"]
        )

    def add_custom_test_result(
        self,
        result: DataFrame,
        name: str,
        description: Optional[str] = None,
        fillna_value: Optional[Any] = None,
        return_extra_cols: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Adds custom test results to the test DataFrame.

        Args:
            result (DataFrame): The DataFrame containing the test results.
            name (str): The name of the custom test, which should be a column in test_result.
            description (Optional[str]): Description of the test for reporting purposes.
            fillna_value (Optional[Any]): The value to fill nulls in the test result column after left joining on the primary_key. Defaults to None.
            return_extra_cols (Optional[list[str]]): Return extra columns from the original dataframe. Defaults to None.

        Returns:
            DataFrame: The updated test DataFrame with the custom test results.

        Raises:
            TypeError: If result is not a pyspark DataFrame.
            ValueError: If the primary key is not found in the result DataFrame or is not unique.
            TypeError: If description is not of type string or None.
        """

        if not isinstance(result, DataFrame):
            raise TypeError(
                f"test_result should be a pyspark DataFrame, but it's a {type(result)}"
            )

        for key in self.primary_key:
            if key not in result.columns:
                raise ValueError(f"primary_key '{key}' not found in DataFrame")

        if name not in result.columns:
            raise ValueError(
                f"A column with test_name '{name}' not found in test_result DataFrame"
            )

        if result.select(self.primary_key).distinct().count() != result.count():
            raise ValueError(
                f"primary_key ('{self.primary_key}') is not unique in test_result DataFrame"
            )

        if not isinstance(description, (str, type(None))):
            raise TypeError("test_description must be of type string")

        self.descriptions[name] = description if description else name

        new_test_results = self.results.drop(name)
        new_test_results = new_test_results.join(
            result.select(self.primary_key + [name]),
            on=self.primary_key,
            how="left",
        )

        if fillna_value is not None:
            new_test_results = new_test_results.fillna({name: fillna_value})

        self.results = new_test_results

        if return_extra_cols:
            return self.results.select(*self.primary_key, name).join(
                self.df.select(*self.primary_key, *return_extra_cols),
                on=self.primary_key,
                how="left",
            )

        return self.results.select(*self.primary_key, name)
