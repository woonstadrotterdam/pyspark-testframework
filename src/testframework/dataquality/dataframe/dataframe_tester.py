import logging
from functools import reduce
from typing import Any, Optional, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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
        return_failed_rows: bool = False,
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
            return_failed_rows (bool): If True, return only the rows where the test has failed. Defaults to False.

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

        if return_failed_rows:
            test_result = test_result.filter(F.col(test_name) == F.lit(False))

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
        return_failed_rows: bool = False,
    ) -> DataFrame:
        """
        Adds custom test results to the test DataFrame.

        Args:
            result (DataFrame): The DataFrame containing the test results.
            name (str): The name of the custom test, which should be a column in test_result.
            description (Optional[str]): Description of the test for reporting purposes.
            fillna_value (Optional[Any]): The value to fill nulls in the test result column after left joining on the primary_key. Defaults to None.
            return_extra_cols (Optional[list[str]]): Return extra columns from the original dataframe. Defaults to None.
            return_failed_rows (bool): If True, return only the rows where the test has failed. Defaults to False.

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

        if return_failed_rows:
            new_test_results = new_test_results.filter(F.col(name) == F.lit(False))

        if return_extra_cols:
            return new_test_results.select(*self.primary_key, name).join(
                self.df.select(*self.primary_key, *return_extra_cols),
                on=self.primary_key,
                how="left",
            )

        return new_test_results.select(*self.primary_key, name)

    @property
    def summary(self) -> DataFrame:
        """
        Generate a summary DataFrame that provides insights into the test results stored in the `results` DataFrame.

        The summary includes:
        - The number of tests (`n_tests`) conducted for each column.
        - For Boolean columns:
            - The number of passed tests (`n_passed`).
            - The percentage of passed tests (`percentage_passed`).
            - The number of failed tests (`n_failed`).
            - The percentage of failed tests (`percentage_failed`).
        - Non-Boolean columns are excluded from the pass/fail calculations.

        Returns:
            DataFrame: A Spark DataFrame containing the summary statistics for each test column. The DataFrame has the following schema:
                - `test`: The name of the test/column.
                - `description`: A description of the test/column (if available).
                - `n_tests`: The number of non-null entries for the test/column.
                - `n_passed`: The number of entries that passed the test (only for Boolean columns).
                - `percentage_passed`: The percentage of passed tests (only for Boolean columns).
                - `n_failed`: The number of entries that failed the test (only for Boolean columns).
                - `percentage_failed`: The percentage of failed tests (only for Boolean columns).

        If there are no test results, returns an empty DataFrame with the appropriate schema.
        """
        df = self.results

        test_columns = df.columns[1:]  # Exclude the first column

        # Lists to collect aggregation expressions and column info
        agg_exprs = []
        col_infos = []

        for col_name in test_columns:
            is_boolean = isinstance(df.schema[col_name].dataType, BooleanType)
            description = self.descriptions.get(col_name, "")

            # Collect column info
            col_infos.append(
                {
                    "col_name": col_name,
                    "is_boolean": is_boolean,
                    "description": description,
                }
            )

            # Expression to count non-null entries
            n_tests_expr = F.sum(
                F.when(F.col(col_name).isNotNull(), 1).otherwise(0)
            ).alias(f"{col_name}_n_tests")
            agg_exprs.append(n_tests_expr)

            if is_boolean:
                # Expressions for n_passed and n_failed
                n_passed_expr = F.sum(F.when(F.col(col_name), 1).otherwise(0)).alias(
                    f"{col_name}_n_passed"
                )
                n_failed_expr = F.sum(F.when(~F.col(col_name), 1).otherwise(0)).alias(
                    f"{col_name}_n_failed"
                )
                agg_exprs.extend([n_passed_expr, n_failed_expr])

        # If there are no expressions, return an empty DataFrame with the defined schema
        if not agg_exprs:
            schema = StructType(
                [
                    StructField("test", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("n_tests", LongType(), True),
                    StructField("n_passed", LongType(), True),
                    StructField("percentage_passed", DoubleType(), True),
                    StructField("n_failed", LongType(), True),
                    StructField("percentage_failed", DoubleType(), True),
                ]
            )
            return self.spark.createDataFrame([], schema)

        # Perform all aggregations in a single pass
        agg_results = df.agg(*agg_exprs).collect()[0]

        # Prepare the summary data
        summary_data = []
        for info in col_infos:
            col_name = info["col_name"]
            is_boolean = info["is_boolean"]
            description = info["description"]

            n_tests = agg_results[f"{col_name}_n_tests"]

            if is_boolean:
                n_passed = agg_results[f"{col_name}_n_passed"]
                n_failed = agg_results[f"{col_name}_n_failed"]
                percentage_passed = (
                    round(n_passed / n_tests * 100, 2) if n_tests > 0 else 0.0
                )
                percentage_failed = (
                    round(n_failed / n_tests * 100, 2) if n_tests > 0 else 0.0
                )
            else:
                n_passed = None
                n_failed = None
                percentage_passed = None
                percentage_failed = None

            summary_data.append(
                (
                    col_name,
                    description,
                    n_tests,
                    n_passed,
                    percentage_passed,
                    n_failed,
                    percentage_failed,
                )
            )

        # Define the schema explicitly
        schema = StructType(
            [
                StructField("test", StringType(), True),
                StructField("description", StringType(), True),
                StructField("n_tests", LongType(), True),
                StructField("n_passed", LongType(), True),
                StructField("percentage_passed", DoubleType(), True),
                StructField("n_failed", LongType(), True),
                StructField("percentage_failed", DoubleType(), True),
            ]
        )

        # Create DataFrame from the summary data
        return self.spark.createDataFrame(summary_data, schema)

    @property
    def passed_tests(self) -> DataFrame:
        """
        Returns a DataFrame containing only the rows where no test has failed (no value is False).

        Returns:
            DataFrame: A DataFrame containing only the rows where no test has failed.
        """
        conditions = [
            (F.col(col) != F.lit(False) | F.col(col).isNull())
            for col in self.results.columns[1:]
            if isinstance(self.results.schema[col].dataType, BooleanType)
        ]

        # If there are no boolean columns, return all rows
        if not conditions:
            return self.results

        combined_condition = reduce(lambda x, y: x & y, conditions)

        return self.results.filter(combined_condition)

    @property
    def failed_tests(self) -> DataFrame:
        """
        Returns a DataFrame containing only the rows where any test has failed (at least one value is False).

        Returns:
            DataFrame: A DataFrame containing only the rows where any test has failed.
        """
        conditions = [
            F.col(col) == F.lit(False)
            for col in self.results.columns[1:]
            if isinstance(self.results.schema[col].dataType, BooleanType)
        ]

        # If there are no boolean columns, return an empty DataFrame
        if not conditions:
            return self.results.limit(0)

        combined_condition = reduce(lambda x, y: x | y, conditions)

        return self.results.filter(combined_condition)
