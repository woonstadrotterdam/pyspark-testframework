import logging
from typing import Any, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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
        primary_key (str): The name of the column used as a primary key. The column should contain only unique values. Rows where the primary key is null are deleted.
        spark (SparkSession): The SparkSession to use for the tests.
        context_cols (Optional[list[str]]): Additional columns to include in the results DataFrame alongside the primary key. Columns that overlap with primary_key are automatically filtered out. Defaults to None.
    """

    def __init__(
        self,
        df: DataFrame,
        primary_key: str,
        spark: SparkSession,
        context_cols: Optional[list[str]] = None,
    ) -> None:
        self.primary_key = primary_key
        self.spark = spark
        self.df = self._check_primary_key(df)
        self.results: DataFrame = self._initialize_results_dataframe(context_cols)
        self.datetime = F.current_timestamp()

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

    def _initialize_results_dataframe(
        self, context_cols: Optional[list[str]]
    ) -> DataFrame:
        """
        Initialize the results DataFrame with long-format structure.

        Args:
            context_cols (Optional[list[str]]): Additional columns to include alongside primary key.

        Returns:
            DataFrame: The initialized empty results DataFrame in long-format.

        Raises:
            ValueError: If any columns in context_cols are not found in the DataFrame.
        """
        if context_cols:
            # Check all columns at once for better performance
            missing_cols = [col for col in context_cols if col not in self.df.columns]
            if missing_cols:
                available_cols = sorted(self.df.columns)
                raise ValueError(
                    f"Column(s) {missing_cols} not found in DataFrame. "
                    f"Available columns: {available_cols}"
                )

            # Remove duplicates and preserve order while avoiding primary key duplication
            context_cols_filtered = [
                col for col in context_cols if col != self.primary_key
            ]
            self.non_test_cols = [self.primary_key] + context_cols_filtered

        else:
            self.non_test_cols = [self.primary_key]

        # Create empty long-format DataFrame schema (ordered for downstream selects)
        schema = StructType(
            [
                StructField(
                    "primary_key", StringType(), True
                ),  # Concatenated primary key
                StructField("test_name", StringType(), True),  # Test name
                StructField("test_result", BooleanType(), True),  # Test result
                StructField("test_value", StringType(), True),  # Actual value tested
                StructField("test_description", StringType(), True),  # Test description
                StructField("test_col", StringType(), True),  # Column tested
                StructField(
                    "primary_key_col", StringType(), True
                ),  # Primary key column name
                StructField("timestamp", TimestampType(), True),  # UTC timestamp
            ]
        )

        # Add context columns if any
        for col in self.non_test_cols:
            if col != self.primary_key:
                schema.add(StructField(col, StringType(), True))

        return self.spark.createDataFrame([], schema)

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
        if self.primary_key not in df.columns:
            unique_primary_keys = self.potential_primary_keys(df)
            raise KeyError(
                f"Primary key column '{self.primary_key}' is not an existing column in this DataFrame.",
                f"Unique primary keys in this DataFrame: {unique_primary_keys}",
            )

        df_filtered = df.dropna(subset=[self.primary_key])
        null_count = df.count() - df_filtered.count()
        if null_count > 0:
            logger.warning(
                f"Primary key '{self.primary_key}' contains null values, {null_count} rows are excluded."
            )

        self.assert_primary_key_unique(df_filtered, self.primary_key)
        return df_filtered

    @classmethod
    def assert_primary_key_unique(cls, df: DataFrame, primary_key: str) -> None:
        """
        Checks if the specified primary key column contains unique values.

        Args:
            df (DataFrame): The DataFrame to check.
            primary_key (str): The primary key column name.

        Raises:
            ValueError: If the primary key column does not contain unique values.
        """
        total_count = df.count()
        distinct_count = df.select(primary_key).distinct().count()
        if total_count != distinct_count:
            logger.error(
                f"Primary key '{primary_key}' is not unique ({total_count = }, {distinct_count = }). Determining potential primary keys.."
            )
            unique_primary_keys = cls.potential_primary_keys(
                df, total_count=total_count
            )
            raise ValueError(
                f"Primary key '{primary_key}' is not unique",
                f"Unique primary keys in this DataFrame: {unique_primary_keys}",
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
            return_extra_cols (Optional[list[str]]): Return extra columns from the original dataframe (not saved to results). Defaults to None.
            dummy_run (bool): If True, perform a dummy run without saving results. Defaults to False.
            return_failed_rows (bool): If True, return only the rows where the test has failed. Defaults to False.

        Returns:
            DataFrame: The test results as a DataFrame in long-format.

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

        # Tests now return long-format DataFrames directly
        long_format_result = test.test(filtered_df, col, self.primary_key, nullable)

        # Add the timestamp and primary_key_col columns (and order them)
        long_format_result = long_format_result.select(
            long_format_result.primary_key,
            long_format_result.test_name,
            long_format_result.test_result,
            long_format_result.test_value,
            long_format_result.test_description,
            long_format_result.test_col,
            F.lit(self.primary_key).alias("primary_key_col"),
            self.datetime.alias("timestamp"),
        )

        # Override description if provided
        if description:
            long_format_result = long_format_result.withColumn(
                "test_description", F.lit(description)
            )

        # Add context columns if they exist
        if hasattr(self, "non_test_cols") and len(self.non_test_cols) > 1:
            # Join with original data to get context columns, excluding the column being tested
            context_cols_to_include = [
                col_name
                for col_name in self.non_test_cols
                if col_name != self.primary_key and col_name != col
            ]
            if context_cols_to_include:
                context_cols_df = filtered_df.select(
                    self.primary_key, *context_cols_to_include
                )
                long_format_result = long_format_result.join(
                    context_cols_df,
                    long_format_result.primary_key == context_cols_df[self.primary_key],
                    how="left",
                ).select(
                    long_format_result.primary_key,
                    long_format_result.test_name,
                    long_format_result.test_result,
                    long_format_result.test_value,
                    long_format_result.test_description,
                    long_format_result.test_col,
                    long_format_result.primary_key_col,
                    long_format_result.timestamp,
                    *[F.col(col).alias(col) for col in context_cols_to_include],
                )

        if not dummy_run:
            # Union with existing results, handling schema differences
            self.results = self._union_with_schema_alignment(
                self.results, long_format_result
            )

        if return_failed_rows:
            long_format_result = long_format_result.filter(
                F.col("test_result") == F.lit(False)
            )

        if return_extra_cols:
            # Join with original data for extra columns (not saved to results)
            # Filter out columns that are already context columns to avoid duplication
            context_col_names = set(self.non_test_cols) - {self.primary_key}
            extra_cols_filtered = [
                col for col in return_extra_cols if col not in context_col_names
            ]

            if extra_cols_filtered:
                extra_cols_df = self.df.select(self.primary_key, *extra_cols_filtered)
                long_format_result = long_format_result.join(
                    extra_cols_df,
                    long_format_result.primary_key == extra_cols_df[self.primary_key],
                    how="left",
                ).select(
                    long_format_result["*"],
                    *[F.col(col) for col in extra_cols_filtered],
                )

        return self._standardize_return_order(
            long_format_result.drop("primary_key_col", "timestamp")
        )

    def _union_with_schema_alignment(self, df1: DataFrame, df2: DataFrame) -> DataFrame:
        """
        Union two DataFrames with schema alignment to handle different context columns.

        Args:
            df1 (DataFrame): First DataFrame (existing results)
            df2 (DataFrame): Second DataFrame (new test results)

        Returns:
            DataFrame: Unioned DataFrame with aligned schema
        """
        # Determine desired column order: keep df1 order, then append new columns from df2 in their order
        desired_order = list(df1.columns) + [
            c for c in df2.columns if c not in df1.columns
        ]

        # Create select expressions for both DataFrames in the desired order, filling missing columns with nulls
        select_exprs_df1 = [
            F.col(c) if c in df1.columns else F.lit(None).alias(c)
            for c in desired_order
        ]
        select_exprs_df2 = [
            F.col(c) if c in df2.columns else F.lit(None).alias(c)
            for c in desired_order
        ]

        # Apply the select expressions and union
        df1_aligned = df1.select(*select_exprs_df1)
        df2_aligned = df2.select(*select_exprs_df2)

        return df1_aligned.union(df2_aligned)

    def _standardize_return_order(self, df: DataFrame) -> DataFrame:
        """
        Ensure a consistent column order for returned DataFrames, led by the
        base columns as defined by the .test() method. Any additional columns
        (context/extra) are appended in their existing order.

        The canonical leading order is:
        ["primary_key", "test_name", "test_result", "test_value", "test_description", "test_col"].
        """
        base_order = [
            "primary_key",
            "test_name",
            "test_result",
            "test_value",
            "test_description",
            "test_col",
        ]

        # Keep only columns that exist; append any remaining columns in existing order
        present_leading = [c for c in base_order if c in df.columns]
        trailing = [c for c in df.columns if c not in present_leading]
        return df.select(
            *([F.col(c) for c in present_leading] + [F.col(c) for c in trailing])
        )

    def add_custom_test_result(
        self,
        result: DataFrame,
        name: str,
        description: Optional[str] = None,
        fillna_value: Optional[Any] = None,
        return_extra_cols: Optional[list[str]] = None,
        return_failed_rows: bool = False,
        value_column: Optional[str] = None,
    ) -> DataFrame:
        """
        Adds custom test results to the test DataFrame.

        Args:
            result (DataFrame): The DataFrame containing the test results.
            name (str): The name of the custom test, which should be a column in test_result.
            description (Optional[str]): Description of the test for reporting purposes.
            fillna_value (Optional[Any]): The value to fill nulls in the test result column after left joining on the primary_key. Defaults to None.
            return_extra_cols (Optional[list[str]]): Return extra columns from the original dataframe (not saved to results). Defaults to None.
            return_failed_rows (bool): If True, return only the rows where the test has failed. Defaults to False.
            value_column (Optional[str]): The name of the column containing the actual values being tested. If provided, these values will be used in the test_value field instead of "__custom__test__value__". Defaults to None.

        Returns:
            DataFrame: The updated test DataFrame with the custom test results in long-format.

        Raises:
            TypeError: If result is not a pyspark DataFrame.
            ValueError: If the primary key is not found in the result DataFrame or is not unique.
            TypeError: If description is not of type string or None.
            ValueError: If value_column is specified but not found in the result DataFrame.
        """

        if not isinstance(result, DataFrame):
            raise TypeError(
                f"test_result should be a pyspark DataFrame, but it's a {type(result)}"
            )

        if self.primary_key not in result.columns:
            raise ValueError(f"primary_key '{self.primary_key}' not found in DataFrame")

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

        if value_column is not None and value_column not in result.columns:
            raise ValueError(
                f"value_column '{value_column}' not found in result DataFrame"
            )

        # Convert custom test result to long-format
        long_format_result = self._convert_custom_to_long_format(
            result, name, description, value_column
        )

        # Add context columns by joining with original DataFrame
        if hasattr(self, "non_test_cols") and len(self.non_test_cols) > 1:
            # For custom tests, we don't know which column is being tested, so include all context columns
            context_cols_df = self.df.select(
                self.primary_key,
                *[col for col in self.non_test_cols if col != self.primary_key],
            )
            long_format_result = long_format_result.join(
                context_cols_df,
                long_format_result.primary_key == context_cols_df[self.primary_key],
                how="left",
            ).select(
                long_format_result.primary_key,
                long_format_result.test_name,
                long_format_result.test_result,
                long_format_result.test_value,
                long_format_result.test_description,
                long_format_result.test_col,
                long_format_result.primary_key_col,
                long_format_result.timestamp,
                *[
                    F.col(col).alias(col)
                    for col in self.non_test_cols
                    if col != self.primary_key
                ],
            )

        # Apply fillna if specified
        if fillna_value is not None:
            long_format_result = long_format_result.fillna(
                {"test_result": fillna_value}
            )

        # Union with existing results, handling schema differences
        self.results = self._union_with_schema_alignment(
            self.results, long_format_result
        )

        if return_failed_rows:
            long_format_result = long_format_result.filter(
                F.col("test_result") == F.lit(False)
            )

        if return_extra_cols:
            # Join with original data for extra columns (not saved to results)
            # Filter out columns that are already context columns to avoid duplication
            context_col_names = set(self.non_test_cols) - {self.primary_key}
            extra_cols_filtered = [
                col for col in return_extra_cols if col not in context_col_names
            ]

            if extra_cols_filtered:
                extra_cols_df = self.df.select(self.primary_key, *extra_cols_filtered)
                long_format_result = long_format_result.join(
                    extra_cols_df,
                    long_format_result.primary_key == extra_cols_df[self.primary_key],
                    how="left",
                ).select(
                    long_format_result["*"],
                    *[F.col(col) for col in extra_cols_filtered],
                )

        return self._standardize_return_order(long_format_result)

    def _convert_custom_to_long_format(
        self,
        result: DataFrame,
        name: str,
        description: Optional[str],
        value_column: Optional[str] = None,
    ) -> DataFrame:
        """
        Convert custom test result to long-format.

        Args:
            result (DataFrame): DataFrame with custom test results
            name (str): Name of the custom test
            description (Optional[str]): Description of the test
            value_column (Optional[str]): Optional column name containing the actual values being tested

        Returns:
            DataFrame: Long-format DataFrame with test results
        """
        # Create primary key string (no concatenation needed for single primary key)
        pk_expr = F.col(self.primary_key).cast(StringType())

        # Determine test_value expression based on whether value_column is provided
        if value_column is not None:
            test_value_expr = F.col(value_column).cast(StringType())
        else:
            test_value_expr = F.lit("__custom__test__value__")

        # Select columns for long-format (ordered)
        select_exprs = [
            pk_expr.alias("primary_key"),
            F.lit(name).alias("test_name"),
            F.col(name).alias("test_result"),
            test_value_expr.alias("test_value"),
            F.lit(description if description else name).alias("test_description"),
            F.lit("__custom__test__col__").alias("test_col"),
            F.lit(self.primary_key).alias("primary_key_col"),
            self.datetime.alias("timestamp"),
        ]

        return result.select(*select_exprs)

    @property
    def summary(self) -> DataFrame:
        """
        Generate a summary DataFrame that provides insights into the test results stored in the `results` DataFrame.

        The summary includes:
        - The number of tests (`n_tests`) conducted for each test.
        - For Boolean columns:
            - The number of passed tests (`n_passed`).
            - The percentage of passed tests (`percentage_passed`).
            - The number of failed tests (`n_failed`).
            - The percentage of failed tests (`percentage_failed`).

        Returns:
            DataFrame: A Spark DataFrame containing the summary statistics for each test. The DataFrame has the following schema:
                - `test_name`: The name of the test.
                - `test_col`: The column that was tested.
                - `test_description`: A description of the test (if available).
                - `primary_key_col`: The name of the primary key column.
                - `n_tests`: The number of non-null entries for the test.
                - `n_passed`: The number of entries that passed the test.
                - `percentage_passed`: The percentage of passed tests.
                - `n_failed`: The number of entries that failed the test.
                - `percentage_failed`: The percentage of failed tests.
                - `timestamp`: The timestamp when the test was executed.

        If there are no test results, returns an empty DataFrame with the appropriate schema.
        """
        if self.results.count() == 0:
            schema = StructType(
                [
                    StructField("test_name", StringType(), True),
                    StructField("test_col", StringType(), True),
                    StructField("test_description", StringType(), True),
                    StructField("n_tests", LongType(), True),
                    StructField("n_passed", LongType(), True),
                    StructField("percentage_passed", DoubleType(), True),
                    StructField("n_failed", LongType(), True),
                    StructField("percentage_failed", DoubleType(), True),
                    StructField("primary_key_col", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                ]
            )
            return self.spark.createDataFrame([], schema)

        # Group by test_name and test_col to get per-column test summaries
        summary_df = (
            self.results.groupBy("test_name", "test_col")
            .agg(
                F.sum(F.when(F.col("test_result").isNotNull(), 1).otherwise(0)).alias(
                    "n_tests"
                ),
                F.sum(F.when(F.col("test_result"), 1).otherwise(0)).alias("n_passed"),
                F.sum(F.when(~F.col("test_result"), 1).otherwise(0)).alias("n_failed"),
                F.first("test_description").alias("test_description"),
                F.first("primary_key_col").alias("primary_key_col"),
                F.first("timestamp").alias("timestamp"),
            )
            .withColumn(
                "percentage_passed",
                F.when(
                    F.col("n_tests") > 0, F.col("n_passed") / F.col("n_tests") * 100
                ).otherwise(0.0),
            )
            .withColumn(
                "percentage_failed",
                F.when(
                    F.col("n_tests") > 0, F.col("n_failed") / F.col("n_tests") * 100
                ).otherwise(0.0),
            )
            .select(
                "test_name",
                "test_description",
                "test_col",
                "n_tests",
                "n_passed",
                "percentage_passed",
                "n_failed",
                "percentage_failed",
                "primary_key_col",
                "timestamp",
            )
        )

        return summary_df

    @property
    def passed_tests(self) -> DataFrame:
        """
        Returns a DataFrame containing only the rows where tests have passed (result is True).

        Returns:
            DataFrame: A DataFrame containing only the rows where tests have passed.
        """
        return self._standardize_return_order(
            self.results.filter(F.col("test_result") == F.lit(True)).drop(
                "primary_key_col", "timestamp"
            )
        )

    @property
    def failed_tests(self) -> DataFrame:
        """
        Returns a DataFrame containing only the rows where tests have failed (result is False).

        Returns:
            DataFrame: A DataFrame containing only the rows where tests have failed.
        """
        return self._standardize_return_order(
            self.results.filter(F.col("test_result") == F.lit(False)).drop(
                "primary_key_col", "timestamp"
            )
        )
