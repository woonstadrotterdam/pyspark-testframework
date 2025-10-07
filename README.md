![](https://img.shields.io/pypi/pyversions/pyspark-testframework)
![Build Status](https://github.com/woonstadrotterdam/pyspark-testframework/actions/workflows/cicd.yml/badge.svg)
[![Version](https://img.shields.io/pypi/v/pyspark-testframework)](https://pypi.org/project/pyspark-testframework/)
![](https://img.shields.io/github/license/woonstadrotterdam/pyspark-testframework)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

# pyspark-testframework

The goal of the `pyspark-testframework` is to provide a simple way to create tests for PySpark DataFrames. The test results are returned in DataFrame format as well.

> [!NOTE]
> From version v3.\*.\* we changed from a wide-format to a **long-format** structure for storing test results.
> This long-format approach makes it easier to:
>
> - Filter and analyze specific test results
> - Add new tests without changing the schema
> - Perform aggregations across different tests
> - Export results to other systems
> - Track when tests were executed
> - Include actual values that were tested for debugging

## Test Results

The framework uses a long-format structure for storing test results. Each test result is stored as a separate row with the following columns:

- `primary_key`: Primary key value as string (e.g., "1", "2", "3")
- `primary_key_col`: Name of the primary key column (e.g., "id")
- `test_name`: Name of the test (e.g., "ValidStreetFormat")
- `test_col`: Name of the column that was tested (e.g., "street")
- `test_value`: The actual value that was tested (e.g., "Rochussenstraat")
- `test_result`: Boolean result of the test (True/False)
- `test_description`: Description of the test
- `timestamp`: UTC timestamp when the test was executed
- Additional columns: Any additional context columns specified during initialization (e.g., if you pass `context_cols=["street", "house_number"]`, these columns will be included in the results)

# Tutorial

**Let's first create an example pyspark DataFrame**

The data will contain the primary keys, street names and house numbers of some addresses.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
```

```python
# Initialize Spark session
spark = SparkSession.builder.appName("PySparkTestFrameworkTutorial").getOrCreate()

# Define the schema
schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("street", StringType(), True),
        StructField("house_number", IntegerType(), True),
    ]
)

# Define the data
data = [
    (1, "Rochussenstraat", 27),
    (2, "Coolsingel", 31),
    (3, "%Witte de Withstraat", 27),
    (4, "Lijnbaan", -3),
    (5, None, 13),
]

df = spark.createDataFrame(data, schema)

df.show(truncate=False)
```

    +---+--------------------+------------+
    |id |street              |house_number|
    +---+--------------------+------------+
    |1  |Rochussenstraat     |27          |
    |2  |Coolsingel          |31          |
    |3  |%Witte de Withstraat|27          |
    |4  |Lijnbaan            |-3          |
    |5  |null                |13          |
    +---+--------------------+------------+

**Import and initialize the `DataFrameTester`**

```python
from testframework.dataquality import DataFrameTester
```

```python
df_tester = DataFrameTester(
    df=df,
    primary_key="id",
    spark=spark,
)
```

**Import configurable tests**

```python
from testframework.dataquality.tests import ValidNumericRange, RegexTest
```

**Initialize the `RegexTest` to test for valid street names**

```python
valid_street_format = RegexTest(
    name="ValidStreetFormat",
    pattern=r"^[A-Z][a-zéèáàëï]*([ -][A-Z]?[a-zéèáàëï]*)*$",
)
```

**Run `valid_street_format` on the _street_ column using the `.test()` method of `DataFrameTester`.**

```python
df_tester.test(
    col="street",
    test=valid_street_format,
    nullable=False,  # nullable is False, hence null values are converted to False
    description="Street is in valid Dutch street format",
).show(truncate=False)
```

    +-----------+-----------------+--------+--------------------+-----------+
    |primary_key|test_name        |test_col|test_value          |test_result|
    +-----------+-----------------+--------+--------------------+-----------+
    |1          |ValidStreetFormat|street  |Rochussenstraat     |true       |
    |2          |ValidStreetFormat|street  |Coolsingel          |true       |
    |3          |ValidStreetFormat|street  |%Witte de Withstraat|false      |
    |4          |ValidStreetFormat|street  |Lijnbaan            |true       |
    |5          |ValidStreetFormat|street  |null                |false      |
    +-----------+-----------------+--------+--------------------+-----------+

**Run the `IntegerString` test on the _number_ column**

By setting the `return_failed_rows` parameter to `True`, we can get only the rows that failed the test.

```python
df_tester.test(
    col="house_number",
    test=ValidNumericRange(
        min_value=1,
    ),
    nullable=False,
    # description="House number is in a valid format" # optional, let's not define it for illustration purposes
    return_failed_rows=True,  # only return the failed rows
).show()
```

    +-----------+-----------------+------------+----------+-----------+
    |primary_key|        test_name|    test_col|test_value|test_result|
    +-----------+-----------------+------------+----------+-----------+
    |          4|ValidNumericRange|house_number|        -3|      false|
    +-----------+-----------------+------------+----------+-----------+

**Let's take a look at the test results of the DataFrame using the `.results` attribute.**

```python
df_tester.results.show(truncate=False)
```

    +-----------+---------------+------------+--------------------------------------+-----------------+-----------+--------------------+-----------------------+
    |primary_key|primary_key_col|test_col    |test_description                      |test_name        |test_result|test_value          |timestamp              |
    +-----------+---------------+------------+--------------------------------------+-----------------+-----------+--------------------+-----------------------+
    |1          |id             |street      |Street is in valid Dutch street format|ValidStreetFormat|true       |Rochussenstraat     |2025-10-07 08:25:51.433|
    |2          |id             |street      |Street is in valid Dutch street format|ValidStreetFormat|true       |Coolsingel          |2025-10-07 08:25:51.433|
    |3          |id             |street      |Street is in valid Dutch street format|ValidStreetFormat|false      |%Witte de Withstraat|2025-10-07 08:25:51.433|
    |4          |id             |street      |Street is in valid Dutch street format|ValidStreetFormat|true       |Lijnbaan            |2025-10-07 08:25:51.433|
    |5          |id             |street      |Street is in valid Dutch street format|ValidStreetFormat|false      |null                |2025-10-07 08:25:51.433|
    |1          |id             |house_number|ValidNumericRange                     |ValidNumericRange|true       |27                  |2025-10-07 08:25:51.433|
    |2          |id             |house_number|ValidNumericRange                     |ValidNumericRange|true       |31                  |2025-10-07 08:25:51.433|
    |3          |id             |house_number|ValidNumericRange                     |ValidNumericRange|true       |27                  |2025-10-07 08:25:51.433|
    |4          |id             |house_number|ValidNumericRange                     |ValidNumericRange|false      |-3                  |2025-10-07 08:25:51.433|
    |5          |id             |house_number|ValidNumericRange                     |ValidNumericRange|true       |13                  |2025-10-07 08:25:51.433|
    +-----------+---------------+------------+--------------------------------------+-----------------+-----------+--------------------+-----------------------+

### Custom tests

Sometimes tests are too specific or complex to be covered by the configurable tests. That's why we can create custom tests and add them to the `DataFrameTester` object.

Let's do this using a custom test which should tests that every house has a bath room. We'll start by creating a new DataFrame with rooms rather than houses.

```python
rooms = [
    (1, 1, "living room"),
    (2, 1, "bathroom"),
    (3, 1, "kitchen"),
    (4, 1, "bed room"),
    (5, 2, "living room"),
    (6, 2, "bed room"),
    (7, 2, "kitchen"),
]

schema_rooms = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("house_id", IntegerType(), True),
        StructField("room", StringType(), True),
    ]
)

room_df = spark.createDataFrame(rooms, schema=schema_rooms)

room_df.show(truncate=False)
```

    +---+--------+-----------+
    |id |house_id|room       |
    +---+--------+-----------+
    |1  |1       |living room|
    |2  |1       |bathroom   |
    |3  |1       |kitchen    |
    |4  |1       |bed room   |
    |5  |2       |living room|
    |6  |2       |bed room   |
    |7  |2       |kitchen    |
    +---+--------+-----------+

To create a custom test, we should create a pyspark DataFrame which contains the same primary_key column as the DataFrame to be tested using the `DataFrameTester`.

Let's create a boolean column that indicates whether the house has a bath room or not.

```python
house_has_bathroom = room_df.groupBy("house_id").agg(
    F.max(F.when(F.col("room") == "bathroom", True).otherwise(False)).alias(
        "has_bathroom"
    )
)

house_has_bathroom.show(truncate=False)
```

    +--------+------------+
    |house_id|has_bathroom|
    +--------+------------+
    |1       |true        |
    |2       |false       |
    +--------+------------+

**We can add this 'custom test' to the `DataFrameTester` using `add_custom_test_result`.**

In the background, all kinds of data validation checks are done by `DataFrameTester` to make sure that it fits the requirements to be added to the other test results.

```python
df_tester.add_custom_test_result(
    result=house_has_bathroom.withColumnRenamed("house_id", "id"),
    name="has_bathroom",
    description="House has a bathroom",
    # fillna_value=0, # optional; by default null.
).show(truncate=False)
```

    +-----------+---------------+------------+------------------+----------+-----------+--------------------+----------------------+
    |primary_key|primary_key_col|test_name   |test_col          |test_value|test_result|test_description    |timestamp             |
    +-----------+---------------+------------+------------------+----------+-----------+--------------------+----------------------+
    |1          |id             |has_bathroom|custom_test_result|N/A       |true       |House has a bathroom|2025-10-07 08:25:52.74|
    |2          |id             |has_bathroom|custom_test_result|N/A       |false      |House has a bathroom|2025-10-07 08:25:52.74|
    +-----------+---------------+------------+------------------+----------+-----------+--------------------+----------------------+

**Despite that the data whether a house has a bath room is not available in the house DataFrame; we can still add the custom test to the `DataFrameTester` object.**

```python
df_tester.results.show(truncate=False)
```

    +-----------+---------------+------------------+--------------------------------------+-----------------+-----------+--------------------+-----------------------+
    |primary_key|primary_key_col|test_col          |test_description                      |test_name        |test_result|test_value          |timestamp              |
    +-----------+---------------+------------------+--------------------------------------+-----------------+-----------+--------------------+-----------------------+
    |1          |id             |street            |Street is in valid Dutch street format|ValidStreetFormat|true       |Rochussenstraat     |2025-10-07 08:25:52.862|
    |2          |id             |street            |Street is in valid Dutch street format|ValidStreetFormat|true       |Coolsingel          |2025-10-07 08:25:52.862|
    |3          |id             |street            |Street is in valid Dutch street format|ValidStreetFormat|false      |%Witte de Withstraat|2025-10-07 08:25:52.862|
    |4          |id             |street            |Street is in valid Dutch street format|ValidStreetFormat|true       |Lijnbaan            |2025-10-07 08:25:52.862|
    |5          |id             |street            |Street is in valid Dutch street format|ValidStreetFormat|false      |null                |2025-10-07 08:25:52.862|
    |1          |id             |house_number      |ValidNumericRange                     |ValidNumericRange|true       |27                  |2025-10-07 08:25:52.862|
    |2          |id             |house_number      |ValidNumericRange                     |ValidNumericRange|true       |31                  |2025-10-07 08:25:52.862|
    |3          |id             |house_number      |ValidNumericRange                     |ValidNumericRange|true       |27                  |2025-10-07 08:25:52.862|
    |4          |id             |house_number      |ValidNumericRange                     |ValidNumericRange|false      |-3                  |2025-10-07 08:25:52.862|
    |5          |id             |house_number      |ValidNumericRange                     |ValidNumericRange|true       |13                  |2025-10-07 08:25:52.862|
    |1          |id             |custom_test_result|House has a bathroom                  |has_bathroom     |true       |N/A                 |2025-10-07 08:25:52.862|
    |2          |id             |custom_test_result|House has a bathroom                  |has_bathroom     |false      |N/A                 |2025-10-07 08:25:52.862|
    +-----------+---------------+------------------+--------------------------------------+-----------------+-----------+--------------------+-----------------------+

**We can also get a summary of the test results using the `.summary` attribute.**

```python
df_tester.summary.show(truncate=False)
```

    Java HotSpot(TM) 64-Bit Server VM warning: CodeCache is full. Compiler has been disabled.
    Java HotSpot(TM) 64-Bit Server VM warning: Try increasing the code cache size using -XX:ReservedCodeCacheSize=


    CodeCache: size=131072Kb used=34412Kb max_used=34428Kb free=96659Kb
     bounds [0x000000010a1f8000, 0x000000010c3c8000, 0x00000001121f8000]
     total_blobs=12180 nmethods=11270 adapters=820
     compilation: disabled (not enough contiguous free space left)
    +-----------------+--------------------------------------+------------------+-------+--------+-----------------+--------+-----------------+---------------+-----------------------+
    |test_name        |test_description                      |test_col          |n_tests|n_passed|percentage_passed|n_failed|percentage_failed|primary_key_col|timestamp              |
    +-----------------+--------------------------------------+------------------+-------+--------+-----------------+--------+-----------------+---------------+-----------------------+
    |ValidNumericRange|ValidNumericRange                     |house_number      |5      |4       |80.0             |1       |20.0             |id             |2025-10-07 08:25:53.558|
    |ValidStreetFormat|Street is in valid Dutch street format|street            |5      |3       |60.0             |2       |40.0             |id             |2025-10-07 08:25:53.558|
    |has_bathroom     |House has a bathroom                  |custom_test_result|2      |1       |50.0             |1       |50.0             |id             |2025-10-07 08:25:53.558|
    +-----------------+--------------------------------------+------------------+-------+--------+-----------------+--------+-----------------+---------------+-----------------------+

**If you want to see all rows that failed any of the tests, you can use the `.failed_tests` attribute.**

```python
df_tester.failed_tests.show(truncate=False)
```

    +-----------+------------------+--------------------------------------+-----------------+-----------+--------------------+
    |primary_key|test_col          |test_description                      |test_name        |test_result|test_value          |
    +-----------+------------------+--------------------------------------+-----------------+-----------+--------------------+
    |3          |street            |Street is in valid Dutch street format|ValidStreetFormat|false      |%Witte de Withstraat|
    |5          |street            |Street is in valid Dutch street format|ValidStreetFormat|false      |null                |
    |4          |house_number      |ValidNumericRange                     |ValidNumericRange|false      |-3                  |
    |2          |custom_test_result|House has a bathroom                  |has_bathroom     |false      |N/A                 |
    +-----------+------------------+--------------------------------------+-----------------+-----------+--------------------+

**Of course, you can also see all rows that passed all tests using the `.passed_tests` attribute.**

```python
df_tester.passed_tests.show(truncate=False)
```

    +-----------+------------------+--------------------------------------+-----------------+-----------+---------------+
    |primary_key|test_col          |test_description                      |test_name        |test_result|test_value     |
    +-----------+------------------+--------------------------------------+-----------------+-----------+---------------+
    |1          |street            |Street is in valid Dutch street format|ValidStreetFormat|true       |Rochussenstraat|
    |2          |street            |Street is in valid Dutch street format|ValidStreetFormat|true       |Coolsingel     |
    |4          |street            |Street is in valid Dutch street format|ValidStreetFormat|true       |Lijnbaan       |
    |1          |house_number      |ValidNumericRange                     |ValidNumericRange|true       |27             |
    |2          |house_number      |ValidNumericRange                     |ValidNumericRange|true       |31             |
    |3          |house_number      |ValidNumericRange                     |ValidNumericRange|true       |27             |
    |5          |house_number      |ValidNumericRange                     |ValidNumericRange|true       |13             |
    |1          |custom_test_result|House has a bathroom                  |has_bathroom     |true       |N/A            |
    +-----------+------------------+--------------------------------------+-----------------+-----------+---------------+
