![](https://img.shields.io/pypi/pyversions/pyspark-testframework)
![Build Status](https://github.com/woonstadrotterdam/pyspark-testframework/actions/workflows/cicd.yml/badge.svg)
[![Version](https://img.shields.io/pypi/v/pyspark-testframework)](https://pypi.org/project/pyspark-testframework/)
![](https://img.shields.io/github/license/woonstadrotterdam/pyspark-testframework)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

# pyspark-testframework

⏳ **Work in progress**

![](https://progress-bar.dev/100/?title=RegexTest&width=120)  
![](https://progress-bar.dev/100/?title=IsIntegerString&width=83)  
![](https://progress-bar.dev/100/?title=ValidNumericRange&width=72)  
![](https://progress-bar.dev/100/?title=ValidCategory&width=95)  
![](https://progress-bar.dev/100/?title=CorrectValue&width=102)  
![](https://progress-bar.dev/100/?title=ValidNumericStringRange&width=36)  
![](https://progress-bar.dev/50/?title=ValidEmail&width=113)  
![](https://progress-bar.dev/0/?title=ContainsValue&width=95)  
![](<https://progress-bar.dev/0/?title=(...)&width=145>)

The goal of the `pyspark-testframework` is to provide a simple way to create tests for PySpark DataFrames. The test results are returned in DataFrame format as well.

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
        StructField("primary_key", IntegerType(), True),
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

    +-----------+--------------------+------------+
    |primary_key|street              |house_number|
    +-----------+--------------------+------------+
    |1          |Rochussenstraat     |27          |
    |2          |Coolsingel          |31          |
    |3          |%Witte de Withstraat|27          |
    |4          |Lijnbaan            |-3          |
    |5          |null                |13          |
    +-----------+--------------------+------------+

**Import and initialize the `DataFrameTester`**

```python
from testframework.dataquality import DataFrameTester
```

```python
df_tester = DataFrameTester(
    df=df,
    primary_key="primary_key",
    spark=spark,
)
```

**Import configurable tests**

```python
from testframework.dataquality.tests import ValidNumericRange, RegexTest
```

**Initialize the `RegexTest` to test for valid street names**

```python
valid_street_name = RegexTest(
    name="ValidStreetName",
    pattern=r"^[A-Z][a-zéèáàëï]*([ -][A-Z]?[a-zéèáàëï]*)*$",
)
```

**Run `valid_street_name` on the _street_ column using the `.test()` method of `DataFrameTester`.**

```python
df_tester.test(
    col="street",
    test=valid_street_name,
    nullable=False,  # nullable, hence null values are converted to True
    description="street contains valid Dutch street name.",
).show(truncate=False)
```

    +-----------+--------------------+-----------------------+
    |primary_key|street              |street__ValidStreetName|
    +-----------+--------------------+-----------------------+
    |1          |Rochussenstraat     |true                   |
    |2          |Coolsingel          |true                   |
    |3          |%Witte de Withstraat|false                  |
    |4          |Lijnbaan            |true                   |
    |5          |null                |false                  |
    +-----------+--------------------+-----------------------+

**Run the `IntegerString` test on the _number_ column**

```python
df_tester.test(
    col="house_number",
    test=ValidNumericRange(
        min_value=0,
    ),
    nullable=True,  # nullable, hence null values are converted to True
    # description is optional, let's not define it for illustration purposes
).show()
```

    +-----------+------------+-------------------------------+
    |primary_key|house_number|house_number__ValidNumericRange|
    +-----------+------------+-------------------------------+
    |          1|          27|                           true|
    |          2|          31|                           true|
    |          3|          27|                           true|
    |          4|          -3|                          false|
    |          5|          13|                           true|
    +-----------+------------+-------------------------------+

**Let's take a look at the test results of the DataFrame using the `.results` attribute.**

```python
df_tester.results.show(truncate=False)
```

    +-----------+-----------------------+-------------------------------+
    |primary_key|street__ValidStreetName|house_number__ValidNumericRange|
    +-----------+-----------------------+-------------------------------+
    |1          |true                   |true                           |
    |2          |true                   |true                           |
    |3          |false                  |true                           |
    |4          |true                   |false                          |
    |5          |false                  |true                           |
    +-----------+-----------------------+-------------------------------+

**We can use `.descriptions` or `.descriptions_df` to get the descriptions of the tests.**

<br>
This can be useful for reporting purposes.   
For example to create reports for the business with more detailed information than just the column name and the test name.

```python
df_tester.descriptions
```

    {'street__ValidStreetName': 'street contains valid Dutch street name.',
     'house_number__ValidNumericRange': 'house_number__ValidNumericRange(min_value=0.0, max_value=inf)'}

```python
df_tester.description_df.show(truncate=False)
```

    +-------------------------------+-------------------------------------------------------------+
    |test                           |description                                                  |
    +-------------------------------+-------------------------------------------------------------+
    |street__ValidStreetName        |street contains valid Dutch street name.                     |
    |house_number__ValidNumericRange|house_number__ValidNumericRange(min_value=0.0, max_value=inf)|
    +-------------------------------+-------------------------------------------------------------+

### Custom tests

Sometimes tests are too specific or complex to be covered by the configurable tests. That's why we can create custom tests and add them to the `DataFrameTester` object.

Let's do this using a custom test which should tests that every house has a bath room. We'll start by creating a new DataFrame with rooms rather than houses.

```python
rooms = [
    (1, "living room"),
    (1, "bath room"),
    (1, "kitchen"),
    (1, "bed room"),
    (2, "living room"),
    (2, "bed room"),
    (2, "kitchen"),
]

schema_rooms = StructType(
    [
        StructField("primary_key", IntegerType(), True),
        StructField("room", StringType(), True),
    ]
)

room_df = spark.createDataFrame(rooms, schema=schema_rooms)

room_df.show(truncate=False)
```

    +-----------+-----------+
    |primary_key|room       |
    +-----------+-----------+
    |1          |living room|
    |1          |bath room  |
    |1          |kitchen    |
    |1          |bed room   |
    |2          |living room|
    |2          |bed room   |
    |2          |kitchen    |
    +-----------+-----------+

To create a custom test, we should create a pyspark DataFrame which contains the same primary_key column as the DataFrame to be tested using the `DataFrameTester`.

Let's create a boolean column that indicates whether the house has a bath room or not.

```python
house_has_bath_room = room_df.groupBy("primary_key").agg(
    F.max(F.when(F.col("room") == "bath room", True).otherwise(False)).alias(
        "has_bath_room"
    )
)

house_has_bath_room.show(truncate=False)
```

    +-----------+-------------+
    |primary_key|has_bath_room|
    +-----------+-------------+
    |1          |true         |
    |2          |false        |
    +-----------+-------------+

**We can add this 'custom test' to the `DataFrameTester` using `add_custom_test_result`.**

In the background, all kinds of data validation checks are done by `DataFrameTester` to make sure that it fits the requirements to be added to the other test results.

```python
df_tester.add_custom_test_result(
    result=house_has_bath_room,
    name="has_bath_room",
    description="House has a bath room",
    # fillna_value=0, # optional; by default null.
).show(truncate=False)
```

    +-----------+-------------+
    |primary_key|has_bath_room|
    +-----------+-------------+
    |1          |true         |
    |2          |false        |
    |3          |null         |
    |4          |null         |
    |5          |null         |
    +-----------+-------------+

**Despite that the data whether a house has a bath room is not available in the house DataFrame; we can still add the custom test to the `DataFrameTester` object.**

```python
df_tester.results.show(truncate=False)
```

    +-----------+-----------------------+-------------------------------+-------------+
    |primary_key|street__ValidStreetName|house_number__ValidNumericRange|has_bath_room|
    +-----------+-----------------------+-------------------------------+-------------+
    |1          |true                   |true                           |true         |
    |2          |true                   |true                           |false        |
    |3          |false                  |true                           |null         |
    |4          |true                   |false                          |null         |
    |5          |false                  |true                           |null         |
    +-----------+-----------------------+-------------------------------+-------------+

```python
df_tester.descriptions
```

    {'street__ValidStreetName': 'street contains valid Dutch street name.',
     'house_number__ValidNumericRange': 'house_number__ValidNumericRange(min_value=0.0, max_value=inf)',
     'has_bath_room': 'House has a bath room'}

**We can also get a summary of the test results using the `.summary` attribute.**

```python
df_tester.summary.show(truncate=False)
```

    +-------------------------------+-------------------------------------------------------------+-------+--------+-----------------+--------+-----------------+
    |test                           |description                                                  |n_tests|n_passed|percentage_passed|n_failed|percentage_failed|
    +-------------------------------+-------------------------------------------------------------+-------+--------+-----------------+--------+-----------------+
    |street__ValidStreetName        |street contains valid Dutch street name.                     |5      |3       |60.0             |2       |40.0             |
    |house_number__ValidNumericRange|house_number__ValidNumericRange(min_value=0.0, max_value=inf)|5      |4       |80.0             |1       |20.0             |
    |has_bath_room                  |House has a bath room                                        |2      |1       |50.0             |1       |50.0             |
    +-------------------------------+-------------------------------------------------------------+-------+--------+-----------------+--------+-----------------+

**If you want to see all rows that failed any of the tests, you can use the `.failed_tests` attribute.**

```python
df_tester.failed_tests.show(truncate=False)
```

    +-----------+-----------------------+-------------------------------+-------------+
    |primary_key|street__ValidStreetName|house_number__ValidNumericRange|has_bath_room|
    +-----------+-----------------------+-------------------------------+-------------+
    |2          |true                   |true                           |false        |
    |3          |false                  |true                           |null         |
    |4          |true                   |false                          |null         |
    |5          |false                  |true                           |null         |
    +-----------+-----------------------+-------------------------------+-------------+

**Of course, you can also see all rows that passed all tests using the `.passed_tests` attribute.**

```python
df_tester.passed_tests.show(truncate=False)
```

    +-----------+-----------------------+-------------------------------+-------------+
    |primary_key|street__ValidStreetName|house_number__ValidNumericRange|has_bath_room|
    +-----------+-----------------------+-------------------------------+-------------+
    |1          |true                   |true                           |true         |
    +-----------+-----------------------+-------------------------------+-------------+
