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
![](https://progress-bar.dev/50/?title=ValidEmail&width=113)  
![](https://progress-bar.dev/0/?title=ContainsValue&width=95)  
![](https://progress-bar.dev/0/?title=CorrectValue&width=102)  
![](<https://progress-bar.dev/0/?title=(...)&width=145>)

The goal of the `pyspark-testframework` is to provide a simple way to create tests for PySpark DataFrames. The test results are returned in DataFrame format as well.

## Example

Input DataFrame:

| primary_key | email                     | number |
| ----------- | ------------------------- | ------ |
| 1           | info@woonstadrotterdam.nl | 123    |
| 2           | infowoonstadrotterdam.nl  | 01     |
| 3           | @woonstadrotterdam.nl     | -45    |
| 4           | dev@woonstadrotterdam.nl  | 1.0    |
| 5           | Null                      | Null   |

```python
from testframework.tests import RegexTest, IsIntegerString

# test for valid email addresses
email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

mail_tester = RegexTest(
    name="ValidEmail",
    pattern=email_regex
)

test_result_email = mail_tester.test(
    df=df,
    col="email",
    primary_key="primary_key",
    nullable=False
)

# test for integer strings
integer_string_tester = IsIntegerString()

test_result_number = number_tester.test(
    df=df,
    col="number",
    primary_key="primary_key",
    nullable=True
)

test_result_email.show()
test_result_number.show()
```

Output for ValidEmail:

| primary_key | email                     | email\_\_ValidEmail |
| ----------- | ------------------------- | ------------------- |
| 1           | info@woonstadrotterdam.nl | True                |
| 2           | infowoonstadrotterdam.nl  | False               |
| 3           | @woonstadrotterdam.nl     | False               |
| 4           | dev@woonstadrotterdam.nl  | True                |
| 5           | Null                      | False               |

Output for IsIntegerString:

| primary_key | number | number\_\_IsIntegerString |
| ----------- | ------ | ------------------------- |
| 1           | 123    | True                      |
| 2           | 01     | False                     |
| 3           | -45    | True                      |
| 4           | 1.0    | True                      |
| 5           | Null   | True                      |
