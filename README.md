[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)
![Build Status](https://github.com/woonstadrotterdam/pyspark-testframework/actions/workflows/cicd.yml/badge.svg)

# pyspark-testframework

⏳ **Work in progress**

![](https://progress-bar.dev/100/?title=RegexTest&width=120)  
![](https://progress-bar.dev/50/?title=IsInteger&width=120)  
![](https://progress-bar.dev/50/?title=ValidEmail&width=113)  
![](https://progress-bar.dev/0/?title=ContainsValue&width=95)  
![](https://progress-bar.dev/0/?title=ValidValueRange&width=83)  
![](https://progress-bar.dev/0/?title=ValidCategory&width=95)  
![](https://progress-bar.dev/0/?title=CorrectValue&width=102)  
![](<https://progress-bar.dev/0/?title=(...)&width=145>)

The goal of the `pyspark-testframework` is to provide a simple way to create tests for PySpark DataFrames. The test results are returned in DataFrame format as well.

## Example

Input DataFrame:

| primary_key | email                     |
| ----------- | ------------------------- |
| 1           | info@woonstadrotterdam.nl |
| 2           | infowoonstadrotterdam.nl  |
| 3           | @woonstadrotterdam.nl     |
| 4           | dev@woonstadrotterdam.nl  |
| 5           | Null                      |

```python
from testframework.tests import RegexTest

email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

mail_tester = RegexTest(
    name="ValidEmail",
    pattern=email_regex
)

test_result = mail_tester.test(
    df=df,
    col="email",
    nullable=False
)

test_result.show()
```

| primary_key | email\_\_ValidEmail |
| ----------- | ------------------- |
| 1           | True                |
| 2           | False               |
| 3           | False               |
| 4           | True                |
| 5           | False               |
