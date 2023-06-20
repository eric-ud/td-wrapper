# td-wrapper: over engineered wrapper for official Teradata SQL Driver for Python

## What is it?

**td-wrapper** is a wrapper for much easer ad-hoc querying Teradata.

**This is project for my own convenience, user discretion is advised.**

## Features

- Lazy row retrieving
- Use of context manager
- Ease of loading data into volatile tables

## Use

There is example in /examples folder.

```python
with teradatasql.connect(
    host=HOST, logmech="LDAP", user=USERNAME, password=PASSWORD, tmode="ANSI"
) as conn:
    x = []
    with Query(conn, query_text, test_input, batch=100) as query:
        for result in query:
            if result is not StatementExecutionResult.NEXT_RESULT:
                x.append(result)

print(x[0])
```

## Installation

```sh
git clone git@github.com:Udzhukhu-Eric/td-wrapper.git
cd td-wrapper
python setup.py sdist
pip install dist/tdwrapper-0.0.1.tar.gz
```
