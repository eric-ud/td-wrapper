# td-wrapper: overengineered wrapper for the official Teradata SQL Driver for Python

## What is it?

**td-wrapper** is a wrapper for much easier ad-hoc querying Teradata.

**This is a project for my own convenience; user discretion is advised.**

## Features

- Lazy row retrieval, so you can fetch by user-defined batch size and process big datasets locally
- Use of context managers, so the cursor always automatically closes
- Ease of loading data into tables using Pandas' dataframes
- Storing credentials in environment variables and passwords in the keyring backend

## TODO

- add docstrings
- create documentation for the helper function

## Use

You can check out the test directory for examples.

```python
query = """
    select date today;
"""

correct_result = DataFrame(data={"today": [date.today()]})

with helpers.TeradataConnectionFromKeyring() as conn:
    with core.Query(conn, query) as query:
        for result in query:
            test_result = result

assert test_result.equals(correct_result)
```

a little bit involved example:

```python
query = """
    CALL user_dm.prc_drop_volatile_table (USER||'.test');

    CREATE VOLATILE TABLE test (
        day_id date NOT NULL
    )
    UNIQUE PRIMARY INDEX (day_id)
    ON COMMIT PRESERVE ROWS;

    INSERT INTO test (?);

    SELECT * FROM test ORDER BY day_id asc;

"""

correct_result = DataFrame(
    data={
        "day_id": [
            date.today() - timedelta(days=2),
            date.today() - timedelta(days=1),
            date.today(),
        ]
    }
)

with helpers.TeradataConnectionFromKeyring() as conn:
    with core.Query(conn, query, [correct_result]) as query:
        for result in query:
            test_result = result

assert test_result.equals(correct_result)
```

## Installation

```sh
git clone git@github.com:Udzhukhu-Eric/td-wrapper.git
cd td-wrapper
python setup.py sdist
pip install dist/tdwrapper-0.1.tar.gz
```

or just download the latest release and

```sh
pip install tdwrapper-0.1.tar.gz
```
