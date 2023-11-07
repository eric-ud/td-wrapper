from pandas import DataFrame
import pandas as pd
from datetime import date, timedelta

from tdwrapper import core, helpers


def test_one_statement_one_return() -> None:
    query = """
        select date today;
    """

    correct_result = DataFrame(data={"today": [date.today()]})

    with helpers.TeradataConnectionFromKeyring() as conn:
        with core.Query(conn, query) as query:
            for result in query:
                test_result = result

    assert test_result.equals(correct_result)


def test_many_statements_one_return() -> None:
    query = """
        CALL user_dm.prc_drop_volatile_table (USER||'.test');

        CREATE VOLATILE TABLE test (
            day_id				date NOT NULL
        )
        UNIQUE PRIMARY INDEX (day_id)
        ON COMMIT PRESERVE ROWS;

        INSERT INTO test values (date);
        INSERT INTO test values (date-1);

        SELECT * FROM test ORDER BY day_id asc;

    """

    correct_result = DataFrame(
        data={"day_id": [date.today() - timedelta(days=1), date.today()]}
    )

    with helpers.TeradataConnectionFromKeyring() as conn:
        with core.Query(conn, query) as query:
            for result in query:
                test_result = result

    assert test_result.equals(correct_result)


def test_insert_many_statements_one_return() -> None:
    query = """
        CALL user_dm.prc_drop_volatile_table (USER||'.test');

        CREATE VOLATILE TABLE test (
            day_id				date NOT NULL
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


def test_many_insert_many_statements_one_return() -> None:
    query = """
        CALL user_dm.prc_drop_volatile_table (USER||'.test');

        CREATE VOLATILE TABLE test (
            day_id				date NOT NULL
        )
        UNIQUE PRIMARY INDEX (day_id)
        ON COMMIT PRESERVE ROWS;

        INSERT INTO test (?);
        INSERT INTO test (?);

        SELECT * FROM test ORDER BY day_id asc;

    """

    first_insert = DataFrame(
        data={
            "day_id": [
                date.today() - timedelta(days=2),
                date.today() - timedelta(days=1),
                date.today(),
            ]
        }
    )

    second_insert = DataFrame(
        data={
            "day_id": [
                date.today() - timedelta(days=5),
                date.today() - timedelta(days=4),
                date.today() - timedelta(days=3),
            ]
        }
    )

    correct_result = (
        pd.concat([first_insert, second_insert], axis=0, ignore_index=True)
        .sort_values(by=["day_id"])
        .reset_index(drop=True)
    )

    with helpers.TeradataConnectionFromKeyring() as conn:
        with core.Query(conn, query, [first_insert, second_insert]) as query:
            for result in query:
                test_result = result

    assert test_result.equals(correct_result)


def test_many_insert_many_statements_many_return() -> None:
    query = """
        CALL user_dm.prc_drop_volatile_table (USER||'.test');

        CREATE VOLATILE TABLE test (
            day_id				date NOT NULL
        )
        UNIQUE PRIMARY INDEX (day_id)
        ON COMMIT PRESERVE ROWS;

        INSERT INTO test (?);

        CALL user_dm.prc_drop_volatile_table (USER||'.test2');

        CREATE VOLATILE TABLE test2 (
            day_id				date NOT NULL
        )
        UNIQUE PRIMARY INDEX (day_id)
        ON COMMIT PRESERVE ROWS;

        INSERT INTO test2 (?);

        SELECT * FROM test ORDER BY day_id asc;

        SELECT * FROM test2 ORDER BY day_id asc;


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

    wrong_result = DataFrame(
        data={
            "day_id": [
                date.today() - timedelta(days=5),
                date.today() - timedelta(days=4),
                date.today() - timedelta(days=3),
            ]
        }
    )

    with helpers.TeradataConnectionFromKeyring() as conn:
        with core.Query(conn, query, [correct_result, wrong_result]) as query:
            for result in query:
                test_result = result

    assert test_result.equals(correct_result)


def test_insert_many_statements_one_lazy_return() -> None:
    query = """
        CALL user_dm.prc_drop_volatile_table (USER||'.test');

        CREATE VOLATILE TABLE test (
            day_id				date NOT NULL
        )
        UNIQUE PRIMARY INDEX (day_id)
        ON COMMIT PRESERVE ROWS;

        INSERT INTO test (?);

        SELECT * FROM test ORDER BY day_id asc;

    """

    correct_result = (
        DataFrame(
            data={
                "day_id": [
                    date.today() - timedelta(days=5),
                    date.today() - timedelta(days=4),
                    date.today() - timedelta(days=3),
                    date.today() - timedelta(days=2),
                    date.today() - timedelta(days=1),
                    date.today(),
                ]
            }
        )
        .sort_values(by=["day_id"])
        .reset_index(drop=True)
    )

    dfs = []

    with helpers.TeradataConnectionFromKeyring() as conn:
        with core.Query(conn, query, [correct_result], batch=3) as query:
            for result in query:
                dfs.append(result)

    test_result = (
        pd.concat(dfs, axis=0, ignore_index=True)
        .sort_values(by=["day_id"])
        .reset_index(drop=True)
    )

    print(test_result)

    assert test_result.equals(correct_result)
