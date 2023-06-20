from tdwrapper import Query, StatementExecutionResult
from getpass import getpass
from datetime import date, datetime
import teradatasql


HOST = input("Host:")
USERNAME = input("Username:")
PASSWORD = getpass("Password:")

query_text = """
    {fn teradata_nativesql}{fn teradata_get_errors};

    CREATE VOLATILE TABLE test (
        ints INT NOT NULL,
        dts date not null,
        dt_time timestamp not null,
        nullable_dt_time timestamp null,
        strs varchar(255) not null,
        floats numeric(16,3) not null,
        nullable_floats float null,
        nullable_ints int null
    )
    PRIMARY INDEX (ints)
    ON COMMIT PRESERVE ROWS;

    insert into test (?, ?, ?, ?, ?, ?, ?, ?);

    select * from test order by ints;

    """

test_input = [
    [
        [
            1,
            date(2022, 1, 1),
            datetime.now(),
            datetime.now(),
            "one",
            4.4443,
            4.44433333,
            11,
        ],
        [2, date(2022, 2, 1), datetime.now(), None, "two", 5.4443, None, None],
        [
            3,
            date(2022, 3, 1),
            datetime.now(),
            datetime.now(),
            "three",
            6.4443,
            4.44455555,
            33,
        ],
    ]
]

with teradatasql.connect(
    host=HOST, logmech="LDAP", user=USERNAME, password=PASSWORD, tmode="ANSI"
) as conn:
    x = []
    with Query(conn, query_text, test_input, batch=100) as query:
        for result in query:
            if result is not StatementExecutionResult.NEXT_RESULT:
                x.append(result)


print(x[0])
