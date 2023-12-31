import teradatasql
from pandas import DataFrame
from abc import ABC, abstractclassmethod
import enum
import re
from typing import Optional, Union
from datetime import datetime


class ExecutionResult(enum.Enum):
    NOTHING_TO_FETCH = enum.auto()
    SOMETHING_TO_FETCH = enum.auto()


class Statement(ABC):
    def __init__(self, text: str) -> None:
        self.text = text
        self.execution_start: datetime = None
        self.execution_end: datetime = None
        self.result: ExecutionResult = None

    @abstractclassmethod
    def execute(self, cur: teradatasql.TeradataCursor) -> ExecutionResult:
        pass


class SqlStatement(Statement):
    def __init__(self, text: str, input_list: Optional[list[list]] = None) -> None:
        self.text = text
        self.input_list = input_list

    def execute(self, cur: teradatasql.TeradataCursor) -> ExecutionResult:
        cur.execute(self.text, self.input_list)
        # if the db is ready to send rows to the client after statement execution,
        # then cur.description is not None.
        if cur.description:
            return ExecutionResult.SOMETHING_TO_FETCH
        else:
            return ExecutionResult.NOTHING_TO_FETCH


class TeradataStatement(Statement):
    def __init__(self, text) -> None:
        self.text = text

    def execute(self, cur: teradatasql.TeradataCursor) -> ExecutionResult:
        cur.execute(self.text)
        return ExecutionResult.NOTHING_TO_FETCH


class Query:
    """Class for returning one result for multiple sql statements lazily.
    Must be used with context manager.

    """

    def __init__(
        self,
        conn: teradatasql.TeradataConnection,
        query: str,
        input_data: Optional[list[Union[list[list], DataFrame]]] = None,
        # list or dictionary that contains 2d data: list of lists or DataFrame
        batch: int = 100_000,
        auto_commit: Optional[bool] = None,
    ):
        self.conn = conn
        self.query_text = query
        self.batch = batch
        self.current_statement = 0
        self.fetched_everything = True
        self.auto_commit = auto_commit
        self.input_data = input_data
        self.statements: list[Statement] = []

    def post_init(self):
        self.cur = self.conn.cursor()

        if self.auto_commit is None:
            pass
        elif self.auto_commit:
            self.cur.execute("{fn teradata_nativesql}{fn teradata_autocommit_on}")
        elif not self.auto_commit:
            self.cur.execute("{fn teradata_nativesql}{fn teradata_autocommit_off}")

        self.cur.arraysize = self.batch

        self.parse_statements()

        self.length = 0

        # self.number_of_iterations = ceil(self.length / self.batch)

    def __enter__(self):
        self.post_init()
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.cur.close()

    def __len__(self):
        return self.length

    def __iter__(self):
        return self

    def get_columns(self) -> dict:
        column_types = dict()
        for col in self.cur.description:
            for n in range(1_000):
                suffix = "" if n == 0 else "_" + str(n)
                column_name, column_type = col[0] + suffix, col[1]
                if column_types.get(column_name) is None:
                    column_types[column_name] = column_type
                    break
        return column_types

    def fetch(self) -> Union[DataFrame, ExecutionResult]:
        column_types = self.get_columns()
        not_fetched = self.cur.fetchmany()
        if not not_fetched:
            self.fetched_everything = True
            return ExecutionResult.NOTHING_TO_FETCH

        else:
            result = DataFrame(not_fetched, columns=list(column_types.keys()))

            self.fetched_everything = False
            return result

    def execute(self) -> ExecutionResult:
        result = self.statements[self.current_statement].execute(self.cur)
        self.current_statement += 1
        self.length = self.cur.rowcount
        return result

    def __next__(self) -> DataFrame:
        # in the process of fetching
        if not self.fetched_everything:
            result = self.fetch()
            if (
                isinstance(result, enum.Enum)
                and result == ExecutionResult.NOTHING_TO_FETCH
            ):
                # last iteration fetched last row, so in this iteration
                # fetched_everything switched to True.
                raise StopIteration
            else:
                return result

        # executed all statements without fetching
        elif self.current_statement >= len(self.statements):
            raise StopIteration

        # executing statements without returning control to the caller
        # until there is something to fetch
        for _ in self.statements[self.current_statement :]:
            if self.execute() == ExecutionResult.SOMETHING_TO_FETCH:
                return self.fetch()

    @staticmethod
    def df_or_list_to_list(input_element: Union[list[list], DataFrame]) -> list[list]:
        if isinstance(input_element, DataFrame):
            return input_element.to_numpy().tolist()
        else:
            return input_element

    def parse_statements(self):
        input_list = []

        if isinstance(self.input_data, list):
            for p in self.input_data:
                input_list.append(self.df_or_list_to_list(p))
        elif self.input_data is None:
            pass
        else:
            raise TypeError(
                'Only list or None type is allowed for property "input_data".'
            )
        query = self.query_text.lower()
        query = re.sub(r"/\*(\n|.(?<!\*/))*\*/", "", query, flags=re.MULTILINE)
        query = re.sub(r"--.*$", "", query, flags=re.MULTILINE)

        insert_count = 0
        for statement in query.split(";")[:-1]:
            reg_td = re.search(
                r"{fn teradata_[\w()]*}", statement, re.MULTILINE | re.IGNORECASE
            )
            reg_insert = re.search(
                r"\s*insert\s+into\s+([\w\d_]+)\s+([value(s)*\s]+)?\([?,\s]+\)\s*;*",
                statement,
                re.MULTILINE | re.IGNORECASE,
            )

            if reg_td is not None:
                self.statements.append(TeradataStatement(statement))

            elif reg_insert is not None:
                if input_list:
                    self.statements.append(SqlStatement(statement, input_list.pop(0)))
                    insert_count = insert_count + 1
                else:
                    raise ValueError(
                        f"Insert into table {reg_insert.string} requires at least {insert_count+1} arrays."
                    )
            else:
                self.statements.append(SqlStatement(statement))

        if len(self.statements) == 0:
            raise ValueError("No statements in query.")
