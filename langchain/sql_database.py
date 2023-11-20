"""SQLAlchemy wrapper around a database."""
from __future__ import annotations

import warnings
from typing import Any, Iterable, List, Optional

from langchain import utils

import psycopg2

from urllib.parse import urlparse
class SQLDatabase:
    """SQLAlchemy wrapper around a database."""

    def __init__(
        self,
        connection: connection,
        schema: Optional[str] = None,
        metadata: Optional[str] = None,
        ignore_tables: Optional[List[str]] = None,
        include_tables: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        indexes_in_table_info: bool = False,
        custom_table_info: Optional[dict] = None,
        truncate_col: int = 50,
        view_support: bool = False,
    ):
        """Create engine from database URI."""
        self._connection = connection
        self._schema = schema
        self._truncate_col = truncate_col
        print(f"\ntruncate_col: {truncate_col}\n")
        sql = """SELECT c.relname,a.attname, t.typname, c.oid
         FROM pg_catalog.pg_attribute a INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid) LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum) LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) left outer join pg_catalog.pg_type t on (a.atttypid = t.oid)
         WHERE NOT a.attisdropped ORDER BY c.relname,a.attnum""";
        connection.cursor().execute(sql)

        if include_tables and ignore_tables:
            raise ValueError("Cannot specify both include_tables and ignore_tables")

        #self._inspector = inspect(self._engine)

        # including view support by adding the views as well as tables to the all
        # tables list if view_support is True
        self._all_tables = set(
            self.get_tables_list()
        )

        self._include_tables = set(include_tables) if include_tables else set()
        if self._include_tables:
            missing_tables = self._include_tables - self._all_tables
            if missing_tables:
                raise ValueError(
                    f"include_tables {missing_tables} not found in database"
                )
        self._ignore_tables = set(ignore_tables) if ignore_tables else set()
        if self._ignore_tables:
            missing_tables = self._ignore_tables - self._all_tables
            if missing_tables:
                raise ValueError(
                    f"ignore_tables {missing_tables} not found in database"
                )
        usable_tables = self.get_usable_table_names()
        self._usable_tables = set(usable_tables) if usable_tables else self._all_tables

        if not isinstance(sample_rows_in_table_info, int):
            raise TypeError("sample_rows_in_table_info must be an integer")

        self._sample_rows_in_table_info = sample_rows_in_table_info
        self._indexes_in_table_info = indexes_in_table_info

        self._custom_table_info = custom_table_info
        if self._custom_table_info:
            if not isinstance(self._custom_table_info, dict):
                raise TypeError(
                    "table_info must be a dictionary with table names as keys and the "
                    "desired table info as values"
                )
            # only keep the tables that are also present in the database
            intersection = set(self._custom_table_info).intersection(self._all_tables)
            self._custom_table_info = dict(
                (table, self._custom_table_info[table])
                for table in self._custom_table_info
                if table in intersection
            )


    @classmethod
    def from_uri(
        cls, database_uri: str, engine_args: Optional[dict] = None, **kwargs: Any
    ) -> SQLDatabase:
        """Construct a SQLAlchemy engine from URI."""
        result = urlparse(database_uri)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port
        print(f"\nfrom_uri:{engine_args}\n")
        return cls(psycopg2.connect(
                host=hostname,
                port=port,
                user=username,
                password=password,
                database=database
        ), **kwargs)

    @property
    def dialect(self) -> str:
        """Return string representation of dialect to use."""
        return "postgresql"

    def get_usable_table_names(self) -> Iterable[str]:
        """Get names of tables available."""
        if self._include_tables:
            return self._include_tables
        return self._all_tables - self._ignore_tables

    def get_table_names(self) -> Iterable[str]:
        """Get names of tables available."""
        warnings.warn(
            "This method is deprecated - please use `get_usable_table_names`."
        )
        return self.get_usable_table_names()

    @property
    def table_info(self) -> str:
        """Information about all tables in the database."""
        return self.get_table_info()

    def get_tables_dict(self) -> dict:
        cursor = self._connection.cursor()
        tables = {}
        sql = """SELECT c.relname,a.attname, t.typname, c.oid
                FROM pg_catalog.pg_attribute a INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid) LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum) LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) left outer join pg_catalog.pg_type t on (a.atttypid = t.oid)
                WHERE NOT a.attisdropped ORDER BY c.relname,a.attnum""";
        cursor.execute(sql)
        for rec in cursor.fetchall():
            tables.setdefault(rec[0],[])
            tables[rec[0]].append((rec))
        return tables
    
    def get_tables_list(self):
        cursor = self._connection.cursor()
        tables = []
        sql = """SELECT c.oid, c.relname--,d.description, dep.objid, dep.refobjsubid
                 FROM pg_catalog.pg_class c
                 WHERE  c.relnamespace=1 and c.relkind not in ('i','I','c')"""
        cursor.execute(sql)
        for rec in cursor.fetchall():
            tables.append(rec[1])
        return tables

    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        """Get information about specified tables.

        Follows best practices as specified in: Rajkumar et al, 2022
        (https://arxiv.org/abs/2204.00498)

        If `sample_rows_in_table_info`, the specified number of sample rows will be
        appended to each table description. This can increase performance as
        demonstrated in the paper.
        """
        tables = self.get_tables_dict()
        all_table_names = self.get_usable_table_names()
        if table_names is not None:
            missing_tables = set(table_names).difference(all_table_names)
            if missing_tables:
                raise ValueError(f"table_names {missing_tables} not found in database")
            all_table_names = table_names

        SQL_template = """\n{SQL}\n\n
                 /*{SAMPLE}*/\n\n\n"""
        create_tables = []
        create_tables = []
        for t in tables.keys():
            create_table = "CREATE TABLE {table} (".format(table=t);
            atts = []
            for att in tables[t]:
                atts.append("{name} {typ}".format(name=att[1],typ=att[2]));
            create_table += ",\n".join(atts) + ")"
            create_tables.append(SQL_template.format(SQL=create_table, SAMPLE=""))
        final_str = "\n\n".join(create_tables)
        return final_str
    
    def run(self, command: str, fetch: str = "all") -> tuple[str,str]:
        """Execute a SQL command and return a string representing the results.

        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.
        """

        cursor = self._connection.cursor()
        cursor.execute(command)
        colnames = [desc[0] for desc in cursor.description]
        if "select" in  command.lower():
            if fetch == "all":
                result = cursor.fetchall()
            elif fetch == "one":
                result = cursor.fetchone()[0]  # type: ignore
            else:
                raise ValueError("Fetch parameter must be either 'one' or 'all'")
            result = self._truncate_results(result)
            return tuple(colnames), result
        return "",""

    def _truncate_results(self, results) -> List:
        list = []
        for row in results:
            list.append(tuple(e[:self._truncate_col] if isinstance(e, str) else e for e in row))
        return list


    def get_table_info_no_throw(self, table_names: Optional[List[str]] = None) -> str:
        """Get information about specified tables.

        Follows best practices as specified in: Rajkumar et al, 2022
        (https://arxiv.org/abs/2204.00498)

        If `sample_rows_in_table_info`, the specified number of sample rows will be
        appended to each table description. This can increase performance as
        demonstrated in the paper.
        """
        try:
            return self.get_table_info(table_names)
        except ValueError as e:
            """Format the error message"""
            return f"Error: {e}"

    def run_no_throw(self, command: str, fetch: str = "all") -> str:
        """Execute a SQL command and return a string representing the results.

        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.

        If the statement throws an error, the error message is returned.
        """
        try:
            return self.run(command, fetch)
        except Exception as e:
            """Format the error message"""
            return f"Error: {e}"
