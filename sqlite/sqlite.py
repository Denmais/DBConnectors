from contextlib import contextmanager
from typing import Generator, Any
import sqlite3


class SqliteConnect:

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = None
        self.cur = None

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class ConnectionBuilder:

    @staticmethod
    def pg_conn(path: str) -> SqliteConnect:
        sqlite = SqliteConnect(path)
        return sqlite


class SqliteTools:

    def __init__(self, path: str) -> None:
        self.sqlite_conn = ConnectionBuilder().pg_conn(path)

    def create(self, table: str = '', **kwargs) -> None:
        columns = ', '.join([' '.join(i) for i in kwargs.items()])  # [name type,]
        with self.sqlite_conn.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {columns}
                    )
                """
                )

    def insert(self, table: str = '', *args) -> None:
        with self.sqlite_conn.connection() as conn:
            cur = conn.cursor()
            for row in args:
                fields = ', '.join([i for i in row])
                data = [row[i] for i in row]
                data_row = ', '.join(['?' for i in row])
                cur.execute(
                    f"""
                        INSERT INTO {table} ({fields}) VALUES (
                        {data_row}
                        )
                    """, data
                    )

    def select(self, table: str = '', statement: str = '') -> Any:
        with self.sqlite_conn.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                    SELECT * FROM {table}
                    {'WHERE ' + statement if statement else ''}
                """
                )
            objs = cur.fetchall()
        return objs

    def delete(self, table: str = '', statement: str = "") -> None:
        with self.sqlite_conn.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                    DELETE FROM {table}
                    {'WHERE ' + statement if statement else ''}
                """
                )


table = 'ORDERS'
columns = {'item': 'text', 'numbers': 'int'}


connector = SqliteTools('/home/dev/Data_Engineering/connectors/sqlite/sqlite.db')


connector.create(table, **columns)


connector.insert(table, {'item': "car", 'numbers': '123'},
                 {'item': 'elden_ring', 'numbers': "456"})

obj = connector.select(table, 'numbers=123')
print(obj)

connector.delete(table, 'numbers=123')
