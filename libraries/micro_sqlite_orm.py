import sqlite3

ALL_ROWS = -1


class MicroSqliteORM:
    def __init__(self, db_path: str, db_options: dict = {}, log: bool = False):
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path, **db_options)
        self.cursor = self.connection.cursor()

        if log:
            self.connection.set_trace_callback(print)

    def __enter__(self):
        self.cursor.execute("PRAGMA foreign_keys = ON")
        self.cursor.execute("PRAGMA encoding=utf8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.close()

    def iterate(self):
        row = self.cursor.fetchone()
        while row:
            yield row
            row = self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def commit(self):
        return self.connection.commit()

    def close(self):
        return self.connection.close()

    def find_many(self, table, *, select="*", skip=0, limit=100, order_by=[], where: dict | list = {}, values=[]):
        query = f"SELECT {select} FROM {table} WHERE 1=1"
        args = []

        if isinstance(where, list):
            query += "".join(f" AND {it}" for it in where)
            args = values
        else:
            for k, v in where.items():
                query += f" AND {k} = ?"
                args.append(v)

        if order_by:
            query += f" ORDER BY {','.join(order_by)}"

        query += f" LIMIT {limit} OFFSET {skip}"

        return self.cursor.execute(query, args)

    def find_one(self, table, **kargs):
        self.find_many(table, **kargs, limit=1)
        return self.cursor.fetchone()

    def insert(self, table, values):
        query = f"INSERT INTO {table}"
        query += f" ({','.join(values.keys())})"
        query += f" VALUES ({','.join(['?'] * len(values))})"
        args = list(values.values())
        return self.cursor.execute(query, args)

    def update(self, table, *, where: str | dict, values: dict):
        args = list(values.values())
        query = f"UPDATE {table} SET"
        query += f" {','.join(f'{k} = ?' for k in values.keys())}"

        if isinstance(where, str):
            query += f" WHERE {where}"
        else:
            query += f" WHERE {' AND '.join(f'{k} = ?' for k in where.keys())}"
            args.extend(where.values())

        return self.cursor.execute(query, args)

    def upsert(self, table, *, where: str | dict, values: dict):
        result = self.find_one(table, where=where)
        if result:
            return self.update(table, where=where, values=values)
        return self.insert(table, values)

    def delete(self, table, *, where):
        query = f"DELETE FROM {table} WHERE {where}"
        return self.cursor.execute(query)

    def begin_transaction(self):
        self.cursor.execute("BEGIN TRANSACTION")

    def commit_transaction(self):
        self.cursor.execute("COMMIT TRANSACTION")

    def end_transaction(self):
        self.cursor.execute("END TRANSACTION")
