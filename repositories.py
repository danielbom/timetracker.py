from datetime import datetime
from typing import Iterable, Optional
from constants import DB_DATE_FORMAT, DB_PATH, CSV_DATE_FORMAT
from core import Row

from libraries.micro_sqlite_orm import ALL_ROWS, MicroSqliteORM


class TimetrackerRepository:
    @staticmethod
    def find_many_iter(start: Optional[datetime] = None, limit=ALL_ROWS, asc=False) -> Iterable[Row]:
        with MicroSqliteORM(DB_PATH) as orm:
            where = []
            values = []
            if start:
                where.append("start >= ?")
                start = start.replace(
                    hour=0, minute=0, second=0, microsecond=0)
                values.append(start.strftime(CSV_DATE_FORMAT))

            orm.find_many(
                "timetrack",
                select="start, message, end, category, rowid",
                order_by=["start ASC"] if asc else ["start DESC"],
                where=where,
                values=values,
                limit=limit,
            )

            yield from (Row.from_db(row) for row in orm.iterate())

    @staticmethod
    def find_many(start: Optional[datetime] = None, limit=ALL_ROWS, asc=False) -> list:
        return list(TimetrackerRepository.find_many_iter(start, limit, asc))

    @staticmethod
    def find_by_id(rowid: int) -> Optional[Row]:
        with MicroSqliteORM(DB_PATH) as orm:
            row = orm.find_one(
                "timetrack",
                select="start, message, end, category, rowid",
                where=["rowid = ?"],
                values=[rowid],
            )

            if row:
                return Row.from_db(row)

    @staticmethod
    def create(row: Row):
        with MicroSqliteORM(DB_PATH) as orm:
            orm.insert("timetrack", row.to_dict_db())
            return row._replace(rowid=orm.cursor.lastrowid)

    @staticmethod
    def update(row: Row):
        with MicroSqliteORM(DB_PATH) as orm:
            where = {}
            if row.rowid != 0:
                where["rowid"] = row.rowid
            else:
                where['start'] = row.start.strftime(DB_DATE_FORMAT)
                where['message'] = row.message

            orm.update(
                "timetrack",
                where=where,
                values={
                    "end": row.end.strftime(DB_DATE_FORMAT),
                },
            )
            return row._replace(rowid=orm.cursor.lastrowid)

    @staticmethod
    def store_or_update(row: Row):
        with MicroSqliteORM(DB_PATH) as orm:
            orm.upsert(
                "timetrack",
                where={
                    "start": row.start.strftime(DB_DATE_FORMAT),
                    "message": row.message
                },
                values=row.to_dict_db(),
            )
            return row._replace(rowid=orm.cursor.lastrowid)

    @staticmethod
    def max_rowid(**kargs) -> int:
        with MicroSqliteORM(DB_PATH) as orm:
            return orm.find_one("timetrack", **kargs, select="MAX(rowid)")[0]
